"""
Maintained alternative to Sqoop (design-doc open item #5): incremental `updated_at > watermark` import over JDBC.
Writes Sqoop-compatible headerless CSV into a per-run folder under the raw landing zone, and stores the new
high-watermark in a small state file only AFTER the write succeeds (so a failed run is simply retried).
    scripts/submit.sh spark/jobs/jdbc_incremental.py
"""
import json
import os
from datetime import datetime

from pyspark.sql import functions as F

from c360.config import settings as S
from spark.common import get_spark, path
from spark.schemas import RAW

STATE = os.environ.get("JDBC_STATE_FILE", "data/lake/_state/jdbc_watermarks.json")


def main():
    spark = get_spark("c360-jdbc-incremental")
    os.makedirs(os.path.dirname(STATE), exist_ok=True)
    wm = json.load(open(STATE)) if os.path.exists(STATE) else {}
    run = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    new_wm = dict(wm)
    for t in RAW:
        last = wm.get(t, "1970-01-01 00:00:00")
        q = f"(SELECT * FROM {t} WHERE updated_at > '{last}') x"
        df = (spark.read.format("jdbc").option("url", S.jdbc_url()).option("dbtable", q)
              .option("user", S.mysql_user).option("password", S.mysql_password)
              .option("driver", "com.mysql.cj.jdbc.Driver").load()).cache()
        n = df.count()
        if n:
            df.coalesce(1).write.mode("overwrite").option("header", False).csv(path(S.raw_root, t, f"run={run}"))
            new_wm[t] = str(df.agg(F.max("updated_at")).first()[0])
        print(f"{t}: {n} changed rows since {last}")
        df.unpersist()
    json.dump(new_wm, open(STATE, "w"))     # commit watermark last


if __name__ == "__main__":
    main()
