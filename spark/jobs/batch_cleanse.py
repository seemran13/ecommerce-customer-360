"""
Spark *Core* (RDD) cleansing of raw web logs  ->  silver/web_events_batch

* regex parse with a **malformed-record accumulator** so data-quality problems are visible and persisted
* **broadcast join** attaches product category/price (product table is tiny vs. the clickstream)
Run:  scripts/submit.sh spark/jobs/batch_cleanse.py --weblogs data/source/weblogs --products data/lake/raw/products
"""
import argparse
import re
from datetime import datetime

from pyspark.sql import Row, functions as F

from c360.config import settings as S
from spark.common import get_spark, path
from spark.schemas import RAW

LOG_RE = re.compile(r'^(\S+) (\S+) "GET /(\w+)\?cid=(\w+)&pid=([\w-]+)&qty=(\d+)&price=([\d.]+) HTTP/1\.1" (\d{3}) sid=(\w+)$')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--weblogs", default="data/source/weblogs")
    ap.add_argument("--products", default=path(S.raw_root, "products"))
    a = ap.parse_args()
    spark = get_spark("c360-batch-cleanse")
    sc = spark.sparkContext
    total, bad = sc.accumulator(0), sc.accumulator(0)

    def parse(line):
        total.add(1)
        m = LOG_RE.match(line)
        if not m:
            bad.add(1)
            return []
        ts, ip, etype, cid, pid, qty, price, status, sid = m.groups()
        return [Row(event_ts=datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ"), customer_id=cid,
                    session_id=sid, event_type=etype, product_id=None if pid == "-" else pid,
                    quantity=int(qty), unit_price=float(price), http_status=int(status), ip=ip)]

    events = spark.createDataFrame(sc.textFile(a.weblogs).flatMap(parse))
    products = spark.read.schema(RAW["products"]).csv(a.products).dropDuplicates(["product_id"]) \
        .select("product_id", "category", F.col("price").alias("list_price"))
    out = (events.join(F.broadcast(products), "product_id", "left")
           .withColumn("event_id", F.sha1(F.concat_ws("|", "customer_id", F.col("event_ts").cast("string"),
                                                      "event_type", F.coalesce("product_id", F.lit("-")), "session_id")))
           .dropDuplicates(["event_id"]))
    out.write.format("delta").mode("overwrite").save(path(S.silver_root, "web_events_batch"))   # action -> accumulators valid

    n_total, n_bad = total.value, bad.value
    print(f"web log lines={n_total} malformed={n_bad} ({100.0 * n_bad / max(n_total, 1):.2f}%)")
    (spark.createDataFrame([(datetime.utcnow(), "web_logs", n_total, n_bad)],
                           "run_ts timestamp, source string, total_lines long, malformed_lines long")
     .write.format("delta").mode("append").save(path(S.gold_root, "dq_metrics")))
    # NOTE: accumulators updated inside transformations can over-count if a task is retried; the persisted
    # figures come from a single action, which is the standard caveat for this teaching pattern.


if __name__ == "__main__":
    main()
