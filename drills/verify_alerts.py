"""Spark-restart verifier: after the job was killed and restarted from its checkpoint, gold/abandoned_carts must hold
exactly one row per abandoned cart of the drill run (no loss, no duplicates).  Run via scripts/submit.sh."""
import json
import sys

from c360.config import settings as S
from spark.common import get_spark, path

run = sys.argv[1]
exp = json.load(open(f"drills/.expected_{run}.json"))
spark = get_spark("verify-alerts")
df = spark.read.format("delta").load(path(S.gold_root, "abandoned_carts")).filter(f"customer_id like 'drill-{run}-%'")
rows, distinct = df.count(), df.select("alert_id").distinct().count()
print(f"expected_alerts={exp['expected_alerts']} rows={rows} distinct_alert_ids={distinct}")
ok = rows == distinct == exp["expected_alerts"]
print("SPARK-RESTART DRILL", "PASSED (exactly-once effect)" if ok else "FAILED")
sys.exit(0 if ok else 1)
