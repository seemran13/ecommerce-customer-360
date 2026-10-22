"""
Gold customer_360 -> HBase (batch sync, nightly).
Column families:  p = profile (batch-owned)   m = model outputs (batch-owned)   c = live cart state (stream-owned)
The batch never touches family c, so it cannot clobber the real-time cart status written by the streaming job.
Writes are idempotent puts keyed by customer_id.
"""
import time

from c360.config import settings as S
from spark.common import get_spark, path

P_COLS = ["name", "city", "state", "tier", "ltv", "order_count", "avg_order_value", "last_order_ts",
          "last_seen_ts", "abandoned_cart_count", "ticket_count", "avg_review_score"]
M_COLS = ["segment", "propensity"]


def write_partition(rows):
    import happybase
    conn = happybase.Connection(S.hbase_host, S.hbase_port, timeout=20000)
    b = conn.table(S.hbase_table).batch(batch_size=1000)
    for r in rows:
        d = r.asDict()
        data = {f"p:{c}".encode(): str(d[c]).encode() for c in P_COLS if c in d and d[c] is not None}
        data.update({f"m:{c}".encode(): str(d[c]).encode() for c in M_COLS if c in d and d[c] is not None})
        b.put(d["customer_id"].encode(), data)
    b.send()
    conn.close()


def main():
    spark = get_spark("c360-hbase-sync")
    df = spark.read.format("delta").load(path(S.gold_root, "customer_360"))
    t0 = time.time()
    df.repartition(8).foreachPartition(write_partition)
    print(f"synced {df.count()} profiles to HBase in {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
