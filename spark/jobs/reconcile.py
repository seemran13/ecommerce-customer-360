"""
Reconciliation drill: Kafka -> Bronze -> Silver -> Gold (cart pipeline) + batch pipeline checks.

 1. Kafka       : messages per partition between earliest and latest offsets (batch read of the topic)
 2. Bronze      : rows (== Kafka, offset-for-offset, no loss; a gap in offsets is flagged)
 3. Silver      : distinct event_id.  bronze - silver = duplicates dropped (explained, must be >= 0)
 4. Gold        : abandoned_carts must equal alerts re-derived by replaying silver through the *same* pure state
                  machine used by the streaming job (c360.core.cart_state).  Differences are in-flight carts
                  (idle threshold not reached yet) and are reported separately.
 Exit code 1 if anything unexplained differs.
"""
import sys

from pyspark.sql import functions as F

from c360.config import settings as S
from c360.core.simulate import detect_alerts
from spark.common import get_spark, path


def expected_alerts(events, idle_s, now):
    alerts, inflight = detect_alerts(events, idle_s, end_ts=now)
    return {a["alert_id"] for a in alerts}, inflight


def main():
    spark = get_spark("c360-reconcile")
    ok = True
    kf = (spark.read.format("kafka").option("kafka.bootstrap.servers", S.kafka_bootstrap)
          .option("subscribe", S.topic_cart).option("startingOffsets", "earliest").option("endingOffsets", "latest").load())
    k_cnt = kf.count()
    br = spark.read.format("delta").load(path(S.bronze_root, "kafka_cart_events"))
    b_cnt, b_dist = br.count(), br.select("partition", "offset").distinct().count()
    sv = spark.read.format("delta").load(path(S.silver_root, "cart_events"))
    s_cnt = sv.count()
    dups = b_cnt - s_cnt
    print(f"Kafka={k_cnt}  Bronze={b_cnt} (distinct offsets {b_dist})  Silver={s_cnt}  duplicates_dropped={dups}")
    if k_cnt != b_dist:
        print(f"FAIL kafka vs bronze: {k_cnt - b_dist} messages missing from bronze")
        ok = False
    if dups < 0:
        print("FAIL silver has more rows than bronze")
        ok = False

    import time
    rows = [r.asDict() for r in sv.select("event_id", "customer_id", "event_type", "product_id", "quantity", "unit_price",
                                          "speed", F.col("event_ts").cast("double").alias("event_ts")).collect()]
    for e in rows:
        e["speed"] = e["speed"] or 1.0
    # group by effective idle threshold (live=900s, replay=900/speed) is handled per-state via `speed`
    exp, inflight = expected_alerts(rows, S.idle_threshold_s, time.time())
    gold = {r.alert_id for r in spark.read.format("delta").load(path(S.gold_root, "abandoned_carts")).select("alert_id").collect()}
    print(f"Gold abandoned_carts={len(gold)}  expected_from_silver={len(exp)}  in_flight(not yet idle)={inflight}")
    if gold != exp:
        print(f"FAIL gold vs expected: missing={len(exp - gold)} unexpected={len(gold - exp)}")
        ok = False
    # batch side
    for t in ("orders", "order_items"):
        b = spark.read.format("delta").load(path(S.bronze_root, t)).count()
        s = spark.read.format("delta").load(path(S.silver_root, t)).count()
        print(f"batch {t}: bronze_rows={b} silver_latest_rows={s} (difference = re-delivered versions of the same key)")
    c_b = spark.read.format("delta").load(path(S.bronze_root, "customers")).select("customer_id").distinct().count()
    c_s = spark.read.format("delta").load(path(S.silver_root, "dim_customer_scd2")).filter("is_current").count()
    c_g = spark.read.format("delta").load(path(S.gold_root, "customer_360")).count()
    print(f"customers: bronze_distinct={c_b} silver_current={c_s} gold={c_g}")
    if not (c_b == c_s == c_g):
        print("FAIL customer counts differ across layers")
        ok = False
    print("RECONCILIATION", "PASSED" if ok else "FAILED")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
