"""
Real-time abandoned-cart detection.   Kafka cart_events -> per-customer stateful processing -> sinks

 state   : one JSON cart per customer (items, last_event_ts, recent event_ids for de-dup)   [c360.core.cart_state]
 trigger : every N seconds; a PROCESSING-TIME TIMEOUT is armed at  last_event_ts + idle_threshold
 output  : kind in ACTIVE | CONVERTED | EMPTY | ABANDONED  (one row per customer per micro-batch)
 sinks   : (1) Delta gold/abandoned_carts  MERGE on deterministic alert_id   -> idempotent
           (2) HBase  c:status / c:cart_value ...  (cell timestamp = event time -> idempotent, order-safe)
           (3) Kafka  cart_alerts (key = alert_id)  -> notifier de-dups on alert_id
 recovery: checkpoint (offsets + state) => after a restart the job resumes where it stopped; replayed batches
           re-run the same idempotent writes  => exactly-once *effect* (Kafka->Delta/HBase).

Second query: 10-min windows sliding every 1 min of event counts -> gold/cart_activity_windows (live BI tile).

    scripts/submit.sh spark/jobs/stream_cart_abandonment.py --trigger 5
"""
import argparse
import json

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout

from c360.config import settings as S
from c360.core import cart_state as cs
from spark.common import get_spark, path
from spark.delta_utils import upsert
from spark.schemas import CART_EVENT

OUT_SCHEMA = ("customer_id string, kind string, cart_id string, item_count long, cart_value double, "
              "last_event_ts double, detected_ts double, alert_id string, items_json string, threshold_s double, "
              "alert_latency_s double")
STATE_SCHEMA = "payload string"
COLS = ["customer_id", "kind", "cart_id", "item_count", "cart_value", "last_event_ts", "detected_ts",
        "alert_id", "items_json", "threshold_s", "alert_latency_s"]


def make_update_fn(idle_s: float):
    def update(key, pdf_iter, state: GroupState):
        cid = key[0]
        rows = []
        if state.hasTimedOut:
            st = json.loads(state.get[0]) if state.exists else None
            now = state.getCurrentProcessingTimeMs() / 1000.0
            alert = cs.expire(st, now, idle_s)
            state.remove()
            if alert:
                rows.append({**alert, "kind": "ABANDONED"})
            yield pd.DataFrame(rows, columns=COLS) if rows else pd.DataFrame(columns=COLS)
            return
        st = json.loads(state.get[0]) if state.exists else None
        events = []
        for pdf in pdf_iter:
            pdf = pdf.copy()
            pdf["event_ts"] = pdf["event_ts"].map(lambda t: t.timestamp())
            events.extend(pdf.to_dict("records"))
        now_ms = state.getCurrentProcessingTimeMs()
        st, status, mid = cs.apply_events_with_expiry(st, events, idle_s, now_ts=now_ms / 1000.0)
        rows.extend({**a, "kind": "ABANDONED"} for a in mid)
        if st is None:
            state.remove()
            row = {"customer_id": cid, "kind": status}
        else:
            state.update((json.dumps(st),))
            state.setTimeoutDuration(cs.timeout_ms(st, now_ms, idle_s))
            row = {"customer_id": cid, "kind": status, "cart_id": st["cart_id"],
                   "item_count": sum(i["qty"] for i in st["items"].values()), "cart_value": cs.cart_value(st),
                   "last_event_ts": st["last_event_ts"]}
        rows.append(row)
        yield pd.DataFrame(rows, columns=COLS)
    return update


def hbase_write(rows):
    import happybase
    conn = happybase.Connection(S.hbase_host, S.hbase_port, timeout=10000)
    t = conn.table(S.hbase_table)
    for r in rows:
        ts = int((r.last_event_ts or r.detected_ts or 0) * 1000) or None
        if r.kind in ("CONVERTED", "EMPTY"):
            data = {b"c:status": b"NONE", b"c:cart_value": b"0", b"c:item_count": b"0"}
        else:
            data = {b"c:status": r.kind.encode(), b"c:cart_value": str(r.cart_value).encode(),
                    b"c:item_count": str(r.item_count).encode(), b"c:cart_id": r.cart_id.encode()}
        if r.last_event_ts:
            data[b"c:last_seen"] = str(r.last_event_ts).encode()
        t.put(r.customer_id.encode(), data, timestamp=ts)
    conn.close()


def make_sink(spark):
    def sink(df, batch_id):
        df = df.persist()
        if df.rdd.isEmpty():
            df.unpersist()
            return
        df.foreachPartition(hbase_write)                                          # (2) HBase
        ab = df.filter("kind = 'ABANDONED'").withColumn("emitted_ts", F.current_timestamp()) \
               .withColumn("last_event_time", F.col("last_event_ts").cast("timestamp")) \
               .withColumn("detected_time", F.col("detected_ts").cast("timestamp"))
        if not ab.rdd.isEmpty():
            upsert(spark, ab, path(S.gold_root, "abandoned_carts"), ["alert_id"])  # (1) Delta MERGE
            (ab.select(F.col("alert_id").alias("key"), F.to_json(F.struct(
                "alert_id", "customer_id", "cart_id", "item_count", "cart_value", "items_json",
                "last_event_ts", "detected_ts", "threshold_s", "alert_latency_s")).alias("value"))
               .write.format("kafka").option("kafka.bootstrap.servers", S.kafka_bootstrap)
               .option("topic", S.topic_alerts).save())                            # (3) Kafka
        df.unpersist()
    return sink


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trigger", type=int, default=5, help="micro-batch interval, seconds")
    ap.add_argument("--idle", type=float, default=S.idle_threshold_s)
    ap.add_argument("--starting", default="latest", choices=["latest", "earliest"])
    a = ap.parse_args()
    spark = get_spark("c360-cart-abandonment")

    raw = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", S.kafka_bootstrap)
           .option("subscribe", S.topic_cart).option("startingOffsets", a.starting)
           .option("failOnDataLoss", "true").option("maxOffsetsPerTrigger", 50000).load())
    ev = (raw.select(F.from_json(F.col("value").cast("string"), CART_EVENT).alias("e")).select("e.*")
          .filter("event_id is not null and customer_id is not null and event_ts is not null"))

    alerts = ev.groupBy("customer_id").applyInPandasWithState(
        make_update_fn(a.idle), OUT_SCHEMA, STATE_SCHEMA, "append", GroupStateTimeout.ProcessingTimeTimeout)
    q1 = (alerts.writeStream.foreachBatch(make_sink(spark)).outputMode("append")
          .option("checkpointLocation", path(S.checkpoint_root, "cart_abandonment"))
          .trigger(processingTime=f"{a.trigger} seconds").queryName("cart_abandonment").start())

    win = (ev.withWatermark("event_ts", "2 minutes")
           .groupBy(F.window("event_ts", "10 minutes", "1 minute"), "event_type").count()
           .select(F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"),
                   "event_type", F.col("count").alias("events")))
    q2 = (win.writeStream.outputMode("update")
          .foreachBatch(lambda d, i: upsert(spark, d, path(S.gold_root, "cart_activity_windows"),
                                            ["window_start", "event_type"]))
          .option("checkpointLocation", path(S.checkpoint_root, "cart_windows"))
          .trigger(processingTime="30 seconds").queryName("cart_windows").start())
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
