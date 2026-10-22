#!/usr/bin/env python3
"""Customer360 Structured Streaming pipeline.

Kafka input: cart_events (JSON)
Outputs: Kafka cart_alerts, HBase customer_360, Parquet cart windows.

The historical replay path intentionally ignores legacy `heartbeat` records
before the watermark. Old heartbeat messages stamped with today's wall clock
can otherwise jump the watermark past months of historical replay data. The
updated heartbeat producer emits `watermark_tick` records whose event time is
coordinated with replay progress.

The alert Kafka and HBase sinks share one foreachBatch query. This prevents
Spark from computing the same stateful abandonment logic twice per micro-batch.
"""

import os
import sys
import time
import traceback
import hashlib

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.streaming.state import GroupStateTimeout
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType,
    IntegerType, BooleanType,
)
from common import spark, EVENT_SCHEMA

BOOT = os.getenv("KAFKA_BOOTSTRAP_INTERNAL", "kafka1:9092,kafka2:9092,kafka3:9092")
INPUT_TOPIC = os.getenv("KAFKA_INPUT_TOPIC", "cart_events")
# New root avoids attempting to reuse state from the older two-query alert topology.
# The existing /checkpoints/abandonment directory is preserved.
CKPT = os.getenv("CKPT", "/checkpoints/abandonment_v2")
IDLE_SEC = int(os.getenv("IDLE_SEC", 15 * 60))
WATERMARK_DELAY = os.getenv("WATERMARK_DELAY", "10 seconds")
MAX_OFFSETS_PER_TRIGGER = int(os.getenv("MAX_OFFSETS_PER_TRIGGER", "3000"))
SHUFFLE_PARTITIONS = int(os.getenv("SPARK_STREAMING_SHUFFLE_PARTITIONS", "4"))
HBASE_THRIFT = os.getenv("HBASE_THRIFT", "hbase:9090")
ALERT_TOPIC = os.getenv("ALERT_TOPIC", "cart_alerts")
WINDOW_PATH = os.getenv("WINDOW_PATH", "/opt/customer360/data/lake/silver/cart_windows")

STATE = StructType([
    StructField("last_ts", LongType()),
    StructField("cart_value", DoubleType()),
    StructField("n_cart", IntegerType()),
    StructField("done", BooleanType()),
    StructField("customer_id", LongType()),
    StructField("alerted", BooleanType()),
])

OUT = StructType([
    StructField("session_id", StringType()),
    StructField("customer_id", LongType()),
    StructField("cart_value", DoubleType()),
    StructField("n_cart", IntegerType()),
    StructField("last_event_ts", LongType()),
    StructField("alert_id", StringType()),
    StructField("detected_at", LongType()),
])


def _epoch_seconds(value):
    """Convert Spark/Pandas timestamps to UTC epoch seconds consistently."""
    ts = pd.Timestamp(value)
    if pd.isna(ts):
        return None
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return int(ts.timestamp())


def track(key, pdfs, state):
    """Maintain per-session cart state and emit one alert after an idle timeout."""
    import time as _time

    (session_id,) = key

    if state.hasTimedOut:
        last_ts, cart_value, n_cart, done, customer_id, alerted = state.get
        state.remove()
        if n_cart > 0 and not done and not alerted:
            alert_id = hashlib.md5(f"{session_id}{last_ts}".encode("utf-8")).hexdigest()
            yield pd.DataFrame([{
                "session_id": str(session_id),
                "customer_id": int(customer_id),
                "cart_value": float(cart_value),
                "n_cart": int(n_cart),
                "last_event_ts": int(last_ts),
                "alert_id": alert_id,
                "detected_at": int(_time.time()),
            }])
        return

    if state.exists:
        last_ts, cart_value, n_cart, done, customer_id, alerted = state.get
    else:
        last_ts, cart_value, n_cart, done, customer_id, alerted = 0, 0.0, 0, False, 0, False

    frames = [pdf for pdf in pdfs if pdf is not None and not pdf.empty]
    if frames:
        records = pd.concat(frames, ignore_index=True).sort_values("event_ts", kind="mergesort")
        for row in records.itertuples(index=False):
            if pd.isna(row.event_ts) or pd.isna(row.customer_id):
                continue
            ts = _epoch_seconds(row.event_ts)
            if ts is None:
                continue
            customer_id = int(row.customer_id)
            last_ts = max(int(last_ts), ts)

            event_type = str(row.event_type or "")
            if event_type == "add_to_cart":
                # A cart created after checkout is a new cart episode in this session.
                if done:
                    cart_value, n_cart, done, alerted = 0.0, 0, False, False
                price = 0.0 if pd.isna(row.price) else float(row.price)
                quantity = 1 if pd.isna(row.qty) else max(1, int(row.qty))
                cart_value += price * quantity
                n_cart += 1
            elif event_type in ("checkout", "purchase"):
                done = True

    if last_ts > 0 and n_cart > 0 and not done:
        state.update((int(last_ts), float(cart_value), int(n_cart), bool(done), int(customer_id), bool(alerted)))
        state.setTimeoutTimestamp((int(last_ts) + IDLE_SEC) * 1000)
    elif state.exists:
        # Completed sessions do not need to occupy RocksDB state until a timeout.
        state.remove()

    # Empty, schema-correct output for ordinary state updates; alerts are emitted on timeout.
    yield pd.DataFrame(columns=[field.name for field in OUT.fields])


def _write_hbase_rows(rows, batch_id):
    """Retry transient HBase Thrift failures; HBase puts are idempotent upserts."""
    if not rows:
        print(f"HBASE_BATCH batch_id={batch_id} alerts=0 customers=0", flush=True)
        return 0

    import happybase

    if ":" not in HBASE_THRIFT:
        raise ValueError(f"HBASE_THRIFT must be host:port, got {HBASE_THRIFT!r}")
    host, port_text = HBASE_THRIFT.rsplit(":", 1)
    port = int(port_text)

    # One Customer360 row per customer: write the most recent abandoned session only.
    latest_by_customer = {}
    for row in rows:
        customer_id = int(row.customer_id)
        ts = int(row.last_event_ts)
        if customer_id not in latest_by_customer or ts >= int(latest_by_customer[customer_id].last_event_ts):
            latest_by_customer[customer_id] = row

    last_error = None
    for attempt in range(1, 6):
        conn = None
        try:
            conn = happybase.Connection(host, port=port, timeout=10000, autoconnect=False)
            conn.open()
            table = conn.table("customer_360")
            with table.batch(batch_size=500) as batch:
                for customer_id, row in latest_by_customer.items():
                    row_key = f"{customer_id:010d}".encode("ascii")
                    batch.put(row_key, {
                        b"cart:active_cart": b"ABANDONED",
                        b"cart:cart_value": str(float(row.cart_value)).encode("ascii"),
                        b"cart:n_items": str(int(row.n_cart)).encode("ascii"),
                        b"cart:last_event_ts": str(int(row.last_event_ts)).encode("ascii"),
                    })
            print(
                f"HBASE_BATCH batch_id={batch_id} alerts={len(rows)} customers={len(latest_by_customer)} attempt={attempt}",
                flush=True,
            )
            return len(latest_by_customer)
        except Exception as exc:
            last_error = exc
            print(
                f"HBASE_BATCH_RETRY batch_id={batch_id} attempt={attempt}/5 error={type(exc).__name__}: {exc}",
                file=sys.stderr, flush=True,
            )
            if attempt < 5:
                time.sleep(min(2 ** (attempt - 1), 8))
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
    raise RuntimeError(f"HBase write failed after 5 attempts for batch {batch_id}") from last_error


def alert_sinks(batch_df, batch_id):
    """Fan out each computed alert batch to Kafka and HBase from one Spark query."""
    from pyspark import StorageLevel

    cached = batch_df.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        # Collect once for HBase and skip both sinks for empty timeout batches.
        rows = cached.collect()
        if not rows:
            print(f"ALERT_BATCH batch_id={batch_id} rows=0", flush=True)
            return

        kafka_rows = cached.select(
            F.col("alert_id").cast("string").alias("key"),
            F.to_json(F.struct(*cached.columns)).alias("value"),
        )
        kafka_rows.write.format("kafka").option("kafka.bootstrap.servers", BOOT).option("topic", ALERT_TOPIC).save()
        print(f"KAFKA_ALERT_BATCH batch_id={batch_id} rows={len(rows)} topic={ALERT_TOPIC}", flush=True)
        _write_hbase_rows(rows, batch_id)
    finally:
        cached.unpersist()


def main():
    session = spark("cart_abandonment_stream", {
        "spark.sql.streaming.stateStore.providerClass":
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
        "spark.sql.shuffle.partitions": str(SHUFFLE_PARTITIONS),
        "spark.sql.adaptive.enabled": "false",
        "spark.streaming.stopGracefullyOnShutdown": "true",
    })
    queries = []
    try:
        raw = (
            session.readStream.format("kafka")
            .option("kafka.bootstrap.servers", BOOT)
            .option("subscribe", INPUT_TOPIC)
            .option("startingOffsets", os.getenv("KAFKA_STARTING_OFFSETS", "earliest"))
            .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
            .option("failOnDataLoss", "false")
            .load()
        )

        parsed = (
            raw.select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"))
            .select("e.*")
            .where(
                "event_id IS NOT NULL AND customer_id IS NOT NULL "
                "AND event_ts IS NOT NULL AND event_type IS NOT NULL"
            )
            .withColumn("event_ts", F.to_timestamp("event_ts"))
            .where(F.col("event_ts").isNotNull())
        )

        # Legacy `heartbeat` messages used UTC-now while replaying old data and
        # poisoned the event-time watermark. Ignore them before the watermark.
        # The updated producer uses `watermark_tick` and coordinates its timestamp
        # with replay progress, so ticks can safely advance the watermark.
        watermark_input = parsed.where(F.col("event_type") != F.lit("heartbeat"))
        ev = watermark_input.withWatermark("event_ts", WATERMARK_DELAY).dropDuplicates(["event_id"])
        business_events = ev.where(~F.col("event_type").isin("heartbeat", "watermark_tick"))

        windows = (
            business_events.where(F.col("event_type") == "add_to_cart")
            .groupBy(F.window("event_ts", "10 minutes", "1 minute"))
            # approx_count_distinct is supported and cheaper in streaming than exact countDistinct.
            .agg(
                F.approx_count_distinct("session_id").alias("carts"),
                F.sum(F.coalesce(F.col("price"), F.lit(0.0)) * F.coalesce(F.col("qty"), F.lit(1))).alias("cart_value"),
            )
        )

        alerts = business_events.groupBy("session_id").applyInPandasWithState(
            track, OUT, STATE, "append", GroupStateTimeout.EventTimeTimeout
        )

        query_alerts = (
            alerts.writeStream.foreachBatch(alert_sinks)
            .option("checkpointLocation", CKPT + "/alerts_combined")
            .trigger(processingTime=os.getenv("ALERT_TRIGGER", "5 seconds"))
            .queryName("customer360_abandonment_alerts")
            .start()
        )
        queries.append(query_alerts)

        query_windows = (
            windows.writeStream.outputMode("append").format("parquet")
            .option("path", WINDOW_PATH)
            .option("checkpointLocation", CKPT + "/windows")
            .trigger(processingTime=os.getenv("WINDOW_TRIGGER", "30 seconds"))
            .queryName("customer360_cart_windows")
            .start()
        )
        queries.append(query_windows)

        print(
            f"CUSTOMER360_STREAM_STARTED inputTopic={INPUT_TOPIC} alertTopic={ALERT_TOPIC} "
            f"checkpoint={CKPT} partitions={SHUFFLE_PARTITIONS} "
            f"maxOffsetsPerTrigger={MAX_OFFSETS_PER_TRIGGER} idleSeconds={IDLE_SEC}",
            flush=True,
        )
        session.streams.awaitAnyTermination()
        print("CUSTOMER360_STREAM_STOPPED: a query terminated; inspecting status", file=sys.stderr, flush=True)
        for query in queries:
            try:
                print(f"QUERY_STATUS name={query.name} active={query.isActive} status={query.status}", file=sys.stderr, flush=True)
                if not query.isActive:
                    print(f"QUERY_EXCEPTION name={query.name}: {query.exception()}", file=sys.stderr, flush=True)
            except Exception as status_error:
                print(f"QUERY_STATUS_ERROR name={query.name}: {status_error}", file=sys.stderr, flush=True)
    except Exception as exc:
        print(f"CUSTOMER360_STREAM_FATAL {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        traceback.print_exc()
        for query in queries:
            try:
                print(f"QUERY_STATUS name={query.name} active={query.isActive} status={query.status}", file=sys.stderr, flush=True)
                if not query.isActive:
                    print(f"QUERY_EXCEPTION name={query.name}: {query.exception()}", file=sys.stderr, flush=True)
            except Exception as status_error:
                print(f"QUERY_STATUS_ERROR name={query.name}: {status_error}", file=sys.stderr, flush=True)
        raise
    finally:
        for query in queries:
            try:
                if query.isActive:
                    query.stop()
            except Exception:
                pass
        try:
            session.stop()
        except Exception:
            pass


if __name__ == "__main__":
    main()
