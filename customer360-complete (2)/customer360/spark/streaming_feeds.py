#!/usr/bin/env python3
"""Capture Kafka brand_mentions and weather topics as partitioned Parquet feed landings.

This small independent Spark job keeps optional external feed processing separate
from the core cart-abandonment query. It writes under the project bind mount by
default, so the landing files can be inspected or promoted to S3/Delta downstream.
"""
import os
import sys
import traceback

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from common import spark

BOOT = os.getenv("KAFKA_BOOTSTRAP_INTERNAL", "kafka1:9092,kafka2:9092,kafka3:9092")
CHECKPOINT_ROOT = os.getenv("FEEDS_CHECKPOINT_ROOT", "/checkpoints/customer360_feeds")
OUTPUT_ROOT = os.getenv("FEEDS_OUTPUT_ROOT", "/opt/customer360/data/lake/feeds")
MAX_OFFSETS = int(os.getenv("FEEDS_MAX_OFFSETS_PER_TRIGGER", "1000"))
TRIGGER = os.getenv("FEEDS_TRIGGER", "10 seconds")
SHUFFLE_PARTITIONS = int(os.getenv("FEEDS_SHUFFLE_PARTITIONS", "2"))

BRAND_SCHEMA = StructType([
    StructField("event_ts", StringType()),
    StructField("brand", StringType()),
    StructField("text", StringType()),
    StructField("sentiment", DoubleType()),
    StructField("did", StringType()),
])
WEATHER_SCHEMA = StructType([
    StructField("event_ts", StringType()),
    StructField("city", StringType()),
    StructField("temp_c", DoubleType()),
    StructField("precip_mm", DoubleType()),
    StructField("weather_code", IntegerType()),
])


def start_feed(session, topic, schema, required, out_name):
    input_stream = (
        session.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOT)
        .option("subscribe", topic)
        .option("startingOffsets", os.getenv("FEEDS_STARTING_OFFSETS", "latest"))
        .option("maxOffsetsPerTrigger", MAX_OFFSETS)
        .option("failOnDataLoss", "false")
        .load()
        .select(F.from_json(F.col("value").cast("string"), schema).alias("event"))
        .select("event.*")
        .withColumn("event_ts", F.to_timestamp("event_ts"))
        .where(F.col("event_ts").isNotNull())
    )
    for field in required:
        input_stream = input_stream.where(F.col(field).isNotNull())
    frame = input_stream.withColumn("event_date", F.to_date("event_ts")).withColumn("_ingest_ts", F.current_timestamp())
    output_path = os.path.join(OUTPUT_ROOT, out_name)
    checkpoint = os.path.join(CHECKPOINT_ROOT, out_name)
    return (
        frame.writeStream.format("parquet")
        .outputMode("append")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint)
        .partitionBy("event_date")
        .trigger(processingTime=TRIGGER)
        .queryName(f"customer360_{out_name}")
        .start()
    )


def main():
    if MAX_OFFSETS < 1 or SHUFFLE_PARTITIONS < 1:
        raise ValueError("FEEDS_MAX_OFFSETS_PER_TRIGGER and FEEDS_SHUFFLE_PARTITIONS must be positive")
    session = spark("customer360_live_feeds", {"spark.sql.shuffle.partitions": str(SHUFFLE_PARTITIONS), "spark.sql.adaptive.enabled": "false"})
    queries = []
    try:
        queries.append(start_feed(session, "brand_mentions", BRAND_SCHEMA, ("brand", "sentiment"), "brand_mentions"))
        queries.append(start_feed(session, "weather", WEATHER_SCHEMA, ("city", "temp_c"), "weather"))
        print(
            f"CUSTOMER360_FEEDS_STARTED bootstrap={BOOT} output={OUTPUT_ROOT} checkpoint={CHECKPOINT_ROOT} "
            f"brandQuery={queries[0].name} weatherQuery={queries[1].name}", flush=True
        )
        session.streams.awaitAnyTermination()
        for query in queries:
            if not query.isActive:
                error = query.exception()
                print(f"CUSTOMER360_FEED_QUERY_STOPPED name={query.name} error={error}", file=sys.stderr, flush=True)
                if error is not None:
                    raise RuntimeError(f"Feed query {query.name} terminated: {error}")
    except Exception as exc:
        print(f"CUSTOMER360_FEEDS_FATAL {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        traceback.print_exc()
        raise
    finally:
        for query in queries:
            try:
                if query.isActive:
                    query.stop()
            except Exception:
                pass
        session.stop()


if __name__ == "__main__":
    main()
