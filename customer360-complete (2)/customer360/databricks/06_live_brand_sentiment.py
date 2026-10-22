# Databricks notebook source
# MAGIC %md ## Gold live brand sentiment: refresh a one-minute rolling view from the Delta feed landing
# MAGIC Run after `05_live_feeds_kafka.py` has created `c360.brand_mentions`.
# MAGIC The target table is intended for a DirectQuery/refreshing BI visual. This is a live operational metric,
# MAGIC not a controlled estimate of business lift.
import os
from pyspark.sql import functions as F
from delta.tables import DeltaTable

SOURCE = "c360.brand_mentions"
TARGET = "c360.gold_brand_sentiment_last_minute"
CHECKPOINT = os.getenv(
    "C360_SENTIMENT_CHECKPOINT", "s3://c360-lake/_checkpoints/gold-brand-sentiment"
)

spark.sql("CREATE SCHEMA IF NOT EXISTS c360")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {TARGET} (
  brand STRING, avg_sentiment_last_minute DOUBLE, mentions_last_minute BIGINT,
  last_mention_ts TIMESTAMP, refreshed_at TIMESTAMP
) USING DELTA""")


def refresh_last_minute(_microbatch, batch_id):
    # Read the Delta landing table, rather than only this incoming microbatch, so
    # several small producer batches contribute to the same rolling 60-second metric.
    recent = spark.table(SOURCE).where(
        F.col("event_ts") >= F.current_timestamp() - F.expr("INTERVAL 1 MINUTE")
    )
    summary = (
        recent.groupBy("brand")
        .agg(
            F.avg("sentiment").alias("avg_sentiment_last_minute"),
            F.count(F.lit(1)).cast("long").alias("mentions_last_minute"),
            F.max("event_ts").alias("last_mention_ts"),
        )
        .withColumn("refreshed_at", F.current_timestamp())
    )
    target = DeltaTable.forName(spark, TARGET)
    (target.alias("t").merge(summary.alias("s"), "t.brand = s.brand")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
    # Drop brands that have no matching mention in the rolling window, avoiding stale
    # sentiment values being displayed as if they were current.
    DeltaTable.forName(spark, TARGET).delete(
        "last_mention_ts IS NULL OR last_mention_ts < current_timestamp() - INTERVAL 1 MINUTE"
    )
    print(f"GOLD_LIVE_SENTIMENT_REFRESH batch_id={batch_id}", flush=True)


# The refresh-clock stream is intentionally independent of mention volume; the
# callback reads the current Delta landing snapshot on every trigger.
refresh_clock = spark.readStream.format("rate").option("rowsPerSecond", 1).load()
query = (
    refresh_clock.writeStream.foreachBatch(refresh_last_minute)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="10 seconds")
    .queryName("c360_gold_live_brand_sentiment")
    .start()
)
print(f"GOLD_LIVE_SENTIMENT_STARTED source={SOURCE} target={TARGET} checkpoint={CHECKPOINT} refreshSeconds=10", flush=True)
spark.streams.awaitAnyTermination()
