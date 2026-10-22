# Databricks notebook source
# MAGIC %md ## Live public feeds: Kafka brand_mentions + weather -> Delta tables
# MAGIC Attach the Spark Kafka connector compatible with your Databricks Runtime. Set
# MAGIC `KAFKA_BOOTSTRAP_SERVERS` to broker addresses routable from the Databricks cluster;
# MAGIC Docker service names such as kafka1 are only resolvable inside the Compose network.
import os
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "")
CHECKPOINT_ROOT = os.getenv("C360_LIVE_FEEDS_CHECKPOINT", "s3://c360-lake/_checkpoints/live-feeds")
if not BOOTSTRAP.strip():
    raise ValueError("Set KAFKA_BOOTSTRAP_SERVERS to brokers reachable from this Databricks cluster.")

spark.sql("CREATE SCHEMA IF NOT EXISTS c360")
brand_schema = StructType([
    StructField("event_ts", StringType()), StructField("brand", StringType()),
    StructField("text", StringType()), StructField("sentiment", DoubleType()), StructField("did", StringType()),
])
weather_schema = StructType([
    StructField("event_ts", StringType()), StructField("city", StringType()),
    StructField("temp_c", DoubleType()), StructField("precip_mm", DoubleType()), StructField("weather_code", IntegerType()),
])


def consume(topic, schema, table_name, required):
    records = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 5000)
        .option("failOnDataLoss", "false")
        .load()
        .select(F.from_json(F.col("value").cast("string"), schema).alias("e"))
        .select("e.*")
        .withColumn("event_ts", F.to_timestamp("event_ts"))
        .where(F.col("event_ts").isNotNull())
    )
    for column in required:
        records = records.where(F.col(column).isNotNull())
    records = records.withColumn("event_date", F.to_date("event_ts")).withColumn("_ingest_ts", F.current_timestamp())
    return (
        records.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/{table_name}")
        .trigger(processingTime="10 seconds")
        .queryName(f"c360_{table_name}_stream")
        .toTable(f"c360.{table_name}")
    )

brand_query = consume("brand_mentions", brand_schema, "brand_mentions", ("brand", "sentiment"))
weather_query = consume("weather", weather_schema, "weather_events", ("city", "temp_c"))
print(f"LIVE_FEEDS_STARTED brand_query={brand_query.name} weather_query={weather_query.name}", flush=True)
spark.streams.awaitAnyTermination()
