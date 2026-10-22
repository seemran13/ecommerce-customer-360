# Databricks notebook source
# MAGIC %md ## Bronze: Auto Loader from S3 raw zone (schema rescue + corrupt-row capture)
import os
from pyspark.sql import functions as F

RAW = os.getenv("C360_RAW_ROOT", "s3://c360-raw")
BRONZE_CHECKPOINTS = os.getenv("C360_CHECKPOINT_ROOT", "s3://c360-lake/_checkpoints")

spark.sql("CREATE SCHEMA IF NOT EXISTS c360")


def run_available_now(path, file_format, table_name, **options):
    """Ingest one input to a Delta bronze table and wait for availableNow to finish.

    Waiting per query prevents the notebook from advancing to Silver while one of
    the Bronze tables is still catching up. Each table has its own schema and stream checkpoint.
    """
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", file_format)
        .option("cloudFiles.schemaLocation", f"{BRONZE_CHECKPOINTS}/{table_name}_schema")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("rescuedDataColumn", "_rescued_data")
    )
    if file_format == "json":
        reader = reader.option("columnNameOfCorruptRecord", "_corrupt_record")
    for key, value in options.items():
        reader = reader.option(key, value)

    frame = (
        reader.load(path)
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
    query = (
        frame.writeStream.format("delta")
        .option("checkpointLocation", f"{BRONZE_CHECKPOINTS}/{table_name}")
        .trigger(availableNow=True)
        .toTable(f"c360.bronze_{table_name}")
    )
    print(f"BRONZE_STARTED table=c360.bronze_{table_name} source={path}")
    query.awaitTermination()
    if query.exception() is not None:
        raise RuntimeError(f"Bronze ingest failed for {table_name}: {query.exception()}")
    print(f"BRONZE_AVAILABLE_NOW_PASS table=c360.bronze_{table_name}")


run_available_now(f"{RAW}/weblogs/", "json", "events")
run_available_now(f"{RAW}/mysql/orders/", "csv", "orders", header="true")
run_available_now(f"{RAW}/mysql/customers/", "csv", "customers", header="true")
run_available_now(f"{RAW}/mysql/products/", "csv", "products", header="true")
run_available_now(f"{RAW}/mysql/order_items/", "csv", "order_items", header="true")
run_available_now(f"{RAW}/tickets/", "text", "tickets")
