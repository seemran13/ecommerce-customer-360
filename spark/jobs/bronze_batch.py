"""
Raw landing (HDFS/S3) -> Bronze Delta.  Bronze = data exactly as ingested + ingest metadata (no cleansing).
 * On Databricks (C360_AUTOLOADER=1) uses **Auto Loader** (cloudFiles) with trigger(availableNow): incremental,
   schema-tracked, exactly-once file discovery.
 * Elsewhere uses a plain batch read of the landing folders.
Tickets (text files) and reviews (CSV) land here too.
"""
import os

from pyspark.sql import functions as F

from c360.config import settings as S
from spark.common import get_spark, path
from spark.schemas import RAW


def with_meta(df):
    return df.withColumn("_ingest_ts", F.current_timestamp()).withColumn("_source_file", F.input_file_name())


def main():
    spark = get_spark("c360-bronze-batch")
    autoloader = os.environ.get("C360_AUTOLOADER") == "1"
    for t, schema in RAW.items():
        src, dst = path(S.raw_root, t), path(S.bronze_root, t)
        if autoloader:
            q = (with_meta(spark.readStream.format("cloudFiles").option("cloudFiles.format", "csv")
                           .option("cloudFiles.schemaLocation", path(S.checkpoint_root, "schema", t))
                           .schema(schema).load(src))
                 .writeStream.format("delta").option("checkpointLocation", path(S.checkpoint_root, "bronze", t))
                 .trigger(availableNow=True).start(dst))
            q.awaitTermination()
        else:
            (with_meta(spark.read.schema(schema).option("recursiveFileLookup", "true").csv(src))
             .write.format("delta").mode("overwrite").save(dst))
        print("bronze", t, spark.read.format("delta").load(dst).count())

    tick = path(S.raw_root, "tickets")
    if os.path.exists(tick) or tick.startswith(("hdfs", "s3", "dbfs")):
        raw = spark.read.option("lineSep", "---\n").text(tick).filter(F.length(F.trim("value")) > 0)
        g = lambda k: F.regexp_extract("value", rf"{k}: (\S+)", 1)
        (with_meta(raw.select(
            g("TICKET_ID").alias("ticket_id"), g("CUSTOMER_ID").alias("customer_id"),
            g("CREATED").cast("timestamp").alias("created_at"), g("CATEGORY").alias("category"),
            F.regexp_extract("value", r"(?s)BODY: (.*)$", 1).alias("body")))
         .write.format("delta").mode("overwrite").save(path(S.bronze_root, "tickets")))
    rev = path(S.raw_root, "reviews")
    if os.path.exists(rev) or rev.startswith(("hdfs", "s3", "dbfs")):
        (with_meta(spark.read.option("header", True).option("inferSchema", True).csv(rev))
         .write.format("delta").mode("overwrite").save(path(S.bronze_root, "reviews")))


if __name__ == "__main__":
    main()
