# Databricks notebook source
# MAGIC %md ## Silver: typed/deduped events + explicit quarantine/duplicate ledger + customer SCD Type 2
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# ---- events: type first, quarantine every invalid required field, and keep a reconciliation ledger.
raw_events = spark.table("c360.bronze_events")
typed_events = raw_events.withColumn("event_ts", F.to_timestamp("event_ts"))
valid_condition = (
    F.col("event_id").isNotNull()
    & F.col("customer_id").isNotNull()
    & F.col("event_ts").isNotNull()
    & F.col("session_id").isNotNull()
    & F.col("event_type").isNotNull()
    & F.col("event_type").isin("view", "add_to_cart", "checkout", "purchase", "remove_from_cart")
)

quarantine = (
    typed_events.where(~F.coalesce(valid_condition, F.lit(False)))
    .withColumn(
        "_quarantine_reason",
        F.when(F.col("event_id").isNull(), "missing_event_id")
        .when(F.col("customer_id").isNull(), "missing_customer_id")
        .when(F.col("event_ts").isNull(), "missing_or_invalid_event_ts")
        .when(F.col("session_id").isNull(), "missing_session_id")
        .when(F.col("event_type").isNull(), "missing_event_type")
        .when(~F.col("event_type").isin("view", "add_to_cart", "checkout", "purchase", "remove_from_cart"), "unsupported_event_type")
        .otherwise("invalid_event"),
    )
)
quarantine.write.mode("overwrite").format("delta").saveAsTable("c360.silver_events_quarantine")

good_with_duplicates = typed_events.where(valid_condition).withColumn("event_date", F.to_date("event_ts"))
dup_ledger = (
    good_with_duplicates.groupBy("event_id").count().where(F.col("count") > 1)
    .withColumnRenamed("count", "source_count")
    .withColumn("duplicate_count", F.col("source_count") - F.lit(1))
    .withColumn("recorded_at", F.current_timestamp())
)
dup_ledger.write.mode("overwrite").format("delta").saveAsTable("c360.silver_event_duplicates")

# Stable survivor selection: keep the most recently ingested valid version for each event_id.
window = Window.partitionBy("event_id").orderBy(F.col("_ingest_ts").desc_nulls_last(), F.col("event_ts").desc())
good = (
    good_with_duplicates.withColumn("_rn", F.row_number().over(window))
    .where(F.col("_rn") == 1).drop("_rn")
)
good.write.mode("overwrite").partitionBy("event_date").format("delta").saveAsTable("c360.silver_events")

# ---- SCD2 customer dimension: close current row when address/tier changes, then insert current row.
spark.sql("""CREATE TABLE IF NOT EXISTS c360.silver_dim_customer (customer_id INT, full_name STRING, email STRING, city STRING, state STRING,
  address STRING, tier STRING, effective_from TIMESTAMP, effective_to TIMESTAMP, is_current BOOLEAN) USING delta""")

src = (
    spark.table("c360.bronze_customers").withColumn("last_modified", F.coalesce(F.to_timestamp("last_modified"), F.current_timestamp()))
    .withColumn("rn", F.row_number().over(Window.partitionBy("customer_id").orderBy(F.col("last_modified").desc_nulls_last())))
    .where("rn = 1").drop("rn")
)
tgt = DeltaTable.forName(spark, "c360.silver_dim_customer")

(tgt.alias("t").merge(src.alias("s"), "t.customer_id = s.customer_id AND t.is_current = true")
    .whenMatchedUpdate(
        condition="NOT (t.address <=> s.address) OR NOT (t.tier <=> s.tier)",
        set={"is_current": "false", "effective_to": "s.last_modified"},
    ).execute())
cur = spark.table("c360.silver_dim_customer").where("is_current = true").select("customer_id")
new_rows = (
    src.join(cur, "customer_id", "left_anti")
    .select("customer_id", "full_name", "email", "city", "state", "address", "tier",
            F.col("last_modified").alias("effective_from"), F.lit(None).cast("timestamp").alias("effective_to"), F.lit(True).alias("is_current"))
)
new_rows.write.mode("append").format("delta").saveAsTable("c360.silver_dim_customer")
