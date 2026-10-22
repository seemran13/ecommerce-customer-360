# Databricks notebook source
# MAGIC %md ## Gold: customer_360 + abandoned_carts + live brand sentiment summary
from pyspark.sql import functions as F

EVENTS = "c360.silver_events"
ORDERS = "c360.bronze_orders"
DIMENSION = "c360.silver_dim_customer"

ev = spark.table(EVENTS)
orders = spark.table(ORDERS)
dim = spark.table(DIMENSION).where("is_current = true")

order_metrics = orders.groupBy("customer_id").agg(
    F.sum("total_amount").alias("ltv"),
    F.countDistinct("order_id").alias("orders"),
    F.max("order_ts").alias("last_order_ts"),
    F.avg("total_amount").alias("aov"),
)
event_metrics = ev.groupBy("customer_id").agg(
    F.max("event_ts").alias("last_seen"),
    F.countDistinct("session_id").alias("sessions"),
    F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("cart_adds"),
    F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchases"),
)

customer_360 = (
    dim.join(order_metrics, "customer_id", "left")
    .join(event_metrics, "customer_id", "left")
    .fillna({"ltv": 0.0, "orders": 0, "sessions": 0, "cart_adds": 0, "purchases": 0})
    .withColumn("ltv_band", F.when(F.col("ltv") > 5000, "high").when(F.col("ltv") > 1000, "mid").otherwise("low"))
    .withColumn("refreshed_at", F.current_timestamp())
)
customer_360.write.mode("overwrite").format("delta").saveAsTable("c360.gold_customer_360")

# Session-level candidates; for historical backfills, the 15-minute age filter makes
# the materialized Gold output consistent with the capstone idle-cart definition.
sessions = ev.groupBy("session_id").agg(
    F.max("customer_id").alias("customer_id"),
    F.max("event_ts").alias("last_ts"),
    F.sum(F.when(F.col("event_type") == "add_to_cart", F.coalesce(F.col("price"), F.lit(0.0)) * F.coalesce(F.col("qty"), F.lit(1))).otherwise(0.0)).alias("cart_value"),
    F.sum(F.when(F.col("event_type").isin("checkout", "purchase"), 1).otherwise(0)).alias("converted"),
    F.first("device", ignorenulls=True).alias("device"),
)
abandoned = sessions.where(
    (F.col("cart_value") > 0)
    & (F.col("converted") == 0)
    & (F.col("last_ts") < F.current_timestamp() - F.expr("INTERVAL 15 MINUTES"))
)
abandoned.write.mode("overwrite").format("delta").saveAsTable("c360.gold_abandoned_carts")

# Live brand sentiment is maintained by the dedicated streaming Gold notebook
# 06_live_brand_sentiment.py. Avoid competing overwrite writes from this batch job.

spark.sql("OPTIMIZE c360.gold_customer_360 ZORDER BY (customer_id)")
