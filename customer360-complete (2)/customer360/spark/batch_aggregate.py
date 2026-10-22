"""Batch aggregation: cleaned events + Hive orders -> customer_360 gold (one row per customer)."""
import sys
from pyspark.sql import functions as F
from common import spark
ev_in, out = sys.argv[1], sys.argv[2]
sp = spark("batch_aggregate")
ev = sp.read.parquet(ev_in)
act = (ev.groupBy("customer_id").agg(F.max("event_ts").alias("last_seen"), F.countDistinct("session_id").alias("sessions"),
       F.sum(F.when(F.col("event_type") == "add_to_cart", F.col("price") * F.col("qty"))).alias("cart_value_total")))
orders = sp.table("c360.fact_orders").groupBy("customer_key").agg(F.sum("line_amount").alias("ltv"), F.countDistinct("order_id").alias("orders"))
cust = sp.table("c360.dim_customer").where("is_current")
g = (cust.join(orders, "customer_key", "left").join(act, "customer_id", "left").fillna({"ltv": 0.0, "orders": 0, "sessions": 0}))
g.coalesce(1).write.mode("overwrite").parquet(out)
