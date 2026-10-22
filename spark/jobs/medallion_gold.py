"""
Silver -> Gold:  customer_360  (one row per customer; the HBase source)  +  ML input exports.
 abandoned_carts is produced by the streaming job; here we only derive per-customer abandonment features.
Segment / propensity columns are joined from ml/score.py output if it exists (data/ml/scores.parquet).
"""
import os

from pyspark.sql import functions as F

from c360.config import settings as S
from spark.common import get_spark, path


def exists(spark, p):
    from delta.tables import DeltaTable
    try:
        return DeltaTable.isDeltaTable(spark, p)
    except Exception:
        return False


def main():
    spark = get_spark("c360-gold")
    rd = lambda t: spark.read.format("delta").load(path(S.silver_root, t))
    cust = rd("dim_customer_scd2").filter("is_current").select("customer_id", "name", "email", "city", "state", "tier")
    orders = rd("orders").filter("status <> 'cancelled'")
    o = orders.groupBy("customer_id").agg(
        F.count("*").alias("order_count"), F.round(F.sum("total_amount"), 2).alias("ltv"),
        F.round(F.avg("total_amount"), 2).alias("avg_order_value"),
        F.min("order_ts").alias("first_order_ts"), F.max("order_ts").alias("last_order_ts"))

    ev = rd("web_events_batch").select("customer_id", "event_ts", "event_type", "session_id", "product_id",
                                       "quantity", "unit_price", "event_id")
    if exists(spark, path(S.silver_root, "cart_events")):
        ev = ev.unionByName(rd("cart_events").select(*ev.columns)).dropDuplicates(["event_id"])
    seen = ev.groupBy("customer_id").agg(F.max("event_ts").alias("last_seen_ts"),
                                         F.countDistinct("session_id").alias("session_count"))

    ab = (spark.read.format("delta").load(path(S.gold_root, "abandoned_carts"))
          if exists(spark, path(S.gold_root, "abandoned_carts")) else None)
    abf = (ab.groupBy("customer_id").agg(F.count("*").alias("abandoned_cart_count"),
                                         F.max("last_event_time").alias("last_abandoned_ts"))
           if ab is not None else None)

    tk = rd("tickets").groupBy("customer_id").agg(F.count("*").alias("ticket_count")) if exists(spark, path(S.silver_root, "tickets")) else None
    rv = rd("reviews").groupBy("customer_id").agg(F.round(F.avg("score"), 2).alias("avg_review_score")) if exists(spark, path(S.silver_root, "reviews")) else None

    g = cust.join(o, "customer_id", "left").join(seen, "customer_id", "left")
    for extra in (abf, tk, rv):
        if extra is not None:
            g = g.join(extra, "customer_id", "left")
    for c in ("order_count", "abandoned_cart_count", "ticket_count"):
        if c in g.columns:
            g = g.withColumn(c, F.coalesce(F.col(c), F.lit(0)))
    g = g.withColumn("ltv", F.coalesce("ltv", F.lit(0.0)))
    if "last_abandoned_ts" in g.columns:   # open abandonment = alert newer than last order
        g = g.withColumn("active_cart_status", F.when(
            F.col("last_abandoned_ts").isNotNull() &
            (F.col("last_order_ts").isNull() | (F.col("last_abandoned_ts") > F.col("last_order_ts"))),
            "ABANDONED").otherwise("NONE"))
    else:
        g = g.withColumn("active_cart_status", F.lit("NONE"))

    scores = os.path.join(S.ml_root, "scores.parquet")
    if os.path.exists(scores):
        g = g.join(spark.read.parquet(scores).select("customer_id", "segment", "propensity"), "customer_id", "left")
    else:
        g = g.withColumn("segment", F.lit(None).cast("string")).withColumn("propensity", F.lit(None).cast("double"))
    g = g.withColumn("updated_at", F.current_timestamp())
    g.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path(S.gold_root, "customer_360"))
    print("gold customer_360", g.count())

    # plain parquet exports consumed by the pandas ML code (demo-scale; see docs)
    ml_in = os.path.join(S.ml_root, "inputs")
    orders.select("order_id", "customer_id", "order_ts", F.col("total_amount").cast("double").alias("total_amount")).toPandas().to_parquet(f"{ml_in}_orders.parquet") if not ml_in.startswith(("hdfs", "s3")) else None
    ev.withColumn("unit_price", F.col("unit_price").cast("double")).toPandas().to_parquet(f"{ml_in}_events.parquet") if not ml_in.startswith(("hdfs", "s3")) else None


if __name__ == "__main__":
    os.makedirs(S.ml_root, exist_ok=True)
    main()
