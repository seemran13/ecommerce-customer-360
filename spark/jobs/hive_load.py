"""
Silver -> Hive star schema  (db c360_dw):  fact_orders (partitioned by order_date), dim_customer, dim_product, dim_date.
Dynamic partition overwrite makes the nightly load idempotent: only partitions present in the load are replaced.
"""
from pyspark.sql import functions as F

from c360.config import settings as S
from spark.common import get_spark, path

DB = "c360_dw"


def main():
    spark = get_spark("c360-hive-load", hive=True)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB}")
    rd = lambda t: spark.read.format("delta").load(path(S.silver_root, t))

    cust = rd("dim_customer_scd2").filter("is_current")
    (cust.select(F.xxhash64("customer_id").alias("customer_key"), "customer_id", "name", "city", "state", "tier")
     .write.mode("overwrite").format("parquet").saveAsTable(f"{DB}.dim_customer"))
    prod = rd("products")
    (prod.select(F.xxhash64("product_id").alias("product_key"), "product_id", "category", "name",
                 F.col("price").alias("list_price"))
     .write.mode("overwrite").format("parquet").saveAsTable(f"{DB}.dim_product"))

    orders = rd("orders").withColumn("order_date", F.to_date("order_ts"))
    lo, hi = orders.agg(F.min("order_date"), F.max("order_date")).first()
    (spark.sql(f"SELECT explode(sequence(to_date('{lo}'), to_date('{hi}'), interval 1 day)) AS d")
     .select(F.date_format("d", "yyyyMMdd").cast("int").alias("date_key"), F.col("d").alias("full_date"),
             F.year("d").alias("year"), F.month("d").alias("month"), F.dayofmonth("d").alias("day"),
             F.date_format("d", "E").alias("weekday"), F.weekofyear("d").alias("week"),
             F.quarter("d").alias("quarter"), F.dayofweek("d").isin(1, 7).alias("is_weekend"))
     .write.mode("overwrite").format("parquet").saveAsTable(f"{DB}.dim_date"))

    fact = (rd("order_items").join(orders.select("order_id", "customer_id", "status", "order_date"), "order_id")
            .select("order_id", "item_no", F.xxhash64("customer_id").alias("customer_key"),
                    F.xxhash64("product_id").alias("product_key"),
                    F.date_format("order_date", "yyyyMMdd").cast("int").alias("date_key"),
                    "quantity", "unit_price", (F.col("quantity") * F.col("unit_price")).alias("line_amount"),
                    F.col("status").alias("order_status"), "order_date"))
    (fact.write.mode("overwrite").format("parquet").partitionBy("order_date").saveAsTable(f"{DB}.fact_orders"))
    spark.sql(f"SHOW PARTITIONS {DB}.fact_orders").show(5)


if __name__ == "__main__":
    main()
