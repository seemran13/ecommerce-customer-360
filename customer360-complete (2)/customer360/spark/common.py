from pyspark.sql import SparkSession
from pyspark.sql.types import *

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()), StructField("event_ts", StringType()), StructField("session_id", StringType()),
    StructField("customer_id", LongType()), StructField("event_type", StringType()), StructField("product_id", LongType()),
    StructField("price", DoubleType()), StructField("qty", IntegerType()), StructField("order_id", LongType()),
    StructField("device", StringType()), StructField("ip", StringType())])

def spark(app, extra=None):
    b = (SparkSession.builder.appName(app).config("spark.sql.shuffle.partitions", "64")
         .config("spark.sql.adaptive.enabled", "true").enableHiveSupport())
    for k, v in (extra or {}).items(): b = b.config(k, v)
    return b.getOrCreate()
