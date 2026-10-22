"""Shared Spark helpers. Submit with scripts/submit.sh (adds Delta + Kafka packages)."""
import os

from pyspark.sql import SparkSession

from c360.config import settings as S

DELTA_PKG = "io.delta:delta-spark_2.12:3.1.0"
KAFKA_PKG = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
MYSQL_PKG = "com.mysql:mysql-connector-j:8.3.0"


def get_spark(name: str, hive: bool = False, shuffle_partitions: int = 8) -> SparkSession:
    b = (SparkSession.builder.appName(name)
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.hadoop.fs.defaultFS", os.environ.get("C360_DEFAULT_FS", "file:///"))
         .config("spark.sql.warehouse.dir", os.path.abspath(os.environ.get("C360_WAREHOUSE", "data/lake/_warehouse")))
         .config("spark.sql.streaming.stateStore.providerClass",
                 "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider"))
    if "SPARK_MASTER" in os.environ:
        b = b.master(os.environ["SPARK_MASTER"])
    if hive:
        b = b.enableHiveSupport()
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))
    return spark


def path(root: str, *parts: str) -> str:
    return "/".join([root.rstrip("/"), *parts])
