from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession


def upsert(spark: SparkSession, df: DataFrame, path: str, keys: list):
    """Idempotent MERGE (insert-or-update on `keys`). Re-running a micro-batch yields the same table state,
    which is what turns Spark's at-least-once foreachBatch into an exactly-once *effect*."""
    if not DeltaTable.isDeltaTable(spark, path):
        df.limit(0).write.format("delta").mode("append").save(path)
    cond = " AND ".join(f"t.{k} <=> s.{k}" for k in keys)
    (DeltaTable.forPath(spark, path).alias("t").merge(df.alias("s"), cond)
     .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())


def insert_if_absent(spark: SparkSession, df: DataFrame, path: str, keys: list):
    if not DeltaTable.isDeltaTable(spark, path):
        df.limit(0).write.format("delta").mode("append").save(path)
    cond = " AND ".join(f"t.{k} <=> s.{k}" for k in keys)
    (DeltaTable.forPath(spark, path).alias("t").merge(df.alias("s"), cond)
     .whenNotMatchedInsertAll().execute())
