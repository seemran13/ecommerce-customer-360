"""
Bronze -> Silver (batch).
 * orders / order_items / products : keep the latest version per key (Sqoop/JDBC may re-deliver changed rows)
 * customers                       : **SCD Type 2** on (city, state, zip, tier)  -> silver/dim_customer_scd2
 * tickets / reviews               : trimmed, typed, de-duplicated
SCD2 uses the standard Delta two-row staging trick: changed customers appear twice in the merge source -
once with the real key (closes the current row) and once with a NULL key (inserts the new version).
"""
from delta.tables import DeltaTable
from pyspark.sql import Window, functions as F

from c360.config import settings as S
from spark.common import get_spark, path
from spark.schemas import KEYS

TRACKED = ["city", "state", "zip", "tier"]


def latest(df, keys):
    w = Window.partitionBy(*keys).orderBy(F.col("updated_at").desc(), F.col("_ingest_ts").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def scd2_customers(spark, src):
    dst = path(S.silver_root, "dim_customer_scd2")
    src = (src.withColumn("row_hash", F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c), F.lit("")) for c in TRACKED]), 256))
           .select("customer_id", "name", "email", *TRACKED, "created_at", "updated_at", "row_hash"))
    if not DeltaTable.isDeltaTable(spark, dst):
        (src.withColumn("effective_from", F.col("updated_at")).withColumn("effective_to", F.lit(None).cast("timestamp"))
         .withColumn("is_current", F.lit(True)).drop("updated_at").write.format("delta").save(dst))
        return
    tgt = DeltaTable.forPath(spark, dst)
    cur = tgt.toDF().filter("is_current").select("customer_id", F.col("row_hash").alias("cur_hash"))
    changed = src.join(cur, "customer_id").filter("row_hash <> cur_hash").drop("cur_hash")
    staged = (changed.withColumn("merge_key", F.lit(None).cast("string"))
              .unionByName(src.withColumn("merge_key", F.col("customer_id"))))
    (tgt.alias("t").merge(staged.alias("s"), "t.customer_id = s.merge_key AND t.is_current = true")
     .whenMatchedUpdate(condition="t.row_hash <> s.row_hash",
                        set={"is_current": "false", "effective_to": "s.updated_at"})
     .whenNotMatchedInsert(values={
         "customer_id": "s.customer_id", "name": "s.name", "email": "s.email", "city": "s.city",
         "state": "s.state", "zip": "s.zip", "tier": "s.tier", "created_at": "s.created_at",
         "row_hash": "s.row_hash", "effective_from": "s.updated_at", "effective_to": "null",
         "is_current": "true"}).execute())


def main():
    spark = get_spark("c360-silver")
    for t in ("orders", "order_items", "products", "customers"):
        b = spark.read.format("delta").load(path(S.bronze_root, t))
        l = latest(b, KEYS[t]).drop("_source_file")
        if t == "customers":
            scd2_customers(spark, l)
        else:
            l.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path(S.silver_root, t))
        print("silver", t, "done")
    try:
        tk = spark.read.format("delta").load(path(S.bronze_root, "tickets"))
        (tk.withColumn("body", F.trim("body")).dropDuplicates(["ticket_id"]).write.format("delta")
         .mode("overwrite").save(path(S.silver_root, "tickets")))
        rv = spark.read.format("delta").load(path(S.bronze_root, "reviews"))
        (rv.dropDuplicates(["review_id"]).write.format("delta").mode("overwrite").save(path(S.silver_root, "reviews")))
    except Exception as e:   # tickets/reviews are optional inputs
        print("tickets/reviews skipped:", e)


if __name__ == "__main__":
    main()
