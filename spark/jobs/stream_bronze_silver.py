"""
Kafka -> Bronze (raw, replayable) -> Silver (parsed + globally de-duplicated)

 bronze/kafka_cart_events : key,value,topic,partition,offset,timestamp   (never modified; reconciliation anchor)
 silver/cart_events       : typed events, MERGE-inserted on event_id  => duplicates (retries, replays) dropped
"""
import argparse

from pyspark.sql import functions as F

from c360.config import settings as S
from spark.common import get_spark, path
from spark.delta_utils import insert_if_absent
from spark.schemas import CART_EVENT


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--starting", default="earliest", choices=["latest", "earliest"])
    ap.add_argument("--trigger", type=int, default=10)
    ap.add_argument("--once", action="store_true", help="process what is available then stop (batch-style)")
    a = ap.parse_args()
    spark = get_spark("c360-bronze-silver")
    trig = {"availableNow": True} if a.once else {"processingTime": f"{a.trigger} seconds"}

    spark.createDataFrame([], "key string, value string, topic string, partition int, offset long, timestamp timestamp") \
        .write.format("delta").mode("ignore").save(path(S.bronze_root, "kafka_cart_events"))

    raw = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", S.kafka_bootstrap)
           .option("subscribe", S.topic_cart).option("startingOffsets", a.starting)
           .option("failOnDataLoss", "true").load()
           .select(F.col("key").cast("string").alias("key"), F.col("value").cast("string").alias("value"),
                   "topic", "partition", "offset", "timestamp"))
    q1 = (raw.writeStream.format("delta").option("checkpointLocation", path(S.checkpoint_root, "bronze_cart"))
          .trigger(**trig).queryName("bronze_cart").start(path(S.bronze_root, "kafka_cart_events")))

    def to_silver(df, batch_id):
        parsed = (df.select(F.from_json("value", CART_EVENT).alias("e"), "partition", "offset").select("e.*", "partition", "offset")
                  .filter("event_id is not null").dropDuplicates(["event_id"]))
        insert_if_absent(spark, parsed, path(S.silver_root, "cart_events"), ["event_id"])

    bronze_stream = spark.readStream.format("delta").load(path(S.bronze_root, "kafka_cart_events"))
    q2 = (bronze_stream.writeStream.foreachBatch(to_silver)
          .option("checkpointLocation", path(S.checkpoint_root, "silver_cart")).trigger(**trig)
          .queryName("silver_cart").start())
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
