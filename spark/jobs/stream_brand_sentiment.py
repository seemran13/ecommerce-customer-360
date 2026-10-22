"""Kafka brand_mentions -> 1-minute tumbling windows per brand -> gold/brand_sentiment (MERGE, updated every 30 s)
Power BI reads this table; with DirectQuery + automatic page refresh the dashboard tile updates each minute."""
from pyspark.sql import functions as F

from c360.config import settings as S
from spark.common import get_spark, path
from spark.delta_utils import upsert
from spark.schemas import BRAND_MENTION


def main():
    spark = get_spark("c360-brand-sentiment")
    m = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", S.kafka_bootstrap)
         .option("subscribe", S.topic_brand).option("startingOffsets", "latest").load()
         .select(F.from_json(F.col("value").cast("string"), BRAND_MENTION).alias("m")).select("m.*")
         .filter("mention_id is not null"))
    agg = (m.dropDuplicates(["mention_id"]).withWatermark("event_ts", "2 minutes")
           .groupBy(F.window("event_ts", "1 minute"), "brand")
           .agg(F.count("*").alias("mentions"), F.round(F.avg("sentiment_score"), 4).alias("avg_sentiment"),
                F.sum((F.col("sentiment") == "positive").cast("int")).alias("positive"),
                F.sum((F.col("sentiment") == "negative").cast("int")).alias("negative"))
           .select(F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"),
                   "brand", "mentions", "avg_sentiment", "positive", "negative"))
    (agg.writeStream.outputMode("update")
     .foreachBatch(lambda d, i: upsert(spark, d, path(S.gold_root, "brand_sentiment"), ["window_start", "brand"]))
     .option("checkpointLocation", path(S.checkpoint_root, "brand_sentiment"))
     .trigger(processingTime="30 seconds").start().awaitTermination())


if __name__ == "__main__":
    main()
