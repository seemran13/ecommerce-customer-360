#!/usr/bin/env bash
# Start optional brand-mention/weather Kafka-to-Parquet landing streams.
set -euo pipefail
SPARK_CONTAINER=${SPARK_CONTAINER:-infra-spark-1}
TOP=$(docker top "$SPARK_CONTAINER" 2>/dev/null) || { echo "Cannot inspect $SPARK_CONTAINER; is it running?" >&2; exit 1; }
if grep -q '/opt/customer360/spark/streaming_feeds.py' <<<"$TOP"; then
  echo "Customer360 feed stream already running in $SPARK_CONTAINER; refusing duplicate start." >&2
  exit 1
fi
docker exec -d "$SPARK_CONTAINER" bash -lc '
  export HOME=/tmp/spark-home
  export PYTHONPATH=/tmp/python-packages:/opt/spark/python:/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.7-src.zip:/opt/customer360/spark
  export PYSPARK_PYTHON=python3
  mkdir -p /tmp/spark-ivy
  /opt/spark/bin/spark-submit --master "local[2]" --driver-memory 512m \
    --conf spark.sql.shuffle.partitions=2 --conf spark.sql.adaptive.enabled=false \
    --conf spark.jars.ivy=/tmp/spark-ivy \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9 \
    /opt/customer360/spark/streaming_feeds.py > /tmp/streaming_feeds.log 2>&1
'
echo "Started Customer360 brand/weather feed landing; log: /tmp/streaming_feeds.log"
