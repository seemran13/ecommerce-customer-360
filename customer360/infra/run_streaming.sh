#!/usr/bin/env bash
# Launch the Customer360 live stream inside the Compose Spark container.
# Uses a new checkpoint root to preserve state from the previous query topology.
set -euo pipefail
SPARK_CONTAINER=${SPARK_CONTAINER:-infra-spark-1}

# Avoid duplicate Structured Streaming drivers writing to the same checkpoint.
TOP=$(docker top "$SPARK_CONTAINER" 2>/dev/null) || { echo "Cannot inspect $SPARK_CONTAINER; is it running?" >&2; exit 1; }
if grep -q '/opt/customer360/spark/streaming_abandonment.py' <<<"$TOP"; then
  echo "A Customer360 production stream is already running in $SPARK_CONTAINER; refusing to start a duplicate." >&2
  exit 1
fi

# Dependencies and the Spark-Kafka connector are kept explicit so the Python
# callback and Structured Streaming use the same Spark 3.5.9 runtime.
docker exec -d "$SPARK_CONTAINER" bash -lc '
  export HOME=/tmp/spark-home
  export PYTHONPATH=/tmp/python-packages:/opt/spark/python:/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.7-src.zip:/opt/customer360/spark
  export PYSPARK_PYTHON=python3
  export CKPT="${CKPT:-/checkpoints/abandonment_v2}"
  export MAX_OFFSETS_PER_TRIGGER="${MAX_OFFSETS_PER_TRIGGER:-3000}"
  export SPARK_STREAMING_SHUFFLE_PARTITIONS="${SPARK_STREAMING_SHUFFLE_PARTITIONS:-4}"
  mkdir -p /tmp/spark-ivy
  /opt/spark/bin/spark-submit \
    --master "local[2]" \
    --driver-memory 768m \
    --conf spark.sql.shuffle.partitions="$SPARK_STREAMING_SHUFFLE_PARTITIONS" \
    --conf spark.sql.adaptive.enabled=false \
    --conf spark.jars.ivy=/tmp/spark-ivy \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9 \
    /opt/customer360/spark/streaming_abandonment.py \
    > /tmp/streaming_abandonment.log 2>&1
'
echo "Started Customer360 stream in $SPARK_CONTAINER; log: /tmp/streaming_abandonment.log"
echo "Default checkpoint: /checkpoints/abandonment_v2 (original checkpoint preserved)"
