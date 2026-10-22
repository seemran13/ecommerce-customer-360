#!/usr/bin/env bash
# DRILL 2: stop the production streaming driver and restart from its checkpoint.
# Matches only the Java SparkSubmit process, not the shell that contains this pattern.
set -euo pipefail
SPARK_CONTAINER=${SPARK_CONTAINER:-infra-spark-1}
STREAM_SCRIPT=${STREAM_SCRIPT:-/opt/customer360/spark/streaming_abandonment.py}

echo "Stopping production stream in $SPARK_CONTAINER..."
PIDS=$(docker exec "$SPARK_CONTAINER" bash -lc "ps -eo pid=,args= | awk -v script='$STREAM_SCRIPT' '\$2 ~ /java/ && /org.apache.spark.deploy.SparkSubmit/ && index(\$0, script) > 0 {print \$1}'")
if [[ -z "${PIDS// /}" ]]; then
  echo "FAIL: no Java SparkSubmit process for $STREAM_SCRIPT was found; refusing to proceed." >&2
  exit 1
fi
# shellcheck disable=SC2086
docker exec "$SPARK_CONTAINER" bash -lc "kill -TERM $PIDS"
for _ in $(seq 1 20); do
  sleep 1
  if docker exec "$SPARK_CONTAINER" bash -lc "ps -eo pid=,args= | awk -v script='$STREAM_SCRIPT' '\$2 ~ /java/ && /org.apache.spark.deploy.SparkSubmit/ && index(\$0, script) > 0 {found=1} END {exit found ? 1 : 0}'"; then
    break
  fi
done

echo "Checkpoint offsets before restart:"
docker exec "$SPARK_CONTAINER" bash -lc 'ls -1 /checkpoints/abandonment_v2/alerts_combined/offsets 2>/dev/null | tail -3 || true'
bash infra/run_streaming.sh
for _ in $(seq 1 90); do
  sleep 2
  if docker exec "$SPARK_CONTAINER" bash -lc "grep -q 'CUSTOMER360_STREAM_STARTED' /tmp/streaming_abandonment.log"; then
    echo "PASS: stream restarted from checkpoint; startup marker observed."
    docker exec "$SPARK_CONTAINER" bash -lc "grep -E 'CUSTOMER360_STREAM_STARTED|KAFKA_ALERT_BATCH|HBASE_BATCH|CUSTOMER360_STREAM_FATAL|QUERY_EXCEPTION' /tmp/streaming_abandonment.log | tail -n 20"
    exit 0
  fi
  if docker exec "$SPARK_CONTAINER" bash -lc "grep -qE 'CUSTOMER360_STREAM_FATAL|ERROR MicroBatchExecution|Query.*terminated with error' /tmp/streaming_abandonment.log"; then
    docker exec "$SPARK_CONTAINER" bash -lc 'tail -n 60 /tmp/streaming_abandonment.log'
    echo "FAIL: restarted stream terminated during startup." >&2
    exit 1
  fi
done
docker exec "$SPARK_CONTAINER" bash -lc 'tail -n 60 /tmp/streaming_abandonment.log'
echo "FAIL: stream did not report CUSTOMER360_STREAM_STARTED within 180 seconds." >&2
exit 1
