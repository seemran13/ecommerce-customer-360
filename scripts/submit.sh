#!/usr/bin/env bash
# usage: scripts/submit.sh spark/jobs/batch_cleanse.py [args...]
set -euo pipefail
cd "$(dirname "$0")/.."
export PYTHONPATH="$PWD:${PYTHONPATH:-}"
JOB="$1"; shift
exec spark-submit --master "${SPARK_MASTER:-local[*]}" \
  --packages io.delta:delta-spark_2.12:3.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.mysql:mysql-connector-j:8.3.0 \
  --conf spark.driver.extraJavaOptions="-Dfile.encoding=UTF-8" \
  "$JOB" "$@"
