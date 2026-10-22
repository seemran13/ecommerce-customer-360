#!/usr/bin/env bash
set -euo pipefail
hdfs dfs -mkdir -p /landing/weblogs /landing/tickets
hdfs dfs -put -f "${1:-data/weblogs}"/dt=* /landing/weblogs/
hdfs dfs -put -f "${2:-data/tickets}"/*.txt /landing/tickets/
