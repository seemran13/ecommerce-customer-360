#!/usr/bin/env bash
# Sqoop 1.4.7 incremental `lastmodified` import  MySQL -> HDFS raw landing zone.
# NOTE: Apache Sqoop was moved to the Apache Attic (2021). It is kept because the capstone specifies it;
# spark/jobs/jdbc_incremental.py is the maintained equivalent (see docs/DESIGN_DECISIONS.md #5).
# A *saved job* stores --last-value in the Sqoop metastore, so every nightly run only pulls new/changed rows.
set -euo pipefail
JDBC="jdbc:mysql://${MYSQL_HOST:-localhost}:3306/${MYSQL_DB:-shop}"
USER="${MYSQL_USER:-c360}"; PASS_FILE="${SQOOP_PASS_FILE:-/user/${USER}/mysql.pwd}"   # hdfs file, chmod 400
RAW="${RAW_ROOT:-hdfs://namenode:9000/c360/raw}"
declare -A KEY=( [customers]=customer_id [products]=product_id [orders]=order_id )

for T in customers products orders; do
  JOB="c360_${T}_incr"
  if ! sqoop job --list | grep -qw "$JOB"; then
    sqoop job --create "$JOB" -- import \
      --connect "$JDBC" --username "$USER" --password-file "$PASS_FILE" \
      --table "$T" --target-dir "$RAW/$T" \
      --incremental lastmodified --check-column updated_at --last-value "1970-01-01 00:00:00" \
      --merge-key "${KEY[$T]}" --num-mappers 2 --as-textfile --fields-terminated-by ','
  fi
  sqoop job --exec "$JOB"
done
# order_items has a composite key; it is append-only so we import by update time without merge:
sqoop import --connect "$JDBC" --username "$USER" --password-file "$PASS_FILE" --table order_items \
  --target-dir "$RAW/order_items/$(date +%Y%m%d%H%M)" --where "updated_at >= NOW() - INTERVAL 1 DAY" \
  --num-mappers 1 --as-textfile --fields-terminated-by ','
