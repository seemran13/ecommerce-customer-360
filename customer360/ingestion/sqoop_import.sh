#!/usr/bin/env bash
# Idempotent nightly Sqoop imports with saved high-water marks.
# Requires Sqoop metastore and a protected local password file on the Sqoop client node.
set -euo pipefail
JDBC="jdbc:mysql://${MYSQL_HOST:-mysql}:${MYSQL_PORT:-3306}/${MYSQL_DATABASE:-shop}"
MYSQL_USER=${MYSQL_USER:-root}
PASSWORD_FILE=${SQOOP_PASSWORD_FILE:-/etc/customer360/mysql.pw}
RUN_TS=${1:-$(date +%F)}
INITIAL_LASTMODIFIED=${SQOOP_INITIAL_LASTMODIFIED:-1970-01-01 00:00:00}

if [[ ! -r "$PASSWORD_FILE" ]]; then
  echo "Sqoop password file is missing/unreadable: $PASSWORD_FILE" >&2
  echo "Create a local file owned by the Sqoop user (chmod 600) or set SQOOP_PASSWORD_FILE." >&2
  exit 2
fi

create_lastmodified_job() {
  local table="$1" job="inc_$1" target="/landing/mysql/$1"
  if ! sqoop job --show "$job" >/dev/null 2>&1; then
    echo "Creating saved Sqoop job $job (initial last_modified=$INITIAL_LASTMODIFIED)"
    sqoop job --create "$job" -- import \
      --connect "$JDBC" --username "$MYSQL_USER" --password-file "$PASSWORD_FILE" \
      --table "$table" --target-dir "$target" \
      --incremental lastmodified --check-column last_modified --last-value "$INITIAL_LASTMODIFIED" \
      --num-mappers 1 --as-avrodatafile
  fi
  sqoop job --exec "$job"
}

for table in orders customers products; do
  create_lastmodified_job "$table"
done

# order_items has a composite (order_id, product_id) primary key but no modified timestamp.
# A saved append job persists its last order_id; using a one-shot --last-value 0 would reload
# every row on every nightly run and create duplicates.
if ! sqoop job --show inc_order_items >/dev/null 2>&1; then
  sqoop job --create inc_order_items -- import \
    --connect "$JDBC" --username "$MYSQL_USER" --password-file "$PASSWORD_FILE" \
    --table order_items --target-dir /landing/mysql/order_items \
    --incremental append --check-column order_id --last-value 0 \
    --num-mappers 1 --as-avrodatafile
fi
sqoop job --exec inc_order_items

echo "SQOOP_INCREMENTAL_IMPORT=PASS run_date=$RUN_TS jobs=inc_orders,inc_customers,inc_products,inc_order_items"
