#!/usr/bin/env bash
# Load generated CSVs into MySQL (run once). Requires local_infile=1.
set -euo pipefail
D=${1:-data/mysql}
H=${MYSQL_HOST:-127.0.0.1}
MYSQL_USER=${MYSQL_USER:-root}
MYSQL_PWD_VALUE=${MYSQL_PWD:-root}

mysql -h"$H" -u"$MYSQL_USER" -p"$MYSQL_PWD_VALUE" < sql/mysql_schema.sql
for t in customers products orders order_items reviews; do
  mysql -h"$H" -u"$MYSQL_USER" -p"$MYSQL_PWD_VALUE" --local-infile=1 shop -e \
    "LOAD DATA LOCAL INFILE '$D/$t.csv' INTO TABLE $t FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' IGNORE 1 LINES;"
done

# Simulate nightly customer changes so Sqoop's last_modified job sees updates.
# Escape CSV values as SQL literals and pass credentials as argv, not interpolated
# source code, so quotes/backslashes in addresses and names don't break the update.
python3 - "$D/customers_delta.csv" "$H" "$MYSQL_USER" "$MYSQL_PWD_VALUE" <<'PY'
import csv
import subprocess
import sys

csv_path, host, user, password = sys.argv[1:]

def sql_quote(value):
    text = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return "'" + text + "'"

with open(csv_path, newline="", encoding="utf-8") as handle:
    rows = list(csv.DictReader(handle))

statements = []
for row in rows:
    customer_id = int(row["customer_id"])
    statements.append(
        "UPDATE customers SET "
        f"tier={sql_quote(row['tier'])}, "
        f"address={sql_quote(row['address'])}, "
        f"last_modified={sql_quote(row['last_modified'])} "
        f"WHERE customer_id={customer_id};"
    )

subprocess.run(
    ["mysql", f"-h{host}", f"-u{user}", f"-p{password}", "shop"],
    input=("\n".join(statements) + "\n").encode("utf-8"),
    check=True,
)
print(f"MYSQL_DELTA_APPLIED rows={len(rows)}")
PY
