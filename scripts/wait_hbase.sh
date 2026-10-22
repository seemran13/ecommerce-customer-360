#!/usr/bin/env bash
# waits until HBase Thrift answers on :9090 (max 4 minutes)
for i in $(seq 1 48); do
  if python - <<'PY' 2>/dev/null
import happybase, sys
try:
    happybase.Connection("localhost", 9090).tables(); sys.exit(0)
except Exception:
    sys.exit(1)
PY
  then echo "HBase ready"; exit 0; fi
  echo "waiting for HBase... ($((i*5))s)"; sleep 5
done
echo "HBase did not come up - run: docker logs hbase --tail 30"; exit 1
