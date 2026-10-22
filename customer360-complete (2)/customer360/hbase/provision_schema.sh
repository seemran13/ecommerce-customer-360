#!/usr/bin/env bash
# Idempotently ensure customer_360 exists with every family used by batch and stream sinks.
# Run from the project root on the Docker host. This adds families but never drops rows.
set -euo pipefail
HBASE_CONTAINER=${HBASE_CONTAINER:-hbase}
SCHEMA_FILE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/provision_schema.hbase"
if ! command -v docker >/dev/null 2>&1; then
  echo "docker CLI not found; run the HBase shell file manually inside HBase." >&2
  exit 1
fi
if [[ "$(docker inspect -f '{{.State.Running}}' "$HBASE_CONTAINER" 2>/dev/null || true)" != "true" ]]; then
  echo "HBase container '$HBASE_CONTAINER' is not running." >&2
  exit 1
fi
docker exec -i "$HBASE_CONTAINER" /hbase/bin/hbase shell -n < "$SCHEMA_FILE"
echo "PASS_HBASE_CUSTOMER360_SCHEMA families=profile,metrics,cart"
