#!/usr/bin/env bash
# DRILL 1: kill one broker while producing to an RF=3 topic with min ISR=2.
# Verifies every unique sequence ID can be read back without loss.
set -euo pipefail
N=${1:-200000}
if ! [[ "$N" =~ ^[1-9][0-9]*$ ]]; then echo "N must be a positive integer" >&2; exit 2; fi
TOPIC="drill_events_$(date +%Y%m%d%H%M%S)_$RANDOM"
BROKER_STOPPED=0
PASS=0

cleanup() {
  if [[ "$BROKER_STOPPED" == "1" ]]; then docker start kafka2 >/dev/null 2>&1 || true; fi
  if [[ "$PASS" == "1" ]]; then
    docker exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092 --delete --topic "$TOPIC" >/dev/null 2>&1 || true
  else
    echo "Diagnostic topic retained for investigation: $TOPIC" >&2
  fi
}
trap cleanup EXIT

docker exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092 --create --if-not-exists \
  --topic "$TOPIC" --partitions 6 --replication-factor 3 --config min.insync.replicas=2 >/dev/null

echo "Producing $N acknowledged messages to $TOPIC; kafka2 will be stopped during production."
python3 - "$TOPIC" "$N" <<'PYDRILL' &
import sys, time
sys.path.insert(0, "ingestion")
from producers_common import make_producer

topic, n = sys.argv[1], int(sys.argv[2])
p = make_producer()
try:
    for i in range(n):
        p.send(topic, key=i, value={"seq": i})
        if i and i % 1000 == 0:
            p.flush(timeout=60)
            time.sleep(0.01)
    p.flush(timeout=120)
    print("PRODUCER_DONE", n, flush=True)
finally:
    p.close(timeout=10)
PYDRILL
PRODUCER_PID=$!
sleep 2
echo ">>> stopping kafka2 while producer is active"
docker stop kafka2 >/dev/null
BROKER_STOPPED=1
wait "$PRODUCER_PID"
echo ">>> consuming $N messages while kafka2 remains stopped"
set +e
docker exec kafka1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka1:9092 --topic "$TOPIC" \
  --from-beginning --max-messages "$N" --timeout-ms 60000 2>/dev/null | python3 -c '
import json, sys
expected=int(sys.argv[1]); seen=set(); malformed=0
for line in sys.stdin:
    line=line.strip()
    if not line: continue
    try: seen.add(int(json.loads(line)["seq"]))
    except Exception: malformed += 1
missing = sorted(set(range(expected)) - seen)
print(f"received_unique={len(seen)} expected={expected} missing={len(missing)} malformed={malformed}")
if missing[:10]: print("missing_sample=", missing[:10])
sys.exit(1 if len(seen) != expected or malformed else 0)
' "$N"
CONSUMER_STATUS=$?
set -e
if [[ "$CONSUMER_STATUS" -ne 0 ]]; then echo "FAIL: broker drill detected message loss or malformed payloads." >&2; exit "$CONSUMER_STATUS"; fi
echo "PASS_BROKER_KILL_ZERO_LOSS topic=$TOPIC messages=$N"
PASS=1
