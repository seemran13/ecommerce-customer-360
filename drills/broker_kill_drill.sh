#!/usr/bin/env bash
# Drill 1 - stop 1 of 3 Kafka brokers while producing; verify zero data loss.
set -euo pipefail
cd "$(dirname "$0")/.."; export PYTHONPATH="$PWD"
RUN="bk$(date +%H%M%S)"
python -m drills.scenario_producer --run "$RUN" --carts 600 --rate 100 &   # ~ 25 s of traffic
PID=$!
sleep 8;  echo ">> stopping kafka2";  docker stop kafka2
wait $PID
echo ">> topic state with one broker down:"; docker exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:19092 --describe --topic cart_events | head -8
echo ">> restarting kafka2"; docker start kafka2; sleep 15
python -m drills.verify_no_loss --run "$RUN"
