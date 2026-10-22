#!/usr/bin/env bash
# Drill 2 - kill -9 the streaming job mid-run, restart it, verify checkpoint recovery + exactly-once effect.
# Requires: HBase + Kafka up.  The detector runs with a 30 s idle threshold so the drill is short.
set -euo pipefail
cd "$(dirname "$0")/.."; export PYTHONPATH="$PWD"
RUN="sp$(date +%H%M%S)"
start_job() { nohup scripts/submit.sh spark/jobs/stream_cart_abandonment.py --idle 30 --trigger 5 --starting earliest > /tmp/c360_stream.log 2>&1 & }
start_job; sleep 45
python -m drills.scenario_producer --run "$RUN" --carts 600 --rate 60 &
PROD=$!
sleep 12; echo ">> kill -9 streaming job"; pkill -9 -f stream_cart_abandonment || true
wait $PROD; sleep 10
echo ">> restart from checkpoint"; start_job
sleep 120                      # state is restored; overdue timeouts fire on the first batch
scripts/submit.sh drills/verify_alerts.py "$RUN"
