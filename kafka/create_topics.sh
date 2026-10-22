#!/usr/bin/env bash
# RF=3 + min.insync.replicas=2 + producers using acks=all  => one broker can die with zero data loss.
set -euo pipefail
BS="${KAFKA_INTERNAL_BOOTSTRAP:-kafka1:19092}"
mk() { docker exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BS" --create --if-not-exists \
        --topic "$1" --partitions "$2" --replication-factor 3 --config min.insync.replicas=2 \
        --config retention.ms="$3"; }
mk cart_events     6 604800000    # 7 d  - keyed by customer_id so per-customer order is preserved
mk brand_mentions  3 259200000    # 3 d  - Bluesky Jetstream, keyed by brand
mk weather_events  1 259200000    # 3 d  - Open-Meteo
mk cart_alerts     3 604800000    # 7 d  - detector output, keyed by alert_id
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BS" --describe
