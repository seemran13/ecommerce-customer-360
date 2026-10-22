#!/usr/bin/env bash
# 3 brokers, RF=3, min.insync=2 => any single broker can die with zero data loss (acks=all producers).
for t in cart_events:12 cart_alerts:6 brand_mentions:3 weather:3 drill_events:6; do
  name=${t%%:*}; parts=${t##*:}
  docker exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092 --create --if-not-exists \
    --topic "$name" --partitions "$parts" --replication-factor 3 --config min.insync.replicas=2 --config retention.ms=604800000
done
