#!/usr/bin/env python3
"""Consume cart alerts with restart-persistent alert_id deduplication.

The email/SMS senders are demo stubs by default. Set SMTP/Twilio configuration
and replace `send()` to perform actual delivery. Delivery and Kafka offset commits
are still at-least-once across a crash in the small window after sending.
"""
import collections
import json
import os
import sqlite3
import time

import numpy as np
from kafka import KafkaConsumer, TopicPartition
from kafka.structs import OffsetAndMetadata

IDLE = int(os.getenv("IDLE_SEC", 15 * 60))
DB_PATH = os.getenv("NOTIFIER_DB", "data/notifier_seen.sqlite3")


def send(channel, customer_id, cart_value):
    print(
        f"[{channel}] customer={customer_id} -> "
        f"'You left items worth {cart_value:.2f} in your cart. Complete your order today!'",
        flush=True,
    )


def main():
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    db = sqlite3.connect(DB_PATH, timeout=30)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("CREATE TABLE IF NOT EXISTS processed_alerts (alert_id TEXT PRIMARY KEY, processed_at REAL NOT NULL)")
    db.commit()

    consumer = KafkaConsumer(
        "cart_alerts",
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092").split(","),
        group_id="notifier",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda b: json.loads(b),
    )
    latencies = collections.deque(maxlen=5000)
    try:
        for message in consumer:
            alert = message.value
            alert_id = str(alert["alert_id"])
            seen = db.execute("SELECT 1 FROM processed_alerts WHERE alert_id = ?", (alert_id,)).fetchone()
            offset = {TopicPartition(message.topic, message.partition): OffsetAndMetadata(message.offset + 1, None)}
            if seen:
                consumer.commit(offsets=offset)
                continue

            # Do not commit the Kafka offset if delivery raises: the message can be retried.
            send("email" if int(alert["customer_id"]) % 2 else "sms", alert["customer_id"], float(alert["cart_value"]))
            db.execute(
                "INSERT OR IGNORE INTO processed_alerts(alert_id, processed_at) VALUES (?, ?)",
                (alert_id, time.time()),
            )
            db.commit()
            consumer.commit(offsets=offset)

            latencies.append(time.time() - (int(alert["last_event_ts"]) + IDLE))
            if len(latencies) and len(latencies) % 100 == 0:
                p95 = float(np.percentile(latencies, 95))
                print(f"alert latency p95={p95:.1f}s", "OK" if p95 < 60 else "SLA BREACH", flush=True)
    finally:
        consumer.close()
        db.close()


if __name__ == "__main__":
    main()
