"""Recovery notifier: consumes cart_alerts, de-duplicates on alert_id (SQLite), 'sends' email/SMS (pluggable),
and logs end-to-end SLA = notified_ts - (last_event_ts + idle_threshold).  Target: < 60 s.
Replace `send_message` with SES/Twilio/SMTP in production."""
import json
import logging
import sqlite3
import time

from c360.config import settings as S

log = logging.getLogger("notifier")


def send_message(alert: dict) -> None:
    log.info("RECOVERY MESSAGE -> customer=%s cart_value=%.2f items=%s", alert["customer_id"], alert["cart_value"], alert["item_count"])


def handle(alert: dict, db, now=time.time, sender=send_message) -> bool:
    """Returns True if sent, False if duplicate. Pure enough to unit-test with an in-memory sqlite db."""
    try:
        db.execute("INSERT INTO sent(alert_id) VALUES (?)", (alert["alert_id"],))   # PK -> atomic dedup
    except sqlite3.IntegrityError:
        return False
    sender(alert)
    t = now()
    sla = t - (alert["last_event_ts"] + alert["threshold_s"])
    db.execute("UPDATE sent SET notified_ts=?, sla_s=? WHERE alert_id=?", (t, sla, alert["alert_id"]))
    db.commit()
    if sla > S.alert_sla_s:
        log.warning("SLA BREACH %.1fs alert=%s", sla, alert["alert_id"])
    return True


def open_db(path="data/notifier.sqlite"):
    db = sqlite3.connect(path)
    db.execute("CREATE TABLE IF NOT EXISTS sent(alert_id TEXT PRIMARY KEY, notified_ts REAL, sla_s REAL)")
    return db


def main():
    from confluent_kafka import Consumer
    logging.basicConfig(level=logging.INFO)
    c = Consumer({"bootstrap.servers": S.kafka_bootstrap, "group.id": "notifier", "enable.auto.commit": False,
                  "auto.offset.reset": "earliest"})
    c.subscribe([S.topic_alerts])
    db = open_db()
    while True:
        m = c.poll(1.0)
        if m is None or m.error():
            continue
        handle(json.loads(m.value()), db)
        c.commit(m)          # commit after handling: at-least-once delivery + dedup => effectively once


if __name__ == "__main__":
    main()
