#!/usr/bin/env python3
"""Safe event-time ticks for Customer360 streaming.

During a historical replay, ticks follow the latest replay event timestamp and
never jump ahead to UTC-now. After replay, the event-time clock advances by one
second per real second. Set C360_HEARTBEAT_WALL_CLOCK=1 only for a live-only run
or after the historical Kafka backlog has fully drained.
"""
import json
import os
import time
import uuid
from datetime import datetime, timezone, timedelta

from producers_common import make_producer

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CLOCK_FILE = os.getenv("C360_REPLAY_CLOCK_FILE", os.path.join(PROJECT_ROOT, ".c360_replay_clock.json"))
WALL_CLOCK = os.getenv("C360_HEARTBEAT_WALL_CLOCK", "0").strip().lower() in {"1", "true", "yes", "on"}
INTERVAL = max(0.2, float(os.getenv("C360_HEARTBEAT_INTERVAL", "1")))


def parse_clock(value):
    if not value:
        return None
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def read_replay_clock():
    try:
        with open(CLOCK_FILE, encoding="utf-8") as handle:
            return json.load(handle)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return None


def main():
    producer = make_producer()
    last_sent_time = None
    try:
        while True:
            clock = read_replay_clock()
            event_time = None

            if clock is None:
                # Do not accidentally inject UTC-now while a replay is about to start.
                if WALL_CLOCK:
                    event_time = datetime.now(timezone.utc)
                else:
                    time.sleep(INTERVAL)
                    continue
            else:
                latest = parse_clock(clock.get("event_ts"))
                if clock.get("active", False):
                    # While replay is active, never put the watermark ahead of its source data.
                    event_time = latest
                    if event_time is None or (last_sent_time is not None and event_time <= last_sent_time):
                        time.sleep(INTERVAL)
                        continue
                elif WALL_CLOCK:
                    candidates = [datetime.now(timezone.utc)]
                    if latest is not None:
                        candidates.append(latest)
                    if last_sent_time is not None:
                        candidates.append(last_sent_time)
                    event_time = max(candidates)
                elif latest is not None:
                    base = max(v for v in (latest, last_sent_time) if v is not None) if last_sent_time else latest
                    event_time = base + timedelta(seconds=1)
                else:
                    time.sleep(INTERVAL)
                    continue

            event = {
                "event_id": f"watermark-{uuid.uuid4()}",
                "event_ts": event_time.astimezone(timezone.utc).replace(tzinfo=None).isoformat(timespec="seconds"),
                "session_id": "__watermark__",
                "customer_id": 0,
                "event_type": "watermark_tick",
                "product_id": None,
                "price": None,
                "qty": None,
                "order_id": None,
                "device": "system",
                "ip": None,
            }
            producer.send("cart_events", key=0, value=event)
            producer.flush()
            last_sent_time = event_time
            time.sleep(INTERVAL)
    finally:
        try:
            producer.flush()
            producer.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
