#!/usr/bin/env python3
"""Replay historical clickstream JSON into Kafka `cart_events`.

When looping, timestamps are rebased each cycle so Kafka event time never moves
backwards. A shared clock file lets the heartbeat producer follow replay time
instead of injecting UTC-now heartbeats into a historical backfill.
Malformed JSON lines are forwarded untouched for downstream quarantine tests.
"""
import argparse
import glob
import json
import os
import time
from datetime import datetime, timezone, timedelta

from producers_common import make_producer

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CLOCK_FILE = os.getenv("C360_REPLAY_CLOCK_FILE", os.path.join(PROJECT_ROOT, ".c360_replay_clock.json"))


def parse_event_time(line):
    try:
        value = json.loads(line).get("event_ts")
        if not value:
            return None
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        return None


def write_replay_clock(active, event_time):
    payload = {
        "active": bool(active),
        "event_ts": event_time.astimezone(timezone.utc).replace(tzinfo=None).isoformat(timespec="seconds") if event_time else None,
        "updated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    os.makedirs(os.path.dirname(CLOCK_FILE) or ".", exist_ok=True)
    temporary = CLOCK_FILE + ".tmp"
    with open(temporary, "w", encoding="utf-8") as handle:
        json.dump(payload, handle)
    os.replace(temporary, CLOCK_FILE)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--src", default="data/weblogs")
    parser.add_argument("--topic", default="cart_events")
    parser.add_argument("--speedup", type=float, default=100.0)
    parser.add_argument("--loop", action="store_true", help="replay continuously; event timestamps are rebased monotonically each cycle")
    parser.add_argument("--from-date", default=None)
    args = parser.parse_args()
    if args.speedup <= 0:
        parser.error("--speedup must be greater than zero")

    files = sorted(glob.glob(os.path.join(args.src, "dt=*", "events.json")))
    if args.from_date:
        files = [f for f in files if f.split("dt=", 1)[1][:10] >= args.from_date]
    if not files:
        parser.error(f"no events.json files found under {args.src!r} (check --src and --from-date)")

    producer = make_producer()
    previous_cycle_end = None
    total_sent = 0
    latest_event_time = None
    write_replay_clock(True, None)

    try:
        while True:
            previous_in_cycle = None
            source_cycle_start = None
            target_cycle_start = None
            sent_this_cycle = 0
            latest_event_time = previous_cycle_end
            write_replay_clock(True, latest_event_time)

            for path in files:
                with open(path, encoding="utf-8") as handle:
                    for original_line in handle:
                        line = original_line.strip()
                        if not line:
                            continue

                        parsed_time = parse_event_time(line)
                        payload = None
                        try:
                            payload = json.loads(line)
                        except Exception:
                            pass

                        if parsed_time and args.loop:
                            if source_cycle_start is None:
                                source_cycle_start = parsed_time
                                target_cycle_start = (previous_cycle_end + timedelta(seconds=1)) if previous_cycle_end else parsed_time
                            shifted_time = target_cycle_start + (parsed_time - source_cycle_start)
                            if payload is not None:
                                payload["event_ts"] = shifted_time.replace(tzinfo=None).isoformat(timespec="seconds")
                                line = json.dumps(payload, separators=(",", ":"))
                            parsed_time = shifted_time

                        event_seconds = parsed_time.timestamp() if parsed_time else None
                        if event_seconds is not None and previous_in_cycle is not None and event_seconds > previous_in_cycle:
                            time.sleep(min((event_seconds - previous_in_cycle) / args.speedup, 1.0))
                        if event_seconds is not None:
                            previous_in_cycle = event_seconds
                            latest_event_time = parsed_time

                        key = payload.get("customer_id") if isinstance(payload, dict) else None
                        producer.send(args.topic, key=key, value=line.encode("utf-8"))
                        sent_this_cycle += 1
                        total_sent += 1

                        if sent_this_cycle % 1000 == 0:
                            # Publish/ack the events before exposing their timestamp to the
                            # heartbeat process, so ticks cannot outrun buffered producer data.
                            producer.flush()
                            write_replay_clock(True, latest_event_time)
                        if total_sent % 50000 == 0:
                            print(f"sent {total_sent}", flush=True)

            producer.flush()
            write_replay_clock(not args.loop, latest_event_time)
            print(f"replay finished this_cycle={sent_this_cycle} total_sent={total_sent} latest_event_ts={latest_event_time}", flush=True)
            if not args.loop:
                break
            previous_cycle_end = latest_event_time
    finally:
        try:
            producer.flush()
            producer.close()
        finally:
            write_replay_clock(False, latest_event_time)


if __name__ == "__main__":
    main()
