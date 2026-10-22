"""Historical replay (Olist-style events.csv or Retailrocket via the adapter) -> cart_events at Nx speed.

Event time is re-stamped to *now* (original kept in orig_ts) so the streaming job's processing-time
timeouts behave as in production, and `speed` is carried on every event so the detector compresses the idle
threshold by the same factor (15 min / 100 = 9 s at 100x).  Event ids are stable -> replays dedupe.

    python -m producers.replay_producer --events data/source/events.csv --speed 100 --limit 20000
"""
import argparse
import time

import pandas as pd

from producers.cart_event import iso_now, stable_event_id
from producers.common import make_producer, send
from c360.config import settings as S


def replay(df: pd.DataFrame, speed: float, producer, topic=None, sleep=time.sleep, now=time.time):
    topic = topic or S.topic_cart
    df = df.sort_values("event_ts")
    prev, wall0, sent = None, now(), 0
    t_first = df["event_ts"].iloc[0]
    for r in df.itertuples():
        due = wall0 + (r.event_ts - t_first).total_seconds() / speed      # absolute schedule: no drift
        wait = due - now()
        if wait > 0:
            sleep(wait)
        ev = {"event_id": getattr(r, "event_id", None) or stable_event_id(r.customer_id, r.event_ts, r.event_type, r.product_id),
              "event_ts": iso_now(), "orig_ts": r.event_ts.isoformat(), "customer_id": r.customer_id,
              "session_id": r.session_id, "event_type": r.event_type,
              "product_id": None if pd.isna(r.product_id) else r.product_id,
              "quantity": int(r.quantity), "unit_price": float(r.unit_price or 0.0),
              "speed": float(speed), "source": "replay", "seq": None}
        send(producer, topic, r.customer_id, ev)
        sent += 1
    producer.flush()
    return sent


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", default="data/source/events.csv")
    ap.add_argument("--speed", type=float, default=100.0)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--loop", action="store_true", help="repeat forever (24/7 generation)")
    a = ap.parse_args()
    df = pd.read_csv(a.events, parse_dates=["event_ts"])
    if a.limit:
        df = df.head(a.limit)
    p = make_producer("replay")
    while True:
        print("replayed", replay(df, a.speed, p))
        if not a.loop:
            break
