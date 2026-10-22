"""Offline replay of an event log through the cart state machine, emulating the streaming job's timeouts.
Used by (a) the local demo, (b) the reconciliation job as the independent 'expected alerts' oracle, (c) tests."""
from collections import defaultdict
import math

from c360.core import cart_state as cs


def detect_alerts(events, idle_s, end_ts=None, trigger_s=0.0):
    """events: iterable of dicts with event_ts(float epoch), customer_id, event_type, ... Returns (alerts, in_flight).
    trigger_s>0 quantises detection to the next micro-batch boundary, emulating Spark's trigger interval."""
    by = defaultdict(list)
    for e in events:
        by[e["customer_id"]].append(e)
    alerts, inflight = [], 0
    for cid, evs in by.items():
        evs.sort(key=lambda e: e["event_ts"])
        st = None
        for e in evs:
            if st is not None:
                a = cs.expire(st, e["event_ts"], idle_s)
                if a:
                    alerts.append(_quantise(st, a, idle_s, trigger_s))
                    st = None
            st, _ = cs.apply_event(st, e)
        if st is not None and st["items"]:
            if end_ts is not None and (a := cs.expire(st, end_ts, idle_s)):
                alerts.append(_quantise(st, a, idle_s, trigger_s))
            else:
                inflight += 1
    return alerts, inflight


def _quantise(st, a, idle_s, trigger_s):
    deadline = st["last_event_ts"] + cs.effective_idle_s(st, idle_s)
    if trigger_s > 0:
        det = math.ceil(deadline / trigger_s) * trigger_s     # next trigger after the deadline
        a = {**a, "detected_ts": det, "alert_latency_s": round(det - deadline, 3)}
    else:
        a = {**a, "detected_ts": deadline, "alert_latency_s": 0.0}
    return a
