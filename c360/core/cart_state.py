"""
Pure-Python abandoned-cart state machine.

The Spark job (spark/jobs/stream_cart_abandonment.py) wraps these functions in
applyInPandasWithState; keeping the logic here means it is unit-testable without Spark
and can be reused by the local simulator (scripts/demo_local.py).

Design (resolves open item #1 of the design doc):
  A fixed 10-minute window can never observe a 15-minute idle period, and Spark's
  session_window only closes after the *watermark* passes, which stalls when traffic stops.
  So we keep one cart state per customer and fire a *processing-time timeout* whose deadline
  is anchored to the last event time:   deadline = last_event_ts + idle_threshold.
  The alert therefore fires at the first micro-batch after the deadline -> latency is bounded
  by the trigger interval (default 5 s), far inside the 60 s SLA.
"""
from __future__ import annotations

import hashlib
import json
from typing import Optional, Tuple

ACTIVE, CONVERTED, EMPTY, DUPLICATE = "ACTIVE", "CONVERTED", "EMPTY", "DUPLICATE"
SEEN_MAX = 500   # bounded per-customer memory of recent event_ids (stateful de-duplication)


def cart_id_for(customer_id: str, first_ts: float) -> str:
    return f"{customer_id}-{int(first_ts)}"


def alert_id_for(cart_id: str, last_event_ts: float) -> str:
    """Deterministic -> MERGE on alert_id makes sink writes idempotent across retries/restarts."""
    return hashlib.sha1(f"{cart_id}|{last_event_ts:.3f}".encode()).hexdigest()[:24]


def new_state(customer_id: str, ts: float, speed: float = 1.0) -> dict:
    return {"customer_id": customer_id, "cart_id": cart_id_for(customer_id, ts), "items": {},
            "first_event_ts": ts, "last_event_ts": ts, "speed": speed or 1.0}


def apply_event(state: Optional[dict], ev: dict) -> Tuple[Optional[dict], str]:
    """Apply one event. Returns (new_state_or_None, status)."""
    ts = float(ev["event_ts"])
    et = ev["event_type"]
    eid = ev.get("event_id")
    if et == "purchase":
        return None, CONVERTED
    if state is None:
        state = new_state(ev["customer_id"], ts, float(ev.get("speed") or 1.0))
    seen = state.setdefault("seen", [])
    if eid:
        if eid in seen:                       # duplicate delivery / replay -> ignore completely
            return state, (ACTIVE if state["items"] else EMPTY)
        seen.append(eid)
        del seen[:-SEEN_MAX]
    state["last_event_ts"] = max(state["last_event_ts"], ts)     # tolerate out-of-order events
    state["speed"] = float(ev.get("speed") or state.get("speed") or 1.0)
    pid = ev.get("product_id")
    qty = int(ev.get("quantity") or 1)
    price = float(ev.get("unit_price") or 0.0)
    if et == "add_to_cart" and pid:
        it = state["items"].setdefault(pid, {"qty": 0, "price": price})
        it["qty"] += qty
        it["price"] = price or it["price"]
    elif et == "remove_from_cart" and pid and pid in state["items"]:
        state["items"][pid]["qty"] -= qty
        if state["items"][pid]["qty"] <= 0:
            del state["items"][pid]
    if not state["items"] and et in ("remove_from_cart",):
        return None, EMPTY
    return state, (ACTIVE if state["items"] else EMPTY)


def apply_events(state: Optional[dict], events: list) -> Tuple[Optional[dict], str]:
    status = EMPTY
    for ev in sorted(events, key=lambda e: float(e["event_ts"])):
        state, status = apply_event(state, ev)
    return state, status


def cart_value(state: dict) -> float:
    return round(sum(i["qty"] * i["price"] for i in state["items"].values()), 2)


def effective_idle_s(state: dict, idle_threshold_s: float) -> float:
    """Replay at Nx compresses wall-clock gaps by N, so the threshold is compressed too."""
    return idle_threshold_s / max(float(state.get("speed") or 1.0), 1e-9)


def timeout_ms(state: dict, now_ms: int, idle_threshold_s: float) -> int:
    """Milliseconds from *now* until the abandonment deadline (anchored to last event)."""
    deadline_ms = (state["last_event_ts"] + effective_idle_s(state, idle_threshold_s)) * 1000
    return max(1, int(deadline_ms - now_ms))


def expire(state: Optional[dict], now_ts: float, idle_threshold_s: float) -> Optional[dict]:
    """Return an alert dict if the cart is non-empty and idle past the threshold, else None."""
    if not state or not state["items"]:
        return None
    idle = effective_idle_s(state, idle_threshold_s)
    if now_ts - state["last_event_ts"] < idle - 1e-6:
        return None
    return {
        "alert_id": alert_id_for(state["cart_id"], state["last_event_ts"]),
        "customer_id": state["customer_id"], "cart_id": state["cart_id"],
        "item_count": sum(i["qty"] for i in state["items"].values()),
        "cart_value": cart_value(state),
        "items_json": json.dumps(state["items"], sort_keys=True),
        "last_event_ts": state["last_event_ts"], "detected_ts": now_ts,
        "threshold_s": idle,
        # SLA clock starts when the threshold is crossed, not at the last event
        "alert_latency_s": round(now_ts - (state["last_event_ts"] + idle), 3),
    }


def apply_events_with_expiry(state, events, idle_threshold_s, now_ts=None):
    alerts, status = [], EMPTY
    for ev in sorted(events, key=lambda e: float(e["event_ts"])):
        if state is not None:
            a = expire(state, float(ev["event_ts"]), idle_threshold_s)
            if a:
                if now_ts is not None:
                    a["detected_ts"] = now_ts
                    a["alert_latency_s"] = round(now_ts - (a["last_event_ts"] + a["threshold_s"]), 3)
                alerts.append(a)
                state = None
        state, status = apply_event(state, ev)
    return state, status, alerts
