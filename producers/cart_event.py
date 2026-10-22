"""Canonical cart-event schema + helpers (used by live, replay and drill producers)."""
import hashlib
import uuid
from datetime import datetime, timezone

SCHEMA_FIELDS = ["event_id", "event_ts", "orig_ts", "customer_id", "session_id", "event_type",
                 "product_id", "quantity", "unit_price", "speed", "source", "seq"]


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def make_event(customer_id, session_id, event_type, product_id=None, quantity=1, unit_price=0.0,
               speed=1.0, source="live", orig_ts=None, event_id=None, seq=None) -> dict:
    now = iso_now()
    return {"event_id": event_id or uuid.uuid4().hex, "event_ts": now, "orig_ts": orig_ts or now,
            "customer_id": customer_id, "session_id": session_id, "event_type": event_type,
            "product_id": product_id, "quantity": int(quantity), "unit_price": float(unit_price or 0.0),
            "speed": float(speed), "source": source, "seq": seq}


def stable_event_id(*parts) -> str:
    """Replays of the same source row always get the same id -> dedup works across replays."""
    return hashlib.md5("|".join(map(str, parts)).encode()).hexdigest()
