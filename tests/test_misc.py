import sqlite3
import pandas as pd
from c360.core import identity, sentiment
from producers.bluesky_producer import to_mention
from producers.replay_producer import replay
from notifier.notify_consumer import handle


def test_sentiment():
    assert sentiment.score("I love it, great and fast") > 0.5
    assert sentiment.score("terrible, arrived broken") < -0.5
    assert sentiment.score("not good") < 0 and sentiment.score("hello world") == 0


def test_identity_mapping_deterministic_and_in_range():
    ids = [f"c{i}" for i in range(50)]
    assert identity.map_visitor(123, ids) == identity.map_visitor(123, ids) and identity.map_visitor(5, ids) in ids


def test_bluesky_filter():
    msg = {"kind": "commit", "did": "d", "time_us": 1, "commit": {"operation": "create", "rkey": "r",
           "record": {"text": "Love the fast delivery from Olist!"}}}
    m = to_mention(msg, ("olist",))
    assert m and m["brand"] == "olist" and m["sentiment"] == "positive"
    msg["commit"]["record"]["text"] = "nothing relevant"
    assert to_mention(msg, ("olist",)) is None
    assert to_mention({"kind": "identity"}, ("olist",)) is None


class FakeProducer:
    def __init__(self): self.msgs = []
    def produce(self, topic, key, value, on_delivery=None): self.msgs.append((topic, key, value))
    def poll(self, *_): pass
    def flush(self): pass


def test_replay_speed_schedule_and_stamping():
    df = pd.DataFrame({"event_ts": pd.to_datetime(["2026-01-01 00:00:00", "2026-01-01 00:10:00"]),
                       "customer_id": ["c", "c"], "session_id": ["s", "s"], "event_type": ["view", "view"],
                       "product_id": ["p", "p"], "quantity": [1, 1], "unit_price": [1.0, 1.0]})
    waits, clock = [], [0.0]
    def sleep(w): waits.append(w); clock[0] += w
    p = FakeProducer()
    assert replay(df, 100.0, p, "t", sleep=sleep, now=lambda: clock[0]) == 2
    assert abs(waits[0] - 6.0) < 1e-6          # 10 min / 100x = 6 s
    import json
    assert json.loads(p.msgs[0][2])["speed"] == 100.0


def test_notifier_dedup_and_sla():
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE sent(alert_id TEXT PRIMARY KEY, notified_ts REAL, sla_s REAL)")
    a = {"alert_id": "x", "customer_id": "c", "cart_value": 5.0, "item_count": 1, "last_event_ts": 0.0, "threshold_s": 900.0}
    sent = []
    assert handle(a, db, now=lambda: 910.0, sender=sent.append) is True
    assert handle(a, db, now=lambda: 920.0, sender=sent.append) is False
    assert len(sent) == 1 and db.execute("select sla_s from sent").fetchone()[0] == 10.0
