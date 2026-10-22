from c360.core import cart_state as cs


def ev(t, typ, pid=None, price=10.0, qty=1, eid=None, speed=1.0, cid="c1"):
    return {"event_ts": t, "event_type": typ, "product_id": pid, "unit_price": price, "quantity": qty,
            "event_id": eid or f"{cid}-{t}-{typ}-{pid}", "customer_id": cid, "speed": speed}


def test_cart_builds_and_values():
    st, status = cs.apply_events(None, [ev(0, "view", "a"), ev(1, "add_to_cart", "a"), ev(2, "add_to_cart", "b", 5.0)])
    assert status == cs.ACTIVE and cs.cart_value(st) == 15.0 and st["last_event_ts"] == 2


def test_purchase_clears_cart():
    st, status = cs.apply_events(None, [ev(1, "add_to_cart", "a"), ev(2, "purchase")])
    assert st is None and status == cs.CONVERTED


def test_remove_to_empty_is_not_alertable():
    st, status = cs.apply_events(None, [ev(1, "add_to_cart", "a"), ev(2, "remove_from_cart", "a")])
    assert st is None and status == cs.EMPTY


def test_alert_only_after_threshold():
    st, _ = cs.apply_events(None, [ev(100, "add_to_cart", "a")])
    assert cs.expire(st, 100 + 899, 900) is None
    a = cs.expire(st, 100 + 905, 900)
    assert a and a["alert_latency_s"] == 5.0 and a["cart_value"] == 10.0


def test_empty_cart_never_alerts():
    st, _ = cs.apply_events(None, [ev(1, "view", "a")])
    assert cs.expire(st, 10_000, 900) is None


def test_duplicates_ignored():
    e = ev(1, "add_to_cart", "a", eid="X")
    st, _ = cs.apply_events(None, [e, dict(e), dict(e)])
    assert st["items"]["a"]["qty"] == 1


def test_out_of_order_keeps_latest_ts():
    st, _ = cs.apply_events(None, [ev(50, "add_to_cart", "a"), ev(10, "view", "a")])
    assert st["last_event_ts"] == 50


def test_alert_id_deterministic():
    st, _ = cs.apply_events(None, [ev(5, "add_to_cart", "a")])
    assert cs.expire(st, 2000, 900)["alert_id"] == cs.expire(st, 3000, 900)["alert_id"]


def test_replay_speed_compresses_threshold():
    st, _ = cs.apply_events(None, [ev(0, "add_to_cart", "a", speed=100.0)])
    assert cs.effective_idle_s(st, 900) == 9.0
    assert cs.expire(st, 8, 900) is None and cs.expire(st, 9, 900)


def test_timeout_anchored_to_event_time():
    st, _ = cs.apply_events(None, [ev(1000, "add_to_cart", "a")])
    assert cs.timeout_ms(st, now_ms=1_000_000 + 500_000, idle_threshold_s=900) == 400_000
    assert cs.timeout_ms(st, now_ms=9_999_999_999, idle_threshold_s=900) == 1   # overdue -> fire immediately
