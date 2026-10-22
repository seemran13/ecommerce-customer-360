from c360.core.simulate import detect_alerts


def mk(cid, t, typ, pid=None):
    return {"customer_id": cid, "event_ts": t, "event_type": typ, "product_id": pid, "unit_price": 10.0, "quantity": 1,
            "event_id": f"{cid}{t}{typ}", "speed": 1.0}


def test_abandon_vs_convert_vs_inflight():
    evs = [mk("a", 0, "add_to_cart", "p"), mk("b", 0, "add_to_cart", "p"), mk("b", 5, "purchase"),
           mk("c", 1000, "add_to_cart", "p")]
    al, inflight = detect_alerts(evs, 900, end_ts=1100)
    assert [a["customer_id"] for a in al] == ["a"] and inflight == 1


def test_return_after_gap_creates_second_cart_alert():
    evs = [mk("a", 0, "add_to_cart", "p"), mk("a", 2000, "add_to_cart", "q")]
    al, _ = detect_alerts(evs, 900, end_ts=5000)
    assert len(al) == 2 and len({a["alert_id"] for a in al}) == 2


def test_trigger_quantisation_bounds_latency():
    al, _ = detect_alerts([mk("a", 3, "add_to_cart", "p")], 900, end_ts=5000, trigger_s=5)
    assert 0 <= al[0]["alert_latency_s"] < 5


def test_epoch_conversion_is_resolution_safe():
    import pandas as pd
    for unit in ("ns", "us", "ms", "s"):
        t = pd.Series(pd.to_datetime(["2026-03-01 00:00:00"])).dt.as_unit(unit)
        assert ((t - pd.Timestamp("1970-01-01")) / pd.Timedelta(seconds=1)).iloc[0] == 1772323200.0
