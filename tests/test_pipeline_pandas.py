import numpy as np
import pandas as pd
import pytest
from c360.datagen.generate import generate
from ml.features import build_features, build_labels
from kpi import measure_kpis as k


@pytest.fixture(scope="module")
def data(tmp_path_factory):
    out = str(tmp_path_factory.mktemp("src"))
    stats = generate(out, n_customers=150, n_products=30, days=40, seed=1)
    return out, stats


def test_datagen_outputs(data):
    out, s = data
    assert s["orders"] > 0 and s["events"] > 0
    orders = pd.read_csv(f"{out}/mysql/orders.csv")
    items = pd.read_csv(f"{out}/mysql/order_items.csv")
    assert set(items.order_id) == set(orders.order_id)            # referential integrity
    logs = "".join(open(f"{out}/weblogs/{f}").read() for f in __import__("os").listdir(f"{out}/weblogs"))
    assert "corrupted" in logs or "???" in logs or "\n\n" in logs   # malformed lines were injected


def test_features_have_no_future_leakage(data):
    out, _ = data
    o = pd.read_csv(f"{out}/mysql/orders.csv", parse_dates=["order_ts"])
    e = pd.read_csv(f"{out}/events.csv", parse_dates=["event_ts"])
    T = e.event_ts.min() + pd.Timedelta(days=25)
    cust = e.customer_id.unique()
    f1 = build_features(o, e, T, cust)
    f2 = build_features(o[o.order_ts < T], e[e.event_ts < T], T, cust)    # drop everything >= T
    pd.testing.assert_frame_equal(f1, f2)
    y = build_labels(o, T, 14, cust)
    assert y.sum() == o[(o.order_ts >= T) & (o.order_ts < T + pd.Timedelta(days=14))].customer_id.nunique()


def test_recovery_and_uplift_formulas():
    alerts = pd.DataFrame({"alert_id": ["a", "b", "c"], "customer_id": ["1", "2", "3"],
                           "detected_ts": pd.to_datetime(["2026-01-01"] * 3), "grp": ["treated", "treated", "holdout"]})
    orders = pd.DataFrame({"customer_id": ["1", "3"], "order_ts": pd.to_datetime(["2026-01-03", "2026-02-20"]),
                           "total_amount": [100.0, 50.0]})
    assert k.recovery_rate(alerts, orders) == pytest.approx(1 / 3)
    r = k.recovery_rate(alerts, orders, group_col="grp")
    assert r == {"holdout": 0.0, "treated": 0.5} and k.incremental_recovery(r) == 0.5
    up = k.ltv_uplift(orders, ["1"], ["2"], pd.Timestamp("2026-01-01"), 90)
    assert up == float("inf") or np.isnan(up) or up > 0
    assert k.verdicts(recovery=0.2)["recovery >= 18%"] == "PASS" and k.verdicts()["AUC >= 0.75"] == "not measured"
