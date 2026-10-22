"""pytest -q tests  (requires data/ generated, small scale is fine: python data_gen/generate_data.py --out data --scale 0.02)"""
import json, os, sys, glob, pandas as pd, numpy as np, pytest
D = os.getenv("C360_DATA", "data")
pytestmark = pytest.mark.skipif(not os.path.exists(f"{D}/_manifest.json"), reason="generate data first")

def test_manifest_counts():
    m = json.load(open(f"{D}/_manifest.json")); o = pd.read_csv(f"{D}/mysql/orders.csv")
    assert len(o) == m["orders"] and o.order_id.is_unique

def test_every_order_has_items_and_totals_match():
    o = pd.read_csv(f"{D}/mysql/orders.csv"); i = pd.read_csv(f"{D}/mysql/order_items.csv")
    assert set(o.order_id) == set(i.order_id)
    t = (i.quantity * i.unit_price).groupby(i.order_id).sum().round(2)
    assert (o.set_index("order_id").total_amount.sub(t).abs() < 0.02).all()

def test_malformed_lines_exist_and_are_detected():
    bad = good = 0
    for f in glob.glob(f"{D}/weblogs/dt=*/events.json")[:5]:
        for l in open(f):
            try: json.loads(l); good += 1
            except Exception: bad += 1
    assert good > 0 and 0.003 < bad / (bad + good) < 0.03

def test_no_label_leakage_in_features():
    sys.path.insert(0, "ml"); from features import FEATURES
    forbidden = {"label", "purchase", "order_id", "checkout"}
    assert not forbidden & set(FEATURES)

def test_scd_delta_changes_tier_or_address():
    c = pd.read_csv(f"{D}/mysql/customers.csv").set_index("customer_id"); d = pd.read_csv(f"{D}/mysql/customers_delta.csv").set_index("customer_id")
    assert (c.loc[d.index, "address"] != d.address).all() and (d.last_modified > c.loc[d.index, "last_modified"]).all()
