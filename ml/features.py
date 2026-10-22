"""Point-in-time feature engineering. Every feature for observation date T uses ONLY data strictly before T,
and the label looks ONLY at (T, T+horizon]  -> no leakage; train/val/test are separated in time."""
import os

import numpy as np
import pandas as pd

from c360.config import settings as S

FEATURES = ["sessions_7d", "sessions_14d", "sessions_28d", "views_14d", "cart_adds_14d", "cart_adds_28d",
            "abandoned_28d", "orders_total", "orders_90d", "spend_total", "avg_order_value",
            "days_since_last_session", "days_since_last_order"]


def load_inputs(src="data/source"):
    """Prefers the Spark gold export; falls back to the generated/landed CSVs (demo)."""
    o, e = os.path.join(S.ml_root, "inputs_orders.parquet"), os.path.join(S.ml_root, "inputs_events.parquet")
    if os.path.exists(o) and os.path.exists(e):
        orders, events = pd.read_parquet(o), pd.read_parquet(e)
    else:
        orders = pd.read_csv(f"{src}/mysql/orders.csv", parse_dates=["order_ts"])
        events = pd.read_csv(f"{src}/events.csv", parse_dates=["event_ts"])
    orders = orders[orders.get("status", "delivered") != "cancelled"] if "status" in orders else orders
    return coerce_numeric(orders, ["total_amount"]), coerce_numeric(events, ["unit_price", "quantity"])


def coerce_numeric(df, cols):
    df = df.copy()
    for c in cols:
        if c in df.columns:
            if df[c].dtype == object:
                df[c] = df[c].map(lambda v: None if v is None else float(v))
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def build_features(orders: pd.DataFrame, events: pd.DataFrame, T: pd.Timestamp, customers=None) -> pd.DataFrame:
    ev = events[events.event_ts < T]
    od = orders[orders.order_ts < T]
    idx = pd.Index(customers if customers is not None else events.customer_id.unique(), name="customer_id")
    f = pd.DataFrame(index=idx)

    def cnt(df, col, days, kind=None, uniq=None):
        d = df[df.event_ts >= T - pd.Timedelta(days=days)]
        if kind:
            d = d[d.event_type == kind]
        g = d.groupby("customer_id")[uniq].nunique() if uniq else d.groupby("customer_id").size()
        return g.reindex(idx).fillna(0)

    f["sessions_7d"] = cnt(ev, "s", 7, uniq="session_id")
    f["sessions_14d"] = cnt(ev, "s", 14, uniq="session_id")
    f["sessions_28d"] = cnt(ev, "s", 28, uniq="session_id")
    f["views_14d"] = cnt(ev, "v", 14, "view")
    f["cart_adds_14d"] = cnt(ev, "a", 14, "add_to_cart")
    f["cart_adds_28d"] = cnt(ev, "a", 28, "add_to_cart")
    e28 = ev[ev.event_ts >= T - pd.Timedelta(days=28)]
    sess = e28.groupby(["customer_id", "session_id"]).event_type.agg(lambda s: ("add_to_cart" in set(s)) and ("purchase" not in set(s)))
    f["abandoned_28d"] = sess.groupby("customer_id").sum().reindex(idx).fillna(0)
    g = od.groupby("customer_id")
    f["orders_total"] = g.size().reindex(idx).fillna(0)
    f["orders_90d"] = od[od.order_ts >= T - pd.Timedelta(days=90)].groupby("customer_id").size().reindex(idx).fillna(0)
    f["spend_total"] = g.total_amount.sum().reindex(idx).fillna(0)
    f["avg_order_value"] = (f.spend_total / f.orders_total.replace(0, np.nan)).fillna(0)
    last_s = ev.groupby("customer_id").event_ts.max().reindex(idx)
    last_o = g.order_ts.max().reindex(idx)
    f["days_since_last_session"] = ((T - last_s).dt.total_seconds() / 86400).fillna(999)
    f["days_since_last_order"] = ((T - last_o).dt.total_seconds() / 86400).fillna(999)
    return f.reset_index()


def build_labels(orders: pd.DataFrame, T: pd.Timestamp, horizon_days: int, customers) -> pd.Series:
    w = orders[(orders.order_ts >= T) & (orders.order_ts < T + pd.Timedelta(days=horizon_days))]
    buyers = set(w.customer_id)
    return pd.Series([int(c in buyers) for c in customers], index=customers, name="label")


def stack(orders, events, cutoffs, horizon, customers):
    parts = []
    for T in cutoffs:
        X = build_features(orders, events, pd.Timestamp(T), customers)
        X["label"] = build_labels(orders, pd.Timestamp(T), horizon, X.customer_id).values
        X["cutoff"] = pd.Timestamp(T)
        parts.append(X)
    return pd.concat(parts, ignore_index=True)
