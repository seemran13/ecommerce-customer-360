"""Shared feature engineering (leak-free): session features are computed ONLY from events up to the last add_to_cart,
customer-history features only from orders strictly before the session start."""
import numpy as np, pandas as pd

TIER = {"bronze": 0, "silver": 1, "gold": 2, "platinum": 3}

def build_session_table(ev, orders, cust):
    ev = ev[["event_ts", "session_id", "customer_id", "event_type", "price", "qty"]].copy()
    ev["session_id"] = ev["session_id"].astype(str); ev["event_type"] = ev["event_type"].astype(str)
    ev = ev.sort_values("event_ts")
    start = ev.groupby("session_id")["event_ts"].min().rename("start")
    cust_of = ev.groupby("session_id")["customer_id"].max()
    last_cart = ev[ev.event_type == "add_to_cart"].groupby("session_id")["event_ts"].max().rename("last_cart_ts")
    s = pd.concat([start, cust_of, last_cart], axis=1).dropna(subset=["last_cart_ts"]).reset_index()
    ev = ev.merge(s[["session_id", "last_cart_ts"]], on="session_id")
    pre = ev[ev.event_ts <= ev.last_cart_ts]                       # <- leakage guard: nothing after the last cart add
    isv = (pre.event_type == "view"); isc = (pre.event_type == "add_to_cart")
    pre = pre.assign(v=isv.astype("int8"), c=isc.astype("int8"), cv=np.where(isc, pre.price * pre.qty.clip(lower=1), 0.0))
    f = pre.groupby("session_id").agg(n_views=("v", "sum"), n_cart=("c", "sum"), cart_value=("cv", "sum"))
    s = s.merge(f, on="session_id")
    s["secs_to_cart"] = (s.last_cart_ts - s.start).dt.total_seconds()
    s["hour"] = s.start.dt.hour + s.start.dt.minute / 60; s["dow"] = s.start.dt.dayofweek
    return s

def add_device_and_labels(s, ev_full):
    dv = ev_full.drop_duplicates("session_id")[["session_id", "device"]]
    dv = dv.assign(session_id=dv.session_id.astype(str), device=dv.device.astype(str))
    s = s.merge(dv, on="session_id", how="left")
    done = ev_full[ev_full.event_type.astype(str) == "purchase"].session_id.astype(str).unique()
    s["label"] = s.session_id.isin(set(done)).astype("int8")
    for d in ("mobile", "desktop", "tablet"): s[f"dev_{d}"] = (s.device == d).astype("int8")
    return s

def add_customer_history(s, orders, cust):
    """prior_orders / prior_ltv strictly before session start (searchsorted on per-customer sorted order times) -- no leakage."""
    o = orders.copy()   # all orders; the session's own order has ts >= start so it is excluded by allow_exact_matches=False + backward
    o["order_ts"] = pd.to_datetime(o.order_ts); o = o.sort_values(["customer_id", "order_ts"])
    o["cum_n"] = o.groupby("customer_id").cumcount() + 1; o["cum_amt"] = o.groupby("customer_id").total_amount.cumsum()
    s = s.assign(customer_id=s.customer_id.astype("int64")).sort_values("start").reset_index(drop=True)
    m = pd.merge_asof(s, o[["customer_id", "order_ts", "cum_n", "cum_amt"]].sort_values("order_ts"), left_on="start", right_on="order_ts",
                      by="customer_id", direction="backward", allow_exact_matches=False)
    s["prior_orders"] = m["cum_n"].fillna(0).values; s["prior_ltv"] = m["cum_amt"].fillna(0).values
    # prior sessions count
    s["prior_sessions"] = s.groupby("customer_id").cumcount()
    # customer's smoothed conversion rate over their PREVIOUS cart sessions only (labels of earlier sessions; current label excluded by shift)
    g = s.groupby("customer_id")["label"]
    s["prior_cart_sessions"] = g.cumcount()
    s["prior_buys"] = g.cumsum() - s["label"]
    s["prior_conv_rate"] = (s["prior_buys"] + 0.35 * 4) / (s["prior_cart_sessions"] + 4)
    s = s.merge(cust[["customer_id", "tier"]].assign(tier_n=lambda d: d.tier.map(TIER)), on="customer_id", how="left")
    return s

FEATURES = ["n_views", "n_cart", "cart_value", "secs_to_cart", "hour", "dow", "dev_mobile", "dev_desktop", "dev_tablet",
            "prior_orders", "prior_ltv", "prior_sessions", "prior_cart_sessions", "prior_conv_rate", "tier_n"]
