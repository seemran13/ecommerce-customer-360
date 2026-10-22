#!/usr/bin/env python3
"""K-Means customer segmentation on RFM + behaviour features. k chosen by silhouette over k=3..7 (on a sample).
Writes data/gold/customer_360.csv (one row per customer: profile, LTV, orders, last_seen, segment, active_cart) used by HBase sync + Power BI."""
import sys, os, json, numpy as np, pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score

def main(d="data"):
    cust = pd.read_csv(f"{d}/mysql/customers.csv"); orders = pd.read_csv(f"{d}/mysql/orders.csv", parse_dates=["order_ts"])
    ev = pd.read_pickle(f"{d}/clean/events_clean.pkl")[["customer_id", "session_id", "event_ts", "event_type", "price", "qty"]]
    ref = max(orders.order_ts.max(), ev.event_ts.max())
    o = orders.groupby("customer_id").agg(ltv=("total_amount", "sum"), orders=("order_id", "count"), last_order=("order_ts", "max"), aov=("total_amount", "mean"))
    ev["event_type"] = ev.event_type.astype(str)
    e = ev.groupby("customer_id").agg(last_seen=("event_ts", "max"), sessions=("session_id", "nunique"))
    ca = ev[ev.event_type == "add_to_cart"].groupby("customer_id").session_id.nunique().rename("cart_sessions")
    cv = ev[ev.event_type.isin(["checkout", "purchase"])].groupby("customer_id").session_id.nunique().rename("conv_sessions")
    g = cust.set_index("customer_id").join([o, e, ca, cv]).fillna({"ltv": 0, "orders": 0, "aov": 0, "sessions": 0, "cart_sessions": 0, "conv_sessions": 0})
    g["last_seen"] = g.last_seen.fillna(pd.Timestamp("2026-06-30")); g["last_order"] = g.last_order.fillna(pd.Timestamp("2025-01-01"))
    g["recency_days"] = (ref - g.last_order).dt.days; g["abandon_rate"] = (1 - g.conv_sessions / g.cart_sessions.replace(0, np.nan)).fillna(0).clip(0, 1)
    X = np.log1p(g[["ltv", "orders", "sessions", "cart_sessions"]]).join(g[["recency_days", "abandon_rate"]]); Xs = StandardScaler().fit_transform(X)
    samp = np.random.default_rng(1).choice(len(Xs), min(20000, len(Xs)), replace=False); best = (-1, 4); sil = {}
    for k in range(3, 8):
        km = KMeans(k, n_init=4, random_state=1).fit(Xs[samp]); sc = silhouette_score(Xs[samp], km.labels_); sil[k] = round(float(sc), 4)
        if sc > best[0]: best = (sc, k)
    km = KMeans(best[1], n_init=10, random_state=1).fit(Xs); g["cluster"] = km.labels_
    prof = g.groupby("cluster").agg(n=("ltv", "size"), ltv=("ltv", "mean"), orders=("orders", "mean"), recency=("recency_days", "mean"), abandon=("abandon_rate", "mean"))
    # rule-based, centroid-driven names (not just LTV rank)
    names, used, med_rec = {}, set(), g.recency_days.median()
    for c, r in prof.sort_values("ltv", ascending=False).iterrows():
        if r.orders < 0.5: nm = "Window-Shoppers"
        elif "Champions" not in used: nm = "Champions"
        elif r.recency > 1.3 * med_rec: nm = "At-Risk"
        elif "Loyal" not in used: nm = "Loyal"
        else: nm = "Promising"
        base, i = nm, 2
        while nm in used: nm = f"{base}-{i}"; i += 1
        used.add(nm); names[c] = nm
    g["segment"] = g.cluster.map(names)
    g = g.reset_index(); g["active_cart"] = "NONE"
    os.makedirs(f"{d}/gold", exist_ok=True); g.to_csv(f"{d}/gold/customer_360.csv", index=False)
    rep = dict(best_k=best[1], silhouette_by_k=sil, segments=prof.rename(index=names).round(2).to_dict("index")); json.dump(rep, open(f"{d}/gold/segmentation_report.json", "w"), indent=2, default=str)
    print(json.dumps(rep, indent=2, default=str))

if __name__ == "__main__": main(*sys.argv[1:])
