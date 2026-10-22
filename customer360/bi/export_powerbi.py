#!/usr/bin/env python3
"""Builds the three Power BI page extracts (CSV) from the local twin. In production Power BI connects to Hive/Databricks SQL warehouse
(views in hive/03_analytics_views.sql) via DirectQuery; these CSVs let the report be built and demoed offline."""
import os, sys, numpy as np, pandas as pd
from pathlib import Path
from build_dashboard import build_dashboard
d = sys.argv[1] if len(sys.argv) > 1 else "data"; o = f"{d}/bi"; os.makedirs(o, exist_ok=True)
ev = pd.read_pickle(f"{d}/clean/events_clean.pkl"); ev["event_type"] = ev.event_type.astype(str); ev["event_date"] = ev.event_ts.dt.date
orders = pd.read_csv(f"{d}/mysql/orders.csv", parse_dates=["order_ts"]); g = pd.read_csv(f"{d}/gold/customer_360_scored.csv")

# Page 1: Sales funnel
f = ev.groupby(["event_date", "event_type"]).session_id.nunique().unstack(fill_value=0).reset_index(); f.to_csv(f"{o}/page1_sales_funnel.csv", index=False)
# Page 2: cohort retention (monthly)
orders["m"] = orders.order_ts.dt.to_period("M"); first = orders.groupby("customer_id").m.min().rename("cohort")
c = orders.join(first, on="customer_id"); c["idx"] = (c.m - c.cohort).apply(lambda x: x.n)
coh = c.groupby(["cohort", "idx"]).customer_id.nunique().unstack(fill_value=0); (coh.div(coh[0], axis=0).round(4)).reset_index().astype({"cohort": str}).to_csv(f"{o}/page2_cohort_retention.csv", index=False)
# Page 3: abandoned carts by day / category / device
s = ev.groupby("session_id").agg(day=("event_date", "min"), cart=("event_type", lambda x: (x == "add_to_cart").any()), done=("event_type", lambda x: x.isin(["checkout", "purchase"]).any()), device=("device", "first"))
ab = s[s.cart & ~s.done].groupby(["day", "device"], observed=False).size().rename("abandoned_sessions").reset_index(); ab.to_csv(f"{o}/page3_abandoned_carts.csv", index=False)
g[["customer_id", "tier", "segment", "ltv", "orders", "propensity", "recency_days", "abandon_rate"]].to_csv(f"{o}/dim_customer_segments.csv", index=False)
print("exported", os.listdir(o))
print("CUSTOMER360_DASHBOARD_BUILT", build_dashboard(Path(d)))
