#!/usr/bin/env python3
"""Nightly: attach propensity score (mean of the customer's latest session scores, else prior) to customer_360 for HBase sync."""
import sys, joblib, pandas as pd
d = sys.argv[1] if len(sys.argv) > 1 else "data"; A = sys.argv[2] if len(sys.argv) > 2 else "ml/artifacts"; art = joblib.load(f"{A}/propensity_rf.joblib"); s = pd.read_pickle(f"{d}/clean/session_features.pkl")
s["propensity"] = art["model"].predict_proba(s[art["features"]])[:, 1]
last = s.sort_values("start").groupby("customer_id").tail(3).groupby("customer_id").propensity.mean()
g = pd.read_csv(f"{d}/gold/customer_360.csv"); g["propensity"] = g.customer_id.map(last).fillna(s.propensity.mean())
g.to_csv(f"{d}/gold/customer_360_scored.csv", index=False); print("scored", len(g))
