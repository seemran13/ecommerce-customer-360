#!/usr/bin/env python3
"""Random Forest purchase-propensity classifier (target: session with add_to_cart ends in purchase).
Time-based split (train on early days, test on the last 20 %), threshold tuned on a validation slice for F1.
SLA: AUC >= 0.75 and F1 >= 0.65.  Outputs model.joblib + metrics.json + feature importances."""
import json, sys, joblib, numpy as np, pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import roc_auc_score, f1_score, precision_recall_curve, precision_score, recall_score
sys.path.insert(0, "ml")
from features import *

def main(d="data", outdir="ml/artifacts"):
    import os; os.makedirs(outdir, exist_ok=True)
    ev = pd.read_pickle(f"{d}/clean/events_clean.pkl"); orders = pd.read_csv(f"{d}/mysql/orders.csv"); cust = pd.read_csv(f"{d}/mysql/customers.csv")
    s = build_session_table(ev, orders, cust); s = add_device_and_labels(s, ev); s = add_customer_history(s, orders, cust)
    s = s.sort_values("start").reset_index(drop=True); n = len(s)
    tr, va, te = s.iloc[: int(.65 * n)], s.iloc[int(.65 * n): int(.8 * n)], s.iloc[int(.8 * n):]
    rf = RandomForestClassifier(n_estimators=250, max_depth=14, min_samples_leaf=20, n_jobs=-1, class_weight="balanced_subsample", random_state=7)
    rf.fit(tr[FEATURES], tr.label)
    pv = rf.predict_proba(va[FEATURES])[:, 1]; pr, rc, th = precision_recall_curve(va.label, pv)
    f1s = 2 * pr * rc / np.clip(pr + rc, 1e-9, None); thr = float(th[np.argmax(f1s[:-1])])
    pt = rf.predict_proba(te[FEATURES])[:, 1]; yp = (pt >= thr).astype(int)
    m = dict(sessions_with_cart=n, train=len(tr), val=len(va), test=len(te), base_rate_test=float(te.label.mean()), threshold=round(thr, 3),
             auc=round(roc_auc_score(te.label, pt), 4), f1=round(f1_score(te.label, yp), 4), precision=round(precision_score(te.label, yp), 4),
             recall=round(recall_score(te.label, yp), 4), auc_sla_pass=None, f1_sla_pass=None)
    m["auc_sla_pass"] = m["auc"] >= .75; m["f1_sla_pass"] = m["f1"] >= .65
    m["feature_importance"] = dict(sorted(zip(FEATURES, map(lambda x: round(float(x), 4), rf.feature_importances_)), key=lambda kv: -kv[1]))
    joblib.dump(dict(model=rf, features=FEATURES, threshold=thr), f"{outdir}/propensity_rf.joblib")
    json.dump(m, open(f"{outdir}/propensity_metrics.json", "w"), indent=2); print(json.dumps(m, indent=2))
    s.to_pickle(f"{d}/clean/session_features.pkl")

if __name__ == "__main__": main(*sys.argv[1:])
