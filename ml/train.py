"""
Train + evaluate:
  * K-Means customer segmentation on RFM-style features (k chosen by silhouette over 3..6)
  * Random Forest purchase-propensity classifier (label: purchase within the next H days)
Temporal split: train cutoffs -> validation cutoff (threshold for F1 is tuned HERE) -> test cutoff (reported once).
Targets from the spec: AUC >= 0.75 and F1 >= 0.65.  Results are written to data/ml/metrics.json - never hard-coded.
    python -m ml.train --horizon 14
"""
import argparse
import json
import os

import joblib
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import f1_score, precision_recall_curve, roc_auc_score, silhouette_score
from sklearn.preprocessing import StandardScaler

from c360.config import settings as S
from ml.features import FEATURES, load_inputs, stack

SEG_FEATURES = ["days_since_last_order", "orders_total", "spend_total", "sessions_28d"]
NAMES = ["Champions", "Loyal", "Promising", "At-risk", "Dormant", "New/Low"]


def name_segments(km, scaler, k):
    """Order clusters by (spend high, recency low) and give them business names."""
    c = scaler.inverse_transform(km.cluster_centers_)
    score = np.argsort(np.argsort(-(c[:, 2] / (c[:, 2].max() + 1e-9)) + c[:, 0] / (c[:, 0].max() + 1e-9)))
    names = {i: NAMES[min(score[i], len(NAMES) - 1)] for i in range(k)}
    return {int(i): n if list(names.values()).count(n) == 1 else f"{n}-{i}" for i, n in names.items()}


def train(horizon=14, src="data/source", out=None, seed=42):
    out = out or S.ml_root
    os.makedirs(out, exist_ok=True)
    orders, events = load_inputs(src)
    customers = events.customer_id.unique()
    t_max = events.event_ts.max().normalize()
    h = pd.Timedelta(days=horizon)
    test_T, val_T = t_max - h, t_max - 2 * h
    train_T = [val_T - h * i for i in (1, 2, 3)]
    tr, va, te = (stack(orders, events, train_T, horizon, customers), stack(orders, events, [val_T], horizon, customers),
                  stack(orders, events, [test_T], horizon, customers))

    rf = RandomForestClassifier(n_estimators=300, min_samples_leaf=5, class_weight="balanced_subsample",
                                n_jobs=-1, random_state=seed).fit(tr[FEATURES], tr.label)
    pv = rf.predict_proba(va[FEATURES])[:, 1]
    p, r, th = precision_recall_curve(va.label, pv)
    f1s = 2 * p * r / np.clip(p + r, 1e-9, None)
    thr = float(th[np.nanargmax(f1s[:-1])])
    pt = rf.predict_proba(te[FEATURES])[:, 1]
    metrics = {"horizon_days": horizon, "train_rows": len(tr), "test_rows": len(te),
               "test_positive_rate": float(te.label.mean()), "threshold_from_validation": thr,
               "auc_val": float(roc_auc_score(va.label, pv)), "auc_test": float(roc_auc_score(te.label, pt)),
               "f1_test": float(f1_score(te.label, pt >= thr))}
    metrics["meets_targets"] = bool(metrics["auc_test"] >= 0.75 and metrics["f1_test"] >= 0.65)
    metrics["importances"] = dict(sorted(zip(FEATURES, map(float, rf.feature_importances_)), key=lambda x: -x[1])[:6])

    # ---- K-Means ---------------------------------------------------------------------------------
    cur = stack(orders, events, [t_max], horizon, customers)
    sc = StandardScaler().fit(np.log1p(cur[SEG_FEATURES]))
    Z = sc.transform(np.log1p(cur[SEG_FEATURES]))
    best = max(((silhouette_score(Z, KMeans(k, n_init=10, random_state=seed).fit_predict(Z), sample_size=2000, random_state=seed), k)
                for k in range(3, 7)))
    km = KMeans(best[1], n_init=10, random_state=seed).fit(Z)
    class _S:  # scaler in log space -> need inverse to original scale for naming
        def inverse_transform(self, c): return np.expm1(sc.inverse_transform(c))
    seg_names = name_segments(km, _S(), best[1])
    metrics.update({"kmeans_k": best[1], "kmeans_silhouette": float(best[0]),
                    "segment_sizes": {seg_names[int(i)]: int(n) for i, n in zip(*np.unique(km.labels_, return_counts=True))}})
    joblib.dump({"rf": rf, "threshold": thr, "km": km, "scaler": sc, "seg_names": seg_names, "horizon": horizon}, f"{out}/models.joblib")
    json.dump(metrics, open(f"{out}/metrics.json", "w"), indent=2)
    return metrics


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--horizon", type=int, default=14)
    ap.add_argument("--src", default="data/source")
    print(json.dumps(train(**{k: v for k, v in vars(ap.parse_args()).items()}), indent=2))
