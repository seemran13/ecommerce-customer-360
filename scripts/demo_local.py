"""
Infrastructure-free end-to-end demo (pure Python/pandas/sklearn):
  generate data -> replay through the abandoned-cart state machine -> baseline KPIs -> ML -> RAG -> BI extracts.
It validates the LOGIC. The Kafka/Spark/HBase/Delta/AWS pieces need the docker stack (README).
    python scripts/demo_local.py
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import numpy as np
import pandas as pd

from bi.extract_for_bi import export
from c360.core.simulate import detect_alerts
from c360.datagen.generate import generate
from kpi import measure_kpis as K
from ml.score import score
from ml.train import train
from rag.chatbot import Chatbot, build_index

SRC = "data/source"
if not os.path.exists(f"{SRC}/events.csv"):
    print("generated:", generate(SRC))
orders = pd.read_csv(f"{SRC}/mysql/orders.csv", parse_dates=["order_ts"])
events = pd.read_csv(f"{SRC}/events.csv", parse_dates=["event_ts"])

# 1) abandoned-cart detection (same state machine as the Spark job), 5 s trigger, 15 min threshold
# NB: do not use astype("int64")/1e9 - pandas>=3 may store datetimes at us resolution, which silently breaks the scale
ev = events.assign(event_ts=(events.event_ts - pd.Timestamp("1970-01-01")) / pd.Timedelta(seconds=1), speed=1.0).to_dict("records")
end = max(e["event_ts"] for e in ev) + 3600
alerts, inflight = detect_alerts(ev, 900, end_ts=end, trigger_s=5)
al = pd.DataFrame(alerts)
al["detected_ts"] = pd.to_datetime(al.detected_ts, unit="s")
al["last_event_time"] = pd.to_datetime(al.last_event_ts, unit="s")
lat = al.alert_latency_s
print(f"[detect] events={len(events)} abandoned_carts={len(al)} latency p50/p95/max = "
      f"{lat.quantile(.5):.1f}/{lat.quantile(.95):.1f}/{lat.max():.1f}s (simulated 5s trigger)")

# 2) baseline KPIs. No messages are sent in this synthetic data, so this is the NATURAL return rate, not campaign lift.
rec = K.recovery_rate(al, orders, window_days=7)
print(f"[kpi] natural 7-day return-and-buy rate after abandonment (baseline, no intervention) = {rec:.1%}")

# 3) ML
m = train()
print(f"[ml] AUC(test)={m['auc_test']:.3f}  F1(test)={m['f1_test']:.3f}  meets_targets={m['meets_targets']}  "
      f"k={m['kmeans_k']} silhouette={m['kmeans_silhouette']:.2f}  segments={m['segment_sizes']}")
sc = score()

# 4) RAG (offline TF-IDF embedder + extractive answer unless ANTHROPIC_API_KEY is set)
print("[rag] indexed", build_index(SRC, prefer="tfidf"))
r = Chatbot().ask("What do customers say about refunds?")
print("[rag] top sources:", [s["id"] for s in r["sources"]][:4])

# 5) BI extracts
c360 = sc.merge(orders.groupby("customer_id").total_amount.agg(ltv="sum", order_count="count").reset_index(), on="customer_id", how="left").fillna({"ltv": 0, "order_count": 0})
print("[bi] wrote", export(c360, al.drop(columns=["items_json"]), orders, events))

report = {"abandoned_carts": len(al), "alert_latency_p95_s_simulated": float(lat.quantile(.95)),
          "natural_recovery_7d": rec, "ml": {k: m[k] for k in ("auc_test", "f1_test", "meets_targets")},
          "verdicts": K.verdicts(recovery=None, sla=float(lat.quantile(.95)), auc=m["auc_test"], f1=m["f1_test"])}
json.dump(report, open("data/demo_report.json", "w"), indent=2)
print(json.dumps(report["verdicts"], indent=2))
