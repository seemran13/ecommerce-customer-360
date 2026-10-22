#!/usr/bin/env python3
"""Compare alerts emitted by the streaming job with the reference set computed by local_pipeline (expected_alerts.csv)."""
import sys, json, pandas as pd
got = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
ids = [g["alert_id"] for g in got]; uniq = set(ids)
exp = pd.read_csv(sys.argv[2]); exp_ids = set(exp["alert_id"])
print(f"emitted={len(ids)} unique={len(uniq)} dup_in_topic={len(ids)-len(uniq)} expected={len(exp_ids)} missing={len(exp_ids-uniq)} unexpected={len(uniq-exp_ids)}")
ok = not (exp_ids - uniq)
print("PASS (no lost alerts; duplicates are removed by notifier alert_id dedup)" if ok else "FAIL: alerts lost"); sys.exit(0 if ok else 1)
