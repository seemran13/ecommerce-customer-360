#!/usr/bin/env python3
"""Pandas twin of spark/batch_cleanse.py (so the whole pipeline logic is verifiable on one machine).
Parse every raw line, count malformed (accumulator analogue), quarantine them, dedup on event_id, broadcast-join (map) product details."""
import glob, json, sys, os, time
import pandas as pd, numpy as np
from datetime import datetime

REQ = ("event_id", "event_ts", "session_id", "customer_id", "event_type")
TYPES = {"view", "add_to_cart", "checkout", "purchase", "remove_from_cart"}

def main(src="data/weblogs", out="data/clean", prod="data/mysql/products.csv"):
    os.makedirs(out, exist_ok=True)
    t0 = time.time(); good, n_raw, n_bad = [], 0, 0
    bad_f = open(f"{out}/quarantine.txt", "w")
    for f in sorted(glob.glob(f"{src}/dt=*/events.json")):
        for line in open(f):
            line = line.strip()
            if not line: continue
            n_raw += 1
            try:
                d = json.loads(line)
                if any(d.get(k) in (None, "") for k in REQ): raise ValueError
                datetime.fromisoformat(d["event_ts"]); int(d["customer_id"])
                if d["event_type"] not in TYPES: raise ValueError
                good.append((d["event_id"], d["event_ts"], d["session_id"], int(d["customer_id"]), d["event_type"], int(d.get("product_id") or 0),
                             float(d.get("price") or 0), int(d.get("qty") or 0), int(d.get("order_id") or 0), d.get("device")))
            except Exception:
                n_bad += 1; bad_f.write(line + "\n")
    bad_f.close()
    ev = pd.DataFrame(good, columns=["event_id", "event_ts", "session_id", "customer_id", "event_type", "product_id", "price", "qty", "order_id", "device"])
    n_ok = len(ev)
    ev = ev.drop_duplicates("event_id")
    ev["event_ts"] = pd.to_datetime(ev["event_ts"])
    p = pd.read_csv(prod)[["product_id", "category", "brand"]]
    ev = ev.merge(p, on="product_id", how="left")                       # broadcast-join analogue
    for c in ("customer_id", "product_id", "qty"): ev[c] = ev[c].astype("int32")
    for c in ("event_type", "device", "category", "brand"): ev[c] = ev[c].astype("category")
    ev.to_pickle(f"{out}/events_clean.pkl")
    s = dict(raw_lines=n_raw, parsed_ok=n_ok, malformed=n_bad, duplicates_removed=n_ok - len(ev), clean_rows=len(ev), seconds=round(time.time() - t0, 1))
    json.dump(s, open(f"{out}/cleanse_summary.json", "w"), indent=2); print("CLEANSE_SUMMARY", s)

if __name__ == "__main__": main(*sys.argv[1:])
