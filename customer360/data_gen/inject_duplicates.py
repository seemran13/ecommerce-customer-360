#!/usr/bin/env python3
"""Simulate Kafka at-least-once redelivery: append DUP_RATE of already-present valid lines again at the end of each day file,
and update the manifest so reconciliation can account for them (events_written += duplicates_injected)."""
import glob, json, random, sys
d = sys.argv[1] if len(sys.argv) > 1 else "data"; rate = float(sys.argv[2]) if len(sys.argv) > 2 else 0.005
random.seed(3); tot = 0
for f in glob.glob(f"{d}/weblogs/dt=*/events.json"):
    def valid(l):
        try: json.loads(l); return True
        except Exception: return False
    lines = [l for l in open(f).read().splitlines() if valid(l)]
    dup = random.sample(lines, int(len(lines) * rate)); tot += len(dup)
    with open(f, "a") as o: o.write("\n".join(dup) + "\n")
m = json.load(open(f"{d}/_manifest.json")); m["duplicates_injected"] = m.get("duplicates_injected", 0) + tot; m["events_written"] += tot
json.dump(m, open(f"{d}/_manifest.json", "w"), indent=2); print("duplicates injected:", tot)
