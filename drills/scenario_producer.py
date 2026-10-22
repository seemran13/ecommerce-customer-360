"""Deterministic drill workload.  N carts: every 3rd cart is purchased, the rest are abandoned.
Each event carries a unique `seq` so the verifier can prove nothing was lost.  Writes drills/.expected_<run>.json.
    python -m drills.scenario_producer --run r1 --carts 600 --rate 100
"""
import argparse
import json
import time

from producers.cart_event import make_event
from producers.common import make_producer, send
from c360.config import settings as S


def build(run, carts):
    seq, evs = 0, []
    for i in range(carts):
        cid, sid = f"drill-{run}-{i:05d}", f"s-{run}-{i}"
        steps = [("view", f"P{i}", 10.0), ("add_to_cart", f"P{i}", 10.0), ("add_to_cart", f"Q{i}", 5.0)]
        if i % 3 == 0:
            steps += [("checkout", None, 0.0), ("purchase", None, 0.0)]
        for et, pid, price in steps:
            evs.append((cid, sid, et, pid, price, seq))
            seq += 1
    expected_alerts = sum(1 for i in range(carts) if i % 3 != 0)
    return evs, {"run": run, "events": len(evs), "carts": carts, "expected_alerts": expected_alerts}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", required=True)
    ap.add_argument("--carts", type=int, default=600)
    ap.add_argument("--rate", type=float, default=100.0, help="events/sec")
    a = ap.parse_args()
    evs, exp = build(a.run, a.carts)
    json.dump(exp, open(f"drills/.expected_{a.run}.json", "w"))
    p = make_producer("drill")
    for cid, sid, et, pid, price, seq in evs:
        ev = make_event(cid, sid, et, pid, 1, price, source=f"drill-{a.run}", seq=seq)
        send(p, S.topic_cart, cid, ev)
        time.sleep(1.0 / a.rate)
    p.flush()
    print("produced", exp)


if __name__ == "__main__":
    main()
