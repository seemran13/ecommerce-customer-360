"""Broker-kill verifier: consume cart_events from the beginning, check that seq 0..N-1 of the drill run all arrived
(no loss) and report duplicates.  Exit 1 on loss.    python -m drills.verify_no_loss --run r1"""
import argparse
import json
import sys
import uuid
from collections import Counter

from c360.config import settings as S


def check(seqs, n):
    c = Counter(seqs)
    missing = sorted(set(range(n)) - set(c))
    dups = sum(v - 1 for v in c.values() if v > 1)
    return missing, dups


def main():
    from confluent_kafka import Consumer, KafkaError
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", required=True)
    a = ap.parse_args()
    exp = json.load(open(f"drills/.expected_{a.run}.json"))
    c = Consumer({"bootstrap.servers": S.kafka_bootstrap, "group.id": f"verify-{uuid.uuid4().hex[:6]}",
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c.subscribe([S.topic_cart])
    seqs, idle = [], 0
    while idle < 8:
        m = c.poll(1.0)
        if m is None:
            idle += 1
            continue
        if m.error():
            if m.error().code() != KafkaError._PARTITION_EOF:
                print("consumer error", m.error())
            continue
        idle = 0
        ev = json.loads(m.value())
        if ev.get("source") == f"drill-{a.run}":
            seqs.append(ev["seq"])
    missing, dups = check(seqs, exp["events"])
    print(f"expected={exp['events']} received_unique={len(set(seqs))} missing={len(missing)} duplicates={dups}")
    print("BROKER-KILL DRILL", "PASSED (zero loss)" if not missing else f"FAILED, first missing seq: {missing[:10]}")
    sys.exit(1 if missing else 0)


if __name__ == "__main__":
    main()
