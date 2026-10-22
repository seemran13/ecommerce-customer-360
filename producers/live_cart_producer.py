"""Live clickstream simulator -> Kafka topic cart_events (24/7).
Generates realistic sessions: browse -> add to cart -> (checkout|abandon).  ~40% of carts are abandoned.
    python -m producers.live_cart_producer --customers data/source/mysql/customers.csv --rate 5
"""
import argparse
import random
import time
import uuid

import pandas as pd

from producers.cart_event import iso_now, make_event
from producers.common import make_producer, send
from c360.config import settings as S


def session_events(cid, products, abandon_p=0.4):
    sid = uuid.uuid4().hex[:16]
    picks = random.sample(products, random.randint(1, 4))
    for pid, price in picks:
        yield make_event(cid, sid, "view", pid, 1, price), random.uniform(1, 6)
    carted = [p for p in picks if random.random() < 0.6]
    for pid, price in carted:
        yield make_event(cid, sid, "add_to_cart", pid, 1, price), random.uniform(1, 4)
    if carted and random.random() > abandon_p:
        yield make_event(cid, sid, "checkout"), random.uniform(2, 8)
        yield make_event(cid, sid, "purchase"), 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--customers", default="data/source/mysql/customers.csv")
    ap.add_argument("--products", default="data/source/mysql/products.csv")
    ap.add_argument("--rate", type=float, default=2.0, help="new sessions per second")
    ap.add_argument("--duration", type=int, default=0, help="seconds; 0 = run forever")
    a = ap.parse_args()
    cust = pd.read_csv(a.customers)["customer_id"].tolist()
    prod = list(pd.read_csv(a.products)[["product_id", "price"]].itertuples(index=False, name=None))
    p = make_producer("live-cart")
    # interleave many concurrent sessions by scheduling each event on a simple time heap
    import heapq
    heap, t_end, n = [], (time.time() + a.duration if a.duration else None), 0
    nxt_session = time.time()
    while t_end is None or time.time() < t_end or heap:
        now = time.time()
        if now >= nxt_session and (t_end is None or now < t_end):
            gen = session_events(random.choice(cust), prod)
            ev, delay = next(gen, (None, 0))
            if ev:
                heapq.heappush(heap, (now, id(gen), gen, ev, delay))
            nxt_session = now + random.expovariate(a.rate)
        while heap and heap[0][0] <= time.time():
            _, _, gen, ev, delay = heapq.heappop(heap)
            ev["event_ts"] = iso_now()
            send(p, S.topic_cart, ev["customer_id"], ev)
            n += 1
            nxt = next(gen, None)
            if nxt:
                heapq.heappush(heap, (time.time() + delay, id(gen), gen, nxt[0], nxt[1]))
        p.poll(0)
        time.sleep(0.01)
    p.flush()
    print("sent", n)


if __name__ == "__main__":
    main()
