"""
Synthetic Olist-style source data for the whole platform.

Outputs under --out:
  mysql/{customers,products,orders,order_items}.csv   -> loaded into MySQL (ingestion/load_mysql.py)
  weblogs/weblog_YYYYMMDD.log                         -> raw web logs (Spark Core parses these, ~1% malformed)
  events.csv                                          -> clean clickstream in the Kafka event schema (replay + ML)
  tickets/tickets_YYYYMMDD.txt                        -> support tickets as text files
  reviews.csv                                         -> product reviews

The data is SYNTHETIC. Any metric computed from it (AUC, recovery rate...) demonstrates that the
pipeline works; it is not a claim about a real marketplace. Swap in real Olist/Retailrocket data
via c360/datagen/retailrocket_adapter.py.
"""
from __future__ import annotations

import argparse
import hashlib
import os
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

CITIES = [("Hyderabad", "TS"), ("Bengaluru", "KA"), ("Mumbai", "MH"), ("Chennai", "TN"),
          ("Delhi", "DL"), ("Pune", "MH"), ("Kolkata", "WB"), ("Ahmedabad", "GJ")]
CATEGORIES = ["electronics", "fashion", "home", "beauty", "sports", "books", "toys", "grocery"]
TIERS = ["BASIC", "PLUS", "VIP"]

TICKET_SUBJECTS = {
    "delivery": ["My order is late and tracking shows no movement.",
                 "Delivery delayed again, this is terrible service.",
                 "Package never arrived but status says delivered."],
    "refund": ["I want a refund for my cancelled order.",
               "Refund still missing after ten days, please help.",
               "Return was picked up but no refund issued."],
    "quality": ["The product arrived damaged and broken.",
                "Item quality is poor and not as described.",
                "Received the wrong item, missing accessories."],
    "payment": ["Payment failed but money was deducted.",
                "Coupon code not applied at checkout, wrong total charged."],
    "praise": ["Great service, the delivery was fast and the product is perfect.",
               "Thanks for the quick support, very happy with my purchase."],
}
REVIEW_TEXT = {
    1: ["Terrible quality, arrived broken and support was useless.", "Worst purchase ever, want a refund."],
    2: ["Poor packaging and slow delivery, disappointed.", "Not as described, product is bad."],
    3: ["Okay product, delivery was a bit late.", "Average quality for the price."],
    4: ["Good product, fast delivery, would recommend.", "Nice quality and easy to use."],
    5: ["Excellent! Love it, amazing quality and fast shipping.", "Perfect, best purchase this year."],
}


def hid(rng: np.random.Generator) -> str:
    return hashlib.md5(rng.bytes(16)).hexdigest()


def generate(out: str, n_customers=2000, n_products=300, days=120, seed=7, start="2026-03-01"):
    rng = np.random.default_rng(seed)
    t0 = datetime.fromisoformat(start)
    for d in ("mysql", "weblogs", "tickets"):
        os.makedirs(os.path.join(out, d), exist_ok=True)

    # ---- master data -------------------------------------------------------------------------
    products = pd.DataFrame({
        "product_id": [hid(rng) for _ in range(n_products)],
        "category": rng.choice(CATEGORIES, n_products),
        "price": np.round(rng.lognormal(4.3, 0.8, n_products) + 5, 2),
    })
    products["name"] = products["category"] + "-item-" + products.index.astype(str)
    products["updated_at"] = t0

    cust_ids = [hid(rng) for _ in range(n_customers)]
    loc = rng.integers(0, len(CITIES), n_customers)
    created = [t0 - timedelta(days=int(x)) for x in rng.integers(30, 700, n_customers)]
    customers = pd.DataFrame({
        "customer_id": cust_ids,
        "name": [f"Customer {i:05d}" for i in range(n_customers)],
        "email": [f"user{i:05d}@example.com" for i in range(n_customers)],
        "city": [CITIES[i][0] for i in loc], "state": [CITIES[i][1] for i in loc],
        "zip": [f"{int(z):06d}" for z in rng.integers(100000, 999999, n_customers)],
        "tier": rng.choice(TIERS, n_customers, p=[0.7, 0.22, 0.08]),
        "created_at": created,
    })
    customers["updated_at"] = customers["created_at"]

    # ---- behaviour ------------------------------------------------------------------------------
    # activity: sessions/day per customer (heterogeneous) ; intent: P(purchase | session)
    activity = rng.gamma(shape=0.6, scale=0.12, size=n_customers)          # sessions per day
    intent = np.clip(rng.beta(2, 6, n_customers) * 1.4, 0.02, 0.9)          # conversion tendency
    price = products["price"].to_numpy()
    pid = products["product_id"].to_numpy()

    ev_rows, order_rows, item_rows = [], [], []
    oid_n = 0
    for ci in range(n_customers):
        cid = cust_ids[ci]
        n_sess = rng.poisson(activity[ci] * days)
        for _ in range(n_sess):
            st = t0 + timedelta(seconds=float(rng.uniform(0, days * 86400)))
            sid = hashlib.md5(f"{cid}{st}".encode()).hexdigest()[:16]
            ts = st
            k = rng.integers(1, 6)
            picks = rng.integers(0, n_products, k)
            for p in picks:                                            # browse
                ev_rows.append((ts, cid, sid, "view", pid[p], 1, price[p]))
                ts += timedelta(seconds=float(rng.uniform(5, 90)))
            carted = [p for p in picks if rng.random() < 0.45]
            for p in carted:
                ev_rows.append((ts, cid, sid, "add_to_cart", pid[p], 1, price[p]))
                ts += timedelta(seconds=float(rng.uniform(3, 40)))
            if carted and rng.random() < 0.15:                          # remove one item
                p = carted[0]
                ev_rows.append((ts, cid, sid, "remove_from_cart", pid[p], 1, price[p]))
                carted = carted[1:]
                ts += timedelta(seconds=5)
            if carted and rng.random() < intent[ci]:                    # checkout + purchase
                ev_rows.append((ts, cid, sid, "checkout", None, 1, 0.0))
                ts += timedelta(seconds=float(rng.uniform(20, 120)))
                ev_rows.append((ts, cid, sid, "purchase", None, 1, 0.0))
                oid_n += 1
                oid = f"O{oid_n:08d}"
                tot = 0.0
                for j, p in enumerate(carted, 1):
                    q = int(rng.integers(1, 3))
                    tot += q * price[p]
                    item_rows.append((oid, j, pid[p], q, price[p], ts))
                status = "delivered" if rng.random() > 0.04 else "cancelled"
                order_rows.append((oid, cid, status, ts, round(tot, 2), ts))
            # else: abandoned cart (items remain, no purchase) -- the detection target

    events = pd.DataFrame(ev_rows, columns=["event_ts", "customer_id", "session_id", "event_type",
                                            "product_id", "quantity", "unit_price"])
    events = events.sort_values("event_ts").reset_index(drop=True)
    events["event_id"] = [hashlib.md5(f"{r.customer_id}{r.event_ts}{r.event_type}{r.product_id}{i}".encode())
                          .hexdigest() for i, r in enumerate(events.itertuples())]
    orders = pd.DataFrame(order_rows, columns=["order_id", "customer_id", "status", "order_ts",
                                               "total_amount", "updated_at"])
    items = pd.DataFrame(item_rows, columns=["order_id", "item_no", "product_id", "quantity",
                                             "unit_price", "updated_at"])

    # ---- write MySQL extracts ----------------------------------------------------------------------
    for name, df in (("customers", customers), ("products", products), ("orders", orders),
                     ("order_items", items)):
        df.to_csv(os.path.join(out, "mysql", f"{name}.csv"), index=False)
    events.to_csv(os.path.join(out, "events.csv"), index=False)

    # ---- raw web logs (with ~1% malformed lines for the accumulator) ------------------------------
    for day, g in events.groupby(events["event_ts"].dt.strftime("%Y%m%d")):
        lines = []
        for r in g.itertuples():
            if rng.random() < 0.01:
                lines.append(rng.choice(["-- corrupted --", "", f"{r.event_ts} ???", "GET / HTTP/1.1 \x00"]))
                continue
            lines.append(f'{r.event_ts.strftime("%Y-%m-%dT%H:%M:%SZ")} 10.0.{rng.integers(0,255)}.{rng.integers(1,255)} '
                         f'"GET /{r.event_type}?cid={r.customer_id}&pid={r.product_id or "-"}&qty={r.quantity}'
                         f'&price={r.unit_price:.2f} HTTP/1.1" 200 sid={r.session_id}')
        with open(os.path.join(out, "weblogs", f"weblog_{day}.log"), "w") as f:
            f.write("\n".join(lines) + "\n")

    # ---- tickets (text files) and reviews ----------------------------------------------------------
    tk_n = 0
    by_day = {}
    for ci in rng.choice(n_customers, int(n_customers * 0.12), replace=False):
        for _ in range(rng.integers(1, 3)):
            cat = rng.choice(list(TICKET_SUBJECTS), p=[.3, .2, .2, .15, .15])
            tk_n += 1
            when = t0 + timedelta(seconds=float(rng.uniform(0, days * 86400)))
            body = rng.choice(TICKET_SUBJECTS[cat])
            by_day.setdefault(when.strftime("%Y%m%d"), []).append(
                f"TICKET_ID: T{tk_n:06d}\nCUSTOMER_ID: {cust_ids[ci]}\nCREATED: {when.isoformat()}\n"
                f"CATEGORY: {cat}\nBODY: {body}\n---\n")
    for day, blocks in by_day.items():
        with open(os.path.join(out, "tickets", f"tickets_{day}.txt"), "w") as f:
            f.write("".join(blocks))

    rv = []
    sample = orders.sample(frac=0.5, random_state=seed)
    for r in sample.itertuples():
        pr = items[items.order_id == r.order_id].iloc[0].product_id
        score = int(rng.choice([1, 2, 3, 4, 5], p=[.06, .06, .12, .3, .46]))
        rv.append((f"R{len(rv)+1:06d}", r.order_id, r.customer_id, pr, score,
                   str(rng.choice(REVIEW_TEXT[score])), r.order_ts + timedelta(days=int(rng.integers(2, 9)))))
    pd.DataFrame(rv, columns=["review_id", "order_id", "customer_id", "product_id", "score",
                              "comment", "created_at"]).to_csv(os.path.join(out, "reviews.csv"), index=False)
    return {"customers": len(customers), "products": len(products), "orders": len(orders),
            "events": len(events), "tickets": tk_n, "reviews": len(rv)}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/source")
    ap.add_argument("--customers", type=int, default=2000)
    ap.add_argument("--products", type=int, default=300)
    ap.add_argument("--days", type=int, default=120)
    ap.add_argument("--seed", type=int, default=7)
    a = ap.parse_args()
    print(generate(a.out, a.customers, a.products, a.days, a.seed))
