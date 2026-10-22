#!/usr/bin/env python3
"""
Large-scale synthetic dataset generator for the E-Commerce Customer 360 capstone.

Produces (under --out, default ./data):
  mysql/customers.csv, customers_delta.csv, products.csv, orders.csv, order_items.csv, reviews.csv
        -> load into MySQL (sql/mysql_schema.sql) for the Sqoop incremental import
  weblogs/dt=YYYY-MM-DD/events.json   (clickstream NDJSON, ~1% malformed lines on purpose)
        -> HDFS landing zone + Kafka `cart_events` replay
  tickets/tickets_NNNN.txt            (support tickets as text files)

Design notes
  * Schema-compatible with Olist / Retailrocket style data (view / addtocart / transaction).
  * Purchase probability is a logistic function of observable features (views, cart value,
    tier, history, device, hour) so the propensity model has real, learnable signal.
  * Every purchase event has a matching row in orders/order_items (needed by reconciliation).
  * Scale: --scale 1.0 ~= 200k customers, 20k products, ~700k sessions, ~3M events, ~500k orders.
    Use --scale 5 for ~15M events (needs ~6 GB disk).
"""
import argparse, os, json
import numpy as np, pandas as pd

TIERS = ["bronze", "silver", "gold", "platinum"]
TIER_P = [0.55, 0.28, 0.13, 0.04]
CATS = ["electronics", "fashion", "home", "beauty", "sports", "books", "toys", "grocery", "garden", "auto"]
BRANDS = ["Acme", "Zenith", "Nova", "Orbit", "Pixel", "Terra", "Lumo", "Vertex", "Kora", "Blip", "Atlas", "Mango"]
CITIES = [("Hyderabad", "TS"), ("Mumbai", "MH"), ("Delhi", "DL"), ("Bengaluru", "KA"), ("Chennai", "TN"), ("Pune", "MH"),
          ("Kolkata", "WB"), ("Ahmedabad", "GJ"), ("Jaipur", "RJ"), ("Lucknow", "UP"), ("Kochi", "KL"), ("Indore", "MP")]
DEVICES = ["mobile", "desktop", "tablet"]
DEVICE_P = [0.62, 0.32, 0.06]
START = pd.Timestamp("2026-07-01")
DAYS = 92  # clickstream window: Jul 1 - Sep 30 2026
SIGNAL_GAIN = float(os.getenv("SIGNAL_GAIN", "1.6"))
INTERCEPT = float(os.getenv("INTERCEPT", "-1.9"))


def sigmoid(x):
    return 1 / (1 + np.exp(-x))


def gen_customers(rng, n):
    ids = np.arange(1, n + 1)
    tier = rng.choice(TIERS, n, p=TIER_P)
    city_idx = rng.integers(0, len(CITIES), n)
    signup = pd.Timestamp("2022-01-01") + pd.to_timedelta(rng.integers(0, 1500, n), unit="D")
    df = pd.DataFrame({
        "customer_id": ids,
        "full_name": [f"Customer {i}" for i in ids],
        "email": [f"user{i}@mail.example" for i in ids],
        "city": [CITIES[i][0] for i in city_idx],
        "state": [CITIES[i][1] for i in city_idx],
        "address": rng.integers(1, 999, n).astype(str) + " Street " + rng.integers(1, 99, n).astype(str),
        "tier": tier,
        "signup_date": signup.strftime("%Y-%m-%d"),
        "last_modified": signup.strftime("%Y-%m-%d 00:00:00"),
    })
    latent = rng.normal(0, 0.7, n)  # hidden propensity, drives behaviour
    return df, latent


def gen_customer_delta(rng, cust, frac=0.08):
    """Address / tier changes after the initial load -> drives SCD Type 2 in Silver."""
    d = cust.sample(frac=frac, random_state=7).copy()
    up = {"bronze": "silver", "silver": "gold", "gold": "platinum", "platinum": "platinum"}
    flip = rng.random(len(d)) < 0.5
    d.loc[flip, "tier"] = d.loc[flip, "tier"].map(up)
    d["address"] = rng.integers(1, 999, len(d)).astype(str) + " New Avenue " + rng.integers(1, 99, len(d)).astype(str)
    d["last_modified"] = (START + pd.to_timedelta(rng.integers(5, 85, len(d)), unit="D")).strftime("%Y-%m-%d 12:00:00")
    return d


def gen_products(rng, n):
    ids = np.arange(1, n + 1)
    price = np.round(np.exp(rng.normal(4.3, 0.9, n)), 2).clip(1.5, 2500)
    return pd.DataFrame({
        "product_id": ids, "product_name": [f"Product {i}" for i in ids],
        "category": rng.choice(CATS, n), "brand": rng.choice(BRANDS, n),
        "price": price, "last_modified": "2026-06-30 00:00:00",
    })


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data")
    ap.add_argument("--scale", type=float, default=1.0)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--malformed-rate", type=float, default=0.01)
    a = ap.parse_args()
    rng = np.random.default_rng(a.seed)

    n_cust = int(200_000 * a.scale)
    n_prod = int(20_000 * a.scale)
    n_sess = int(700_000 * a.scale)
    n_hist_orders = int(400_000 * a.scale)
    for d in ("mysql", "weblogs", "tickets"):
        os.makedirs(os.path.join(a.out, d), exist_ok=True)

    cust, latent = gen_customers(rng, n_cust)
    prod = gen_products(rng, n_prod)
    tier_idx = cust["tier"].map({t: i for i, t in enumerate(TIERS)}).values
    prices = prod["price"].values
    w = np.exp(rng.normal(0, 1.0, n_cust) + 0.3 * latent)  # heavy-tailed activity
    w /= w.sum()

    cust.to_csv(f"{a.out}/mysql/customers.csv", index=False)
    gen_customer_delta(rng, cust).to_csv(f"{a.out}/mysql/customers_delta.csv", index=False)
    prod.to_csv(f"{a.out}/mysql/products.csv", index=False)

    # ---------------- sessions / clickstream ----------------
    CH = 100_000
    order_id_ctr = 1
    order_rows, item_rows = [], []
    n_events = n_malformed = n_purchase_sessions = n_cart_sessions = 0
    hist_cnt = np.zeros(n_cust + 1, dtype=np.int32)

    for s0 in range(0, n_sess, CH):
        m = min(CH, n_sess - s0)
        sid = np.arange(s0, s0 + m)
        ci = rng.choice(n_cust, m, p=w)
        cid = ci + 1
        day = rng.integers(0, DAYS, m)
        hour = np.clip(rng.normal(15, 5.5, m), 0, 23.99)
        start = START + pd.to_timedelta(day, unit="D") + pd.to_timedelta((hour * 3600).astype(int), unit="s")
        dev = rng.choice(DEVICES, m, p=DEVICE_P)
        n_views = np.minimum(1 + rng.geometric(0.33, m), 12)
        sess_id = np.array([f"s{x:08d}" for x in sid])

        p_cart = sigmoid(-1.6 + 0.22 * n_views + 0.25 * tier_idx[ci] + 0.5 * latent[ci] + 0.15 * (dev == "desktop"))
        has_cart = rng.random(m) < p_cart
        n_cart = np.where(has_cart, np.minimum(1 + rng.poisson(0.7, m), 5), 0)

        # views
        vs = np.repeat(np.arange(m), n_views)
        rank = np.arange(len(vs)) - np.repeat(np.cumsum(n_views) - n_views, n_views)
        v_ts = start[vs] + pd.to_timedelta((rank * rng.uniform(12, 60, len(vs))).astype(int), unit="s")
        v_pid = rng.integers(1, n_prod + 1, len(vs))
        ev_view = pd.DataFrame({"session_idx": vs, "ts": v_ts, "event_type": "view", "product_id": v_pid, "qty": 0})

        # add_to_cart
        cs = np.repeat(np.arange(m), n_cart)
        crank = np.arange(len(cs)) - np.repeat(np.cumsum(n_cart) - n_cart, n_cart)
        c_ts = start[cs] + pd.to_timedelta(((n_views[cs] + crank) * 35 + rng.integers(5, 25, len(cs))).astype(int), unit="s")
        c_pid = rng.integers(1, n_prod + 1, len(cs))
        c_qty = 1 + (rng.random(len(cs)) < 0.2).astype(int)
        ev_cart = pd.DataFrame({"session_idx": cs, "ts": c_ts, "event_type": "add_to_cart", "product_id": c_pid, "qty": c_qty})
        cart_val = np.bincount(cs, weights=prices[c_pid - 1] * c_qty, minlength=m)

        # purchase decision = logistic(observable features + hidden latent)
        # SIGNAL_GAIN scales how strongly *observable* behaviour (cart value, views, device, tier, history, time of day) drives purchase.
        # Real e-commerce intent signals are strong; with gain=1 the hidden customer latent dominates and no model can exceed AUC ~0.66.
        z = SIGNAL_GAIN * (-0.35 + 0.55 * tier_idx[ci] + 0.55 * latent[ci] - 0.9 * (np.log1p(cart_val / 50) - 1.5) + 0.30 * n_cart
                           + 0.16 * n_views + 0.30 * np.minimum(hist_cnt[cid], 6) + 0.8 * (dev == "desktop") - 0.5 * (dev == "mobile")
                           - 0.10 * np.abs(hour - 20)) + INTERCEPT
        buys = has_cart & (rng.random(m) < sigmoid(z))
        checkout_only = has_cart & ~buys & (rng.random(m) < 0.35)

        oid = np.zeros(m, dtype=np.int64)
        oid[buys] = np.arange(order_id_ctr, order_id_ctr + buys.sum()) + 5_000_000
        order_id_ctr += int(buys.sum())
        last_cart_ts = pd.Series(c_ts).groupby(cs).max()
        co_idx = np.where(buys | checkout_only)[0]
        co_ts = last_cart_ts.reindex(co_idx).values + pd.to_timedelta(rng.integers(20, 120, len(co_idx)), unit="s")
        ev_co = pd.DataFrame({"session_idx": co_idx, "ts": co_ts, "event_type": "checkout", "product_id": 0, "qty": 0})
        pi_ = np.where(buys)[0]
        p_ts = pd.Series(co_ts, index=co_idx).reindex(pi_).values + pd.to_timedelta(rng.integers(30, 180, len(pi_)), unit="s")
        ev_pur = pd.DataFrame({"session_idx": pi_, "ts": p_ts, "event_type": "purchase", "product_id": 0, "qty": 0})

        ev = pd.concat([ev_view, ev_cart, ev_co, ev_pur], ignore_index=True)
        si = ev["session_idx"].values
        ev["session_id"] = sess_id[si]
        ev["customer_id"] = cid[si]
        ev["device"] = dev[si]
        ev["price"] = np.where(ev["product_id"] > 0, prices[ev["product_id"].values.clip(1) - 1], 0.0)
        ev["order_id"] = np.where(ev["event_type"] == "purchase", oid[si], 0)
        ev = ev.sort_values("ts", kind="stable").reset_index(drop=True)
        ev["event_id"] = [f"e{s0}_{i}" for i in range(len(ev))]
        ev["ip"] = "10." + (ev["customer_id"] % 250).astype(str) + "." + (ev["customer_id"] % 97).astype(str) + ".1"
        n_events += len(ev)
        n_purchase_sessions += int(buys.sum())
        n_cart_sessions += int(has_cart.sum())

        # orders derived from purchase sessions
        cart_b = ev[(ev.event_type == "add_to_cart") & np.isin(ev.session_id, sess_id[pi_])]
        if len(cart_b):
            so = pd.Series(oid[pi_], index=sess_id[pi_])
            it = cart_b.assign(order_id=so.reindex(cart_b.session_id).values)
            it = it.groupby(["order_id", "product_id"], as_index=False).agg(quantity=("qty", "sum"), unit_price=("price", "first"))
            item_rows.append(it)
            tot = it.assign(t=it.quantity * it.unit_price).groupby("order_id", as_index=False)["t"].sum()
            od = pd.DataFrame({"order_id": oid[pi_], "customer_id": cid[pi_], "order_ts": p_ts})
            order_rows.append(od.merge(tot.rename(columns={"t": "total_amount"}), on="order_id"))
        np.add.at(hist_cnt, cid[pi_], 1)

        # NDJSON partitioned by day, with deliberate corruption
        ev["event_ts"] = ev["ts"].dt.strftime("%Y-%m-%dT%H:%M:%S")
        ev["dt"] = ev["ts"].dt.strftime("%Y-%m-%d")
        cols = ["event_id", "event_ts", "session_id", "customer_id", "event_type", "product_id", "price", "qty", "order_id", "device", "ip"]
        for dt, g in ev.groupby("dt"):
            lines = g[cols].to_json(orient="records", lines=True).splitlines()
            bad = rng.random(len(lines)) < a.malformed_rate
            for k in np.where(bad)[0]:
                r = rng.random()
                if r < 0.4:
                    lines[k] = lines[k][: int(rng.integers(5, 40))]                  # truncated
                elif r < 0.7:
                    lines[k] = "<<GARBAGE " + lines[k][-20:]                          # garbage prefix
                else:
                    lines[k] = lines[k].replace('"customer_id":', '"customer_id":"N/A",x:')  # invalid json
            n_malformed += int(bad.sum())
            p = f"{a.out}/weblogs/dt={dt}"
            os.makedirs(p, exist_ok=True)
            with open(f"{p}/events.json", "a") as f:
                f.write("\n".join(lines) + "\n")
        print(f"  sessions {s0 + m:>9,}/{n_sess:,}  events so far {n_events:,}", flush=True)

    # ---------------- orders: historical + session-derived ----------------
    h = n_hist_orders
    hc = rng.choice(n_cust, h, p=w) + 1
    h_ts = pd.Timestamp("2025-10-01") + pd.to_timedelta(rng.integers(0, 273 * 86400, h), unit="s")
    h_id = np.arange(1, h + 1)
    h_nitems = rng.integers(1, 5, h)
    hi = np.repeat(h_id, h_nitems)
    hp = rng.integers(1, n_prod + 1, len(hi))
    hq = rng.integers(1, 3, len(hi))
    hitems = pd.DataFrame({"order_id": hi, "product_id": hp, "quantity": hq, "unit_price": prices[hp - 1]})
    htot = hitems.assign(t=hitems.quantity * hitems.unit_price).groupby("order_id", as_index=False)["t"].sum()
    hord = pd.DataFrame({"order_id": h_id, "customer_id": hc, "order_ts": h_ts}).merge(
        htot.rename(columns={"t": "total_amount"}), on="order_id")
    orders = pd.concat([hord] + order_rows, ignore_index=True)
    items = pd.concat([hitems] + item_rows, ignore_index=True)
    orders["status"] = rng.choice(["delivered", "shipped", "returned", "cancelled"], len(orders), p=[0.82, 0.1, 0.05, 0.03])
    orders["payment_type"] = rng.choice(["upi", "card", "cod", "netbanking"], len(orders), p=[0.45, 0.3, 0.15, 0.1])
    orders["total_amount"] = orders["total_amount"].round(2)
    orders["last_modified"] = orders["order_ts"].dt.strftime("%Y-%m-%d %H:%M:%S")
    orders["order_ts"] = orders["order_ts"].dt.strftime("%Y-%m-%d %H:%M:%S")
    orders.sort_values("order_ts").to_csv(f"{a.out}/mysql/orders.csv", index=False)
    items.to_csv(f"{a.out}/mysql/order_items.csv", index=False)

    # ---------------- support tickets (text files) + reviews ----------------
    T = [("delivery", "negative", "My order {o} has not arrived yet and the tracking has been stuck for {d} days. Please help."),
         ("delivery", "negative", "The package for order {o} was delivered to the wrong address. I need a redelivery."),
         ("refund", "negative", "I returned the item from order {o} {d} days ago but the refund has not been credited."),
         ("refund", "neutral", "Could you confirm the refund timeline for order {o}? I paid via UPI."),
         ("product", "negative", "The product in order {o} stopped working after {d} days. It looks defective."),
         ("product", "positive", "Great quality on order {o}! Delivered early, would like to buy more from the same brand."),
         ("payment", "negative", "Money was debited twice for order {o}. Please reverse the duplicate payment."),
         ("account", "neutral", "I want to update my delivery address and phone number on my account."),
         ("cart", "negative", "My cart keeps emptying at checkout and the coupon code will not apply, so I abandoned the purchase."),
         ("cart", "neutral", "Is free shipping available if I complete my cart purchase today?")]
    nt = int(120_000 * a.scale)
    per = 1000
    tc = rng.choice(n_cust, nt) + 1
    tk = rng.integers(0, len(T), nt)
    tdate = pd.Timestamp("2026-07-01") + pd.to_timedelta(rng.integers(0, DAYS * 86400, nt), unit="s")
    ords = rng.choice(orders["order_id"].values, nt)
    dd = rng.integers(2, 15, nt)
    for fi, s in enumerate(range(0, nt, per)):
        with open(f"{a.out}/tickets/tickets_{fi:04d}.txt", "w") as f:
            for j in range(s, min(s + per, nt)):
                cat, sent, tpl = T[tk[j]]
                f.write(f"TICKET_ID: T{j + 1:07d}\nCUSTOMER_ID: {tc[j]}\nDATE: {tdate[j]:%Y-%m-%d %H:%M:%S}\n"
                        f"CATEGORY: {cat}\nSENTIMENT: {sent}\nBODY: {tpl.format(o=ords[j], d=dd[j])}\n---\n")
    nr = int(150_000 * a.scale)
    rv = pd.DataFrame({"review_id": np.arange(1, nr + 1), "customer_id": rng.choice(n_cust, nr) + 1,
                       "product_id": rng.integers(1, n_prod + 1, nr),
                       "rating": rng.choice([1, 2, 3, 4, 5], nr, p=[.08, .07, .15, .3, .4])})
    cm = {1: "Terrible, broke in a week", 2: "Poor quality for the price", 3: "Average, does the job",
          4: "Good value, happy with it", 5: "Excellent product, fast delivery"}
    rv["review_text"] = rv["rating"].map(cm)
    rv["review_date"] = "2026-08-15"
    rv.to_csv(f"{a.out}/mysql/reviews.csv", index=False)

    summary = dict(customers=n_cust, products=n_prod, sessions=n_sess, events_written=n_events,
                   malformed_injected=n_malformed, cart_sessions=n_cart_sessions,
                   purchase_sessions=n_purchase_sessions, orders=len(orders), order_items=len(items),
                   tickets=nt, reviews=nr)
    json.dump(summary, open(f"{a.out}/_manifest.json", "w"), indent=2)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
