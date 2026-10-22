"""
Convert the real Retailrocket `events.csv` (timestamp[ms], visitorid, event, itemid, transactionid)
into the platform's event schema, using the deterministic identity map (open item #3).

    python -m c360.datagen.retailrocket_adapter --events events.csv \
        --customers data/source/mysql/customers.csv --products data/source/mysql/products.csv \
        --out data/source/events_retailrocket.csv --max-rows 500000
"""
import argparse
import hashlib

import pandas as pd

from c360.core.identity import map_item, map_visitor

EVENT_MAP = {"view": "view", "addtocart": "add_to_cart", "transaction": "purchase"}


def convert(events_csv, customers_csv, products_csv, max_rows=None) -> pd.DataFrame:
    cust = pd.read_csv(customers_csv)["customer_id"].tolist()
    prod = pd.read_csv(products_csv)
    pids, price = prod["product_id"].tolist(), dict(zip(prod["product_id"], prod["price"]))
    df = pd.read_csv(events_csv, nrows=max_rows).sort_values("timestamp")
    df["event_type"] = df["event"].map(EVENT_MAP)
    df["customer_id"] = [map_visitor(v, cust) for v in df["visitorid"]]
    df["product_id"] = [map_item(i, pids) for i in df["itemid"]]
    df["event_ts"] = pd.to_datetime(df["timestamp"], unit="ms")
    df["unit_price"] = df["product_id"].map(price)
    df.loc[df.event_type == "purchase", ["product_id", "unit_price"]] = [None, 0.0]
    df["quantity"] = 1
    # session = visitor + 30 min inactivity gap
    gap = df.groupby("visitorid")["event_ts"].diff() > pd.Timedelta("30min")
    df["session_id"] = df["visitorid"].astype(str) + "-" + gap.groupby(df["visitorid"]).cumsum().astype(str)
    df["event_id"] = [hashlib.md5(f"{a}{b}{c}{d}".encode()).hexdigest() for a, b, c, d in
                      zip(df["visitorid"], df["timestamp"], df["event"], df["itemid"])]
    return df[["event_id", "event_ts", "customer_id", "session_id", "event_type",
               "product_id", "quantity", "unit_price"]].reset_index(drop=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", required=True)
    ap.add_argument("--customers", required=True)
    ap.add_argument("--products", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--max-rows", type=int)
    a = ap.parse_args()
    convert(a.events, a.customers, a.products, a.max_rows).to_csv(a.out, index=False)
