"""Load generated CSVs into MySQL, and optionally apply a batch of changes to demonstrate incremental
(lastmodified) ingestion and SCD Type 2:   python -m ingestion.load_mysql --apply-updates 50"""
import argparse
import datetime as dt
import os
import random

import pandas as pd
import pymysql

from c360.config import settings as S

TABLES = ["customers", "products", "orders", "order_items"]


def conn():
    return pymysql.connect(host=S.mysql_host, port=S.mysql_port, user=S.mysql_user,
                           password=S.mysql_password, database=S.mysql_db, autocommit=False)


def load(src):
    c = conn()
    cur = c.cursor()
    for t in TABLES:
        df = pd.read_csv(os.path.join(src, "mysql", f"{t}.csv")).astype(object).where(lambda d: d.notna(), None)
        cols = ",".join(df.columns)
        ph = ",".join(["%s"] * len(df.columns))
        upd = ",".join(f"{c_}=VALUES({c_})" for c_ in df.columns)
        for i in range(0, len(df), 2000):
            cur.executemany(f"INSERT INTO {t} ({cols}) VALUES ({ph}) ON DUPLICATE KEY UPDATE {upd}",
                            df.iloc[i:i + 2000].values.tolist())
        c.commit()
        print("loaded", t, len(df))


def apply_updates(n):
    """Change city/tier for n customers (updated_at = now) -> next incremental import picks them up."""
    c = conn()
    cur = c.cursor()
    cur.execute("SELECT customer_id FROM customers")
    ids = [r[0] for r in cur.fetchall()]
    now = dt.datetime.utcnow().replace(microsecond=0)
    for cid in random.sample(ids, min(n, len(ids))):
        cur.execute("UPDATE customers SET city=%s, tier=%s, updated_at=%s WHERE customer_id=%s",
                    (random.choice(["Hyderabad", "Pune", "Chennai", "Delhi"]),
                     random.choice(["BASIC", "PLUS", "VIP"]), now, cid))
    c.commit()
    print("updated", n, "customers at", now)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", default="data/source")
    ap.add_argument("--apply-updates", type=int, default=0)
    a = ap.parse_args()
    apply_updates(a.apply_updates) if a.apply_updates else load(a.src)
