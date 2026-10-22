#!/usr/bin/env python3
"""Idempotently sync customer-level Gold CSV/Parquet rows to HBase.

Required table schema: customer_360 with profile, metrics and cart families.
Run `bash hbase/provision_schema.sh` once before the first sync. The script
fails early with an actionable error instead of failing part-way through a batch.
"""
import argparse
import happybase
import pandas as pd


def cell(value, default=""):
    if value is None:
        return default
    try:
        if pd.isna(value):
            return default
    except (TypeError, ValueError):
        pass
    return str(value)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("src", help="Gold customer_360 CSV or Parquet file/directory")
    parser.add_argument("--host", default="hbase")
    parser.add_argument("--port", type=int, default=9090)
    parser.add_argument("--batch-size", type=int, default=500)
    args = parser.parse_args()
    if args.port < 1 or args.port > 65535 or args.batch_size < 1:
        parser.error("--port must be 1..65535 and --batch-size must be positive")

    if args.src.lower().endswith((".parquet", ".pq")):
        frame = pd.read_parquet(args.src)
    else:
        frame = pd.read_csv(args.src)

    required_columns = {"customer_id"}
    missing_columns = required_columns - set(frame.columns)
    if missing_columns:
        raise SystemExit(f"Input is missing required columns: {sorted(missing_columns)}")

    conn = happybase.Connection(args.host, port=args.port, timeout=10000, autoconnect=False)
    try:
        conn.open()
        if b"customer_360" not in conn.tables():
            raise RuntimeError("HBase table customer_360 is missing; run bash hbase/provision_schema.sh")
        table = conn.table("customer_360")
        families = set(table.families())
        required_families = {b"profile", b"metrics", b"cart"}
        missing_families = required_families - families
        if missing_families:
            names = ", ".join(sorted(x.decode("ascii") for x in missing_families))
            raise RuntimeError(f"HBase customer_360 is missing families [{names}]; run bash hbase/provision_schema.sh")

        synced = 0
        with table.batch(batch_size=args.batch_size) as batch:
            for row in frame.to_dict(orient="records"):
                customer_id = int(row["customer_id"])
                if customer_id < 0:
                    raise ValueError(f"customer_id must be non-negative, got {customer_id}")
                row_key = f"{customer_id:010d}".encode("ascii")
                ltv = cell(row.get("ltv"), "0")
                orders = int(float(cell(row.get("orders"), "0") or 0))
                last_seen = cell(row.get("last_seen"), "")
                propensity = cell(row.get("propensity"), "0")
                cells = {
                    b"profile:name": cell(row.get("full_name"), "").encode("utf-8"),
                    b"profile:tier": cell(row.get("tier"), "unknown").encode("utf-8"),
                    b"profile:city": cell(row.get("city"), "").encode("utf-8"),
                    b"profile:segment": cell(row.get("segment"), "unknown").encode("utf-8"),
                    b"metrics:ltv": ltv.encode("utf-8"),
                    b"metrics:orders": str(orders).encode("ascii"),
                    b"metrics:last_seen": last_seen.encode("utf-8"),
                    b"metrics:propensity": propensity.encode("utf-8"),
                }
                # Preserve real-time streaming status if the batch source has no active_cart field.
                if "active_cart" in row and row.get("active_cart") is not None and not pd.isna(row.get("active_cart")):
                    cells[b"cart:active_cart"] = cell(row.get("active_cart"), "NONE").encode("utf-8")
                batch.put(row_key, cells)
                synced += 1
        print(f"HBASE_SYNC_PASS rows={synced} table=customer_360 families=profile,metrics,cart")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
