#!/usr/bin/env python3
"""Batch-sync Spark Gold customer rows to HBase without collecting the dataset to the driver.

Usage (Hadoop cluster): spark-submit --master yarn spark/batch_hbase_sync.py /lake/gold/customer_360
The HBase Thrift endpoint must be reachable from Spark executors and HappyBase must
be installed on the Spark Python environment. HBase puts are idempotent upserts.
"""
import argparse
import math
import os
import sys

from common import spark


def _text(value, default=""):
    if value is None:
        return default
    if isinstance(value, float) and math.isnan(value):
        return default
    return str(value)


def _write_partition(iterator, host, port, batch_size):
    import happybase

    conn = happybase.Connection(host, port=port, timeout=10000, autoconnect=False)
    try:
        conn.open()
        table = conn.table("customer_360")
        count = 0
        with table.batch(batch_size=batch_size) as batch:
            for spark_row in iterator:
                row = spark_row.asDict(recursive=True)
                customer_id = int(row["customer_id"])
                if customer_id < 0 or customer_id > 9_999_999_999:
                    raise ValueError(f"customer_id cannot be encoded as a 10-digit HBase row key: {customer_id}")
                cells = {
                    b"profile:name": _text(row.get("full_name")).encode("utf-8"),
                    b"profile:tier": _text(row.get("tier"), "unknown").encode("utf-8"),
                    b"profile:city": _text(row.get("city")).encode("utf-8"),
                    b"profile:segment": _text(row.get("segment"), "unknown").encode("utf-8"),
                    b"metrics:ltv": _text(row.get("ltv"), "0").encode("utf-8"),
                    b"metrics:orders": str(int(float(_text(row.get("orders"), "0") or 0))).encode("ascii"),
                    b"metrics:last_seen": _text(row.get("last_seen")).encode("utf-8"),
                    b"metrics:propensity": _text(row.get("propensity"), "0").encode("utf-8"),
                }
                # Do not clear a live abandoned-cart status when the nightly Gold
                # aggregation has no active_cart column. Write it only if explicitly supplied.
                if "active_cart" in row and row.get("active_cart") is not None:
                    cells[b"cart:active_cart"] = _text(row.get("active_cart"), "NONE").encode("utf-8")
                batch.put(f"{customer_id:010d}".encode("ascii"), cells)
                count += 1
        return count
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("src", help="Spark Parquet Gold directory (for example /lake/gold/customer_360)")
    parser.add_argument("--host", default=os.getenv("HBASE_THRIFT_HOST", "hbase"))
    parser.add_argument("--port", type=int, default=int(os.getenv("HBASE_THRIFT_PORT", "9090")))
    parser.add_argument("--batch-size", type=int, default=500)
    args = parser.parse_args()
    if not (1 <= args.port <= 65535) or args.batch_size < 1:
        parser.error("--port must be 1..65535 and --batch-size must be positive")

    session = spark("customer360_gold_hbase_sync", {"spark.sql.adaptive.enabled": "false"})
    try:
        df = session.read.parquet(args.src)
        if "customer_id" not in df.columns:
            raise ValueError(f"Gold input {args.src} has no customer_id column; columns={df.columns}")

        # Preflight from driver before scheduling executors; avoids a partial run if schema is missing.
        import happybase
        connection = happybase.Connection(args.host, port=args.port, timeout=10000, autoconnect=False)
        try:
            connection.open()
            if b"customer_360" not in connection.tables():
                raise RuntimeError("HBase table customer_360 is missing; provision it with hbase/provision_schema.sh")
            families = set(connection.table("customer_360").families())
            required = {b"profile", b"metrics", b"cart"}
            missing = required - families
            if missing:
                names = ", ".join(sorted(f.decode("ascii") for f in missing))
                raise RuntimeError(f"HBase customer_360 is missing families [{names}]; run hbase/provision_schema.sh")
        finally:
            connection.close()

        expected = df.count()
        # Collect successful partition return values rather than using an accumulator:
        # task retry/speculation can increment an accumulator more than once.
        written = df.rdd.mapPartitions(
            lambda iterator: iter([_write_partition(iterator, args.host, args.port, args.batch_size)])
        ).sum()
        print(f"BATCH_HBASE_SYNC_PASS expected_rows={expected} written_rows={written} table=customer_360", flush=True)
        if written != expected:
            raise RuntimeError(f"HBase sync row-count mismatch: expected={expected}, written={written}")
    except Exception as exc:
        print(f"BATCH_HBASE_SYNC_FATAL {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        raise
    finally:
        session.stop()


if __name__ == "__main__":
    main()
