#!/usr/bin/env python3
"""Acceptance test: p99 HBase customer_360 point-lookup latency must be <100 ms."""
import os
import random
import sys
import time
import numpy as np
import happybase

N = int(sys.argv[1]) if len(sys.argv) > 1 else 1000
MAXID = int(sys.argv[2]) if len(sys.argv) > 2 else 200000
HOST = os.getenv("HBASE_HOST", "hbase")
PORT = int(os.getenv("HBASE_PORT", "9090"))
if N < 1 or MAXID < 1:
    raise SystemExit("N and MAXID must be positive integers")

conn = happybase.Connection(HOST, PORT, timeout=10000, autoconnect=False)
lat = []
try:
    conn.open()
    table = conn.table("customer_360")
    for _ in range(N):
        key = f"{random.randint(1, MAXID):010d}".encode("ascii")
        started = time.perf_counter()
        table.row(key)
        lat.append((time.perf_counter() - started) * 1000)
finally:
    conn.close()

p50, p95, p99 = np.percentile(lat, [50, 95, 99])
print(f"HBase lookup ms p50={p50:.2f} p95={p95:.2f} p99={p99:.2f} samples={N}")
if p99 < 100:
    print("PASS_HBASE_LOOKUP_P99_LT_100MS")
    sys.exit(0)
print("FAIL_HBASE_LOOKUP_P99_LT_100MS")
sys.exit(1)
