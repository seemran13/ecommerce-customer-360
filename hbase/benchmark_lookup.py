"""Measure HBase point-lookup latency (target: p99 < 100 ms).   python -m hbase.benchmark_lookup --n 2000"""
import argparse
import random
import statistics
import time

import happybase

from c360.config import settings as S


def percentile(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(round(p / 100 * (len(xs) - 1))))]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=2000)
    ap.add_argument("--target-ms", type=float, default=100.0)
    a = ap.parse_args()
    conn = happybase.Connection(S.hbase_host, S.hbase_port)
    t = conn.table(S.hbase_table)
    keys = [k for k, _ in t.scan(limit=5000, columns=[b"p:ltv"])]
    if not keys:
        raise SystemExit("table empty - run hbase_sync first")
    for k in keys[:50]:           # warm-up (connection + block cache)
        t.row(k)
    lat = []
    for _ in range(a.n):
        k = random.choice(keys)
        s = time.perf_counter()
        r = t.row(k)
        lat.append((time.perf_counter() - s) * 1000)
        assert r, k
    res = {"n": a.n, "p50_ms": percentile(lat, 50), "p95_ms": percentile(lat, 95), "p99_ms": percentile(lat, 99),
           "max_ms": max(lat), "mean_ms": statistics.mean(lat)}
    print({k: round(v, 2) for k, v in res.items()})
    print("PASS" if res["p99_ms"] < a.target_ms else "FAIL", f"(p99 < {a.target_ms} ms)")


if __name__ == "__main__":
    main()
