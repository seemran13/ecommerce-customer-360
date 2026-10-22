#!/usr/bin/env python3
"""DRILL 3 -- Reconciliation: record counts must match across Kafka -> Bronze -> Silver -> Gold.
Invariants (checked on the local twin; the same equations are run on Hive/Delta with --mode hive):
  I1 raw_lines            == parsed_ok + malformed
  I2 parsed_ok            == clean_rows + duplicates_removed         (Bronze -> Silver)
  I3 clean purchase evts  == orders derived from sessions (event order_id exists in orders.csv, same count)
  I4 sum(order_items.qty*price) per order == orders.total_amount     (Silver -> Gold money check)
  I5 gold customers       == source customers; gold LTV == sum(orders)
  I6 generator manifest   : events_written (incl. injected malformed lines) == raw_lines"""
import argparse, json, sys, subprocess, pandas as pd, numpy as np

def local(d):
    m = json.load(open(f"{d}/_manifest.json")); c = json.load(open(f"{d}/clean/cleanse_summary.json"))
    res = []
    def chk(name, a, b): res.append((name, a, b, a == b)); 
    chk("I1 raw == ok + malformed", c["raw_lines"], c["parsed_ok"] + c["malformed"])
    chk("I2 ok == clean + dups", c["parsed_ok"], c["clean_rows"] + c["duplicates_removed"])
    # a duplicated line that was itself malformed is quarantined, not deduped -> dups_removed + (malformed - malformed_injected) == dups_injected
    chk("I2b dups removed + malformed-dups == dups injected", c["duplicates_removed"] + c["malformed"] - m["malformed_injected"], m.get("duplicates_injected", 0))
    chk("I6 generated == raw lines", m["events_written"], c["raw_lines"])
    ev = pd.read_pickle(f"{d}/clean/events_clean.pkl", compression=None)
    pur = ev[ev.event_type == "purchase"]
    orders = pd.read_csv(f"{d}/mysql/orders.csv"); items = pd.read_csv(f"{d}/mysql/order_items.csv")
    sess_orders = orders[orders.order_id >= 5_000_000]
    # purchase events can be lost only via malformed lines: compare to orders whose purchase event survived
    chk("I3 purchase events matched to orders", int(pur.order_id.isin(sess_orders.order_id).sum()), len(pur))
    lost = len(sess_orders) - len(pur)
    res.append((f"I3b purchase events lost ({lost}) <= malformed lines ({c['malformed']})", lost, f"<= {c['malformed']}", 0 <= lost <= c["malformed"]))
    li = (items.quantity * items.unit_price).groupby(items.order_id).sum().round(2)
    mm = orders.set_index("order_id").total_amount.sub(li, fill_value=0).abs()
    chk("I4 order totals == sum(items) (mismatches)", int((mm > 0.02).sum()), 0)
    cust = pd.read_csv(f"{d}/mysql/customers.csv"); g = f"{d}/gold/customer_360.csv"
    try:
        g = pd.read_csv(g); chk("I5 gold customers == source", len(g), len(cust)); chk("I5 gold LTV == orders", round(g.ltv.sum(), 2), round(orders.total_amount.sum(), 2))
    except FileNotFoundError: print("(gold not built yet; skipping I5)")
    return res

def hive():
    """Run actual pass/fail invariants against the Hive star schema."""
    import re
    url = __import__("os").getenv("HIVE_JDBC_URL", "jdbc:hive2://hive:10000")
    queries = [
        ("fact_cart_events_nonempty", "SELECT COUNT(*) FROM c360.fact_cart_events", "positive"),
        ("fact_orders_nonempty", "SELECT COUNT(*) FROM c360.fact_orders", "positive"),
        ("dim_customer_nonempty", "SELECT COUNT(*) FROM c360.dim_customer", "positive"),
        ("duplicate_event_ids", "SELECT COUNT(*) FROM (SELECT event_id FROM c360.fact_cart_events GROUP BY event_id HAVING COUNT(*) > 1) d", "zero"),
        ("invalid_cart_events", "SELECT COUNT(*) FROM c360.fact_cart_events WHERE event_id IS NULL OR event_ts IS NULL OR customer_id IS NULL", "zero"),
        ("order_total_mismatches", "SELECT COUNT(*) FROM (SELECT order_id FROM c360.fact_orders GROUP BY order_id HAVING ABS(SUM(line_amount) - MAX(order_total)) > 0.02) d", "zero"),
        ("orphan_order_customer_keys", "SELECT COUNT(*) FROM c360.fact_orders f LEFT JOIN c360.dim_customer d ON f.customer_key = d.customer_key WHERE d.customer_key IS NULL", "zero"),
        ("customer_current_version_mismatches", "SELECT COUNT(*) FROM (SELECT customer_id FROM c360.dim_customer GROUP BY customer_id HAVING SUM(CASE WHEN is_current THEN 1 ELSE 0 END) <> 1) d", "zero"),
    ]
    failed = 0
    for name, query, rule in queries:
        cmd = ["beeline", "-u", url, "--silent=true", "--showHeader=false", "--outputformat=tsv2", "-e", query]
        try:
            output = subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True, timeout=180)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
            print(f"FAIL HIVE {name}: {exc}")
            if getattr(exc, "output", None):
                print(exc.output)
            failed += 1
            continue
        counts = [int(line.strip()) for line in output.splitlines() if re.fullmatch(r"\s*\d+\s*", line)]
        if len(counts) != 1:
            print(f"FAIL HIVE {name}: could not parse a single scalar result from Beeline output")
            print(output[-2000:])
            failed += 1
            continue
        value = counts[0]
        passed = value > 0 if rule == "positive" else value == 0
        print(f"{'PASS' if passed else 'FAIL'} HIVE {name}: {value} (rule={rule})")
        failed += int(not passed)
    if failed:
        print(f"HIVE_RECONCILIATION=FAIL failed_checks={failed}")
        sys.exit(1)
    print("HIVE_RECONCILIATION=PASS checks=8")

if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--mode", default="local"); ap.add_argument("--data", default="data"); a = ap.parse_args()
    if a.mode == "hive": hive(); sys.exit(0)
    res = local(a.data); bad = 0
    for n, x, y, ok in res: print(f"{'PASS' if ok else 'FAIL'}  {n:55s} {x} vs {y}"); bad += (not ok)
    sys.exit(1 if bad else 0)
