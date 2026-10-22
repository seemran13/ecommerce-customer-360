"""
KPI definitions (design-doc open item #6) and measurement.  All formulas are explicit and testable.

 Abandoned-cart recovery rate  = alerted carts followed by a purchase by the same customer within W days of the
                                 alert (default 7)  /  alerted carts.
   Incremental recovery        = rate(treated) - rate(holdout).  A 10% holdout that gets NO message is what turns
                                 'recovered' into 'recovered because of us'.  Without a holdout the figure is the
                                 *natural* return rate (baseline), not campaign effectiveness.
 LTV uplift                    = mean 90-day forward net revenue per customer, treated vs baseline cohort,
                                 (treated/baseline - 1).  Target +12%.
 Alert SLA                     = p95 of (notified_ts - (last_event_ts + threshold)) < 60 s.
 HBase lookup                  = p99 point-get latency < 100 ms  (hbase/benchmark_lookup.py).
"""
import numpy as np
import pandas as pd


def recovery_rate(alerts: pd.DataFrame, orders: pd.DataFrame, window_days=7, group_col=None):
    """alerts: customer_id, detected_ts (datetime) [, group]; orders: customer_id, order_ts, total_amount."""
    o = orders[["customer_id", "order_ts"]].sort_values("order_ts")
    m = alerts.merge(o, on="customer_id", how="left")
    m["hit"] = (m.order_ts > m.detected_ts) & (m.order_ts <= m.detected_ts + pd.Timedelta(days=window_days))
    per = m.groupby(["alert_id"] + ([group_col] if group_col else []), as_index=False)["hit"].max()
    if group_col:
        return per.groupby(group_col)["hit"].mean().to_dict()
    return float(per["hit"].mean()) if len(per) else float("nan")


def incremental_recovery(rates: dict, treated="treated", holdout="holdout"):
    return rates[treated] - rates[holdout]


def forward_revenue(orders, customer_ids, start, days=90):
    w = orders[(orders.order_ts >= start) & (orders.order_ts < start + pd.Timedelta(days=days))]
    rev = w.groupby("customer_id").total_amount.sum()
    return float(rev.reindex(list(customer_ids)).fillna(0).mean())


def ltv_uplift(orders, treated_ids, baseline_ids, start, days=90):
    t, b = forward_revenue(orders, treated_ids, start, days), forward_revenue(orders, baseline_ids, start, days)
    return (t / b - 1.0) if b else float("nan")


def sla_p95(latencies_s):
    return float(np.percentile(latencies_s, 95)) if len(latencies_s) else float("nan")


def verdicts(recovery=None, uplift=None, sla=None, hbase_p99_ms=None, auc=None, f1=None):
    def v(x, ok):
        return "not measured" if x is None or (isinstance(x, float) and np.isnan(x)) else ("PASS" if ok(x) else "FAIL")
    return {"recovery >= 18%": v(recovery, lambda x: x >= 0.18), "LTV uplift >= +12%": v(uplift, lambda x: x >= 0.12),
            "alert SLA p95 < 60s": v(sla, lambda x: x < 60), "HBase p99 < 100ms": v(hbase_p99_ms, lambda x: x < 100),
            "AUC >= 0.75": v(auc, lambda x: x >= 0.75), "F1 >= 0.65": v(f1, lambda x: x >= 0.65)}
