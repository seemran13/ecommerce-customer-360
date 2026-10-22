#!/usr/bin/env python3
"""Reference implementation + simulator of the streaming abandonment logic.

1) RULE CHECK: session has >=1 add_to_cart, no checkout/purchase, and no activity for IDLE (15 min) -> abandoned. Output = expected_alerts.csv
   (used by drills/check_alerts.py to validate the Spark job output).
2) 10-min / 1-min sliding window counts of carts (what the Spark window aggregate computes), spot-verified.
3) LATENCY MODEL: replays the *global* event-time stream through the same state machine as Spark (event-time timeout, watermark = max_event_ts - 10 s,
   micro-batch trigger 5 s). Alert is emitted when the watermark passes last_ts + 15 min. Delay beyond threshold is reported in event-time seconds.
   This is a model of the engine semantics -- the real SLA must be confirmed with notifier/alert_consumer.py on the running stack."""
import hashlib, json, sys, numpy as np, pandas as pd
IDLE = 900; WM = 10; TRIG = 5

def main(clean="data/clean/events_clean.pkl", outdir="data"):
    ev = pd.read_pickle(clean)[["event_ts", "session_id", "customer_id", "event_type", "price", "qty"]].sort_values("event_ts")
    ev["t"] = ((ev["event_ts"] - pd.Timestamp("1970-01-01")) // pd.Timedelta("1s")).astype("int64")   # epoch seconds, independent of datetime64 unit (ns/us)
    ev["cart"] = (ev.event_type == "add_to_cart"); ev["done"] = ev.event_type.isin(["checkout", "purchase"])
    ev["cv"] = np.where(ev.cart, ev.price * ev.qty.clip(lower=1), 0.0)
    g = ev.groupby("session_id", observed=True).agg(customer_id=("customer_id", "max"), last_ts=("t", "max"), n_cart=("cart", "sum"), done=("done", "max"), cart_value=("cv", "sum"))
    ab = g[(g.n_cart > 0) & (~g.done)].reset_index()
    ab["alert_id"] = [hashlib.md5(f"{s}{t}".encode()).hexdigest() for s, t in zip(ab.session_id, ab.last_ts)]
    ab.to_csv(f"{outdir}/_expected_alerts.csv", index=False)

    # latency model: sweep through the stream; watermark(t) = max event time seen so far - WM. Alert due at last_ts+IDLE.
    max_t = ev["t"].values  # sorted => running max == current value
    due = (ab.last_ts + IDLE).values
    # first event whose (t - WM) >= due ; its time is when engine can fire (rounded up to next trigger)
    idx = np.searchsorted(max_t, due + WM, side="left")
    ok = idx < len(max_t)
    fire = np.where(ok, max_t[np.minimum(idx, len(max_t) - 1)], np.nan)
    delay = np.ceil((fire - due) / TRIG) * TRIG
    d = pd.Series(delay[ok])
    hb_delay = float(np.ceil(WM / TRIG) * TRIG)      # with a 1 Hz heartbeat the watermark always advances: fire = due + WM, aligned to the next trigger
    # windows spot check
    adds = ev[ev.cart]; probe = adds["t"].iloc[len(adds) // 2]
    w_carts = adds[(adds.t > probe - 600) & (adds.t <= probe)].session_id.nunique()
    rep = dict(sessions=int(len(g)), cart_sessions=int((g.n_cart > 0).sum()), abandoned_sessions=int(len(ab)),
               abandonment_rate=round(len(ab) / max(1, (g.n_cart > 0).sum()), 4), abandoned_cart_value=round(float(ab.cart_value.sum()), 2),
               alerts_within_stream=int(ok.sum()), model_delay_p50_s=float(d.quantile(.5)), model_delay_p95_s=float(d.quantile(.95)),
               model_delay_p99_s=float(d.quantile(.99)), model_delay_max_s=float(d.max()),
               share_over_60s_without_heartbeat=round(float((d > 60).mean()), 4), model_delay_with_heartbeat_s=f"{hb_delay:.0f}-{hb_delay + TRIG:.0f}",
               sla_60s_p95_without_heartbeat=bool(d.quantile(.95) < 60), sla_60s_with_heartbeat=bool(hb_delay + TRIG < 60), window_probe_carts_10min=int(w_carts))
    json.dump(rep, open(f"{outdir}/abandonment_report.json", "w"), indent=2); print(json.dumps(rep, indent=2))

if __name__ == "__main__": main(*sys.argv[1:])
