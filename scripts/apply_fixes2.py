def edit(path, fn):
    s = open(path).read(); new = fn(s)
    print(("already ok   " if new == s else "patched      "), path)
    if new != s: open(path, "w").write(new)

NEWFN = '''

def apply_events_with_expiry(state, events, idle_threshold_s, now_ts=None):
    alerts, status = [], EMPTY
    for ev in sorted(events, key=lambda e: float(e["event_ts"])):
        if state is not None:
            a = expire(state, float(ev["event_ts"]), idle_threshold_s)
            if a:
                if now_ts is not None:
                    a["detected_ts"] = now_ts
                    a["alert_latency_s"] = round(now_ts - (a["last_event_ts"] + a["threshold_s"]), 3)
                alerts.append(a)
                state = None
        state, status = apply_event(state, ev)
    return state, status, alerts
'''
def fix_state(s):
    return s if "def apply_events_with_expiry" in s else s.rstrip("\n") + "\n" + NEWFN

def fix_job(s):
    if "apply_events_with_expiry" in s:
        return s
    s = s.replace('''        st, status = cs.apply_events(st, events)
        now_ms = state.getCurrentProcessingTimeMs()''', '''        now_ms = state.getCurrentProcessingTimeMs()
        st, status, mid = cs.apply_events_with_expiry(st, events, idle_s, now_ts=now_ms / 1000.0)
        rows.extend({**a, "kind": "ABANDONED"} for a in mid)''')
    s = s.replace('''        yield pd.DataFrame([row], columns=COLS)
    return update''', '''        rows.append(row)
        yield pd.DataFrame(rows, columns=COLS)
    return update''')
    return s

edit("c360/core/cart_state.py", fix_state)
edit("spark/jobs/stream_cart_abandonment.py", fix_job)
