"""Export Gold-layer extracts as CSV for Power BI Desktop (Import mode / offline demo).
In production connect Power BI to Databricks SQL (DirectQuery) or Athena instead - see bi/DASHBOARD_SPEC.md."""
import os

import pandas as pd

OUT = "data/bi"


def export(customers360: pd.DataFrame, alerts: pd.DataFrame, orders: pd.DataFrame, events: pd.DataFrame):
    os.makedirs(OUT, exist_ok=True)
    customers360.to_csv(f"{OUT}/customer_360.csv", index=False)
    alerts.to_csv(f"{OUT}/abandoned_carts.csv", index=False)
    f = events.assign(day=events.event_ts.dt.date).groupby(["day", "event_type"]).size().unstack(fill_value=0).reset_index()
    f.to_csv(f"{OUT}/funnel_daily.csv", index=False)
    first = orders.groupby("customer_id").order_ts.min().dt.to_period("M").rename("cohort")
    o = orders.join(first, on="customer_id").assign(month=orders.order_ts.dt.to_period("M"))
    coh = o.groupby(["cohort", "month"]).customer_id.nunique().reset_index(name="active_customers")
    coh["month_index"] = (coh.month - coh.cohort).apply(lambda x: x.n)
    coh.assign(cohort=coh.cohort.astype(str), month=coh.month.astype(str)).to_csv(f"{OUT}/cohort_retention.csv", index=False)
    return sorted(os.listdir(OUT))
