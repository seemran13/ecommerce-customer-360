# Power BI dashboard specification (3 pages + live sentiment tile)

A `.pbix` is a binary authored in Power BI Desktop, so it cannot be generated from code. This spec + `dax_measures.md`
+ the CSV extracts (`python -m bi.extract_for_bi` / demo script) let you build it in ~1 hour.

## Data sources
| Mode | Source | Tables |
|---|---|---|
| Production | Databricks SQL / Athena connector (DirectQuery) | gold.customer_360, gold.abandoned_carts, gold.brand_sentiment, gold.cart_activity_windows, hive/Athena fact_orders + dims |
| Demo | `data/bi/*.csv` (Import) | customer_360, abandoned_carts, funnel_daily, cohort_retention |

## Page 1 - Sales Funnel
Funnel visual: view -> add_to_cart -> checkout -> purchase (from `funnel_daily`). KPI cards: conversion %, revenue, AOV.
Line chart: daily revenue; slicers: date, category, city, tier.

## Page 2 - Cohort Retention
Matrix: rows = cohort month, columns = `month_index`, value = `Retention %` (measure). Conditional-format as heatmap.

## Page 3 - Abandoned Carts
Cards: carts abandoned (period), value at risk, **recovery rate**, p95 alert latency. Table: top 20 open carts by value.
Bar: abandonment by hour and by tier. Line: `cart_activity_windows` events per 10-min window.

## Live brand-sentiment tile (updates every 1 minute)
Source `gold.brand_sentiment` (written every 30 s). Line (avg_sentiment) + column (mentions) per minute, last 60 min.
Set **Automatic page refresh = 1 minute** on that page with **DirectQuery**.
> Licence caveat (verify for your tenant): automatic page refresh intervals below 30 minutes require Premium/PPU/Fabric
> capacity (or an admin-set minimum on shared capacity). On a plain Pro licence use a streaming/push dataset for the
> sentiment tile instead, or accept 30-minute refresh and document it. This is the one acceptance criterion that
> depends on licensing rather than on the pipeline.
