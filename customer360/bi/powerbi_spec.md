# Power BI report (3 pages)

**Connection:** Databricks SQL warehouse / Hive (DirectQuery for Abandoned Carts page, Import for the others). Offline: load `data/bi/*.csv`.

## Page 1 - Sales Funnel
Visuals: funnel (view -> add_to_cart -> checkout -> purchase sessions), daily conversion line, device split donut, KPI cards.
```DAX
View Sessions      = SUM(page1_sales_funnel[view])
Cart Sessions      = SUM(page1_sales_funnel[add_to_cart])
Purchase Sessions  = SUM(page1_sales_funnel[purchase])
Cart Rate          = DIVIDE([Cart Sessions],[View Sessions])
Checkout Rate      = DIVIDE(SUM(page1_sales_funnel[checkout]),[Cart Sessions])
Conversion Rate    = DIVIDE([Purchase Sessions],[View Sessions])
```
## Page 2 - Cohort Retention
Matrix: rows = cohort month, columns = months since first order, values = retention % (conditional-format heat map). Line: average retention by month index.
```DAX
Retention % = AVERAGE(page2_cohort_retention[Value])
M1 Retention = CALCULATE([Retention %], page2_cohort_retention[Attribute]="1")
```
## Page 3 - Abandoned Carts (+ live brand sentiment, refreshed every 1 min)
Visuals: abandoned sessions per day, by device, abandoned value, recovery rate, segment bar (from `dim_customer_segments`), live sentiment gauge from `brand_mentions` (Kafka -> Spark -> Delta -> automatic page refresh = 1 minute).
```DAX
Abandoned Sessions = SUM(page3_abandoned_carts[abandoned_sessions])
Recovery Rate      = DIVIDE(CALCULATE(COUNTROWS(recovery_log), recovery_log[recovered]=TRUE()), COUNTROWS(recovery_log))   -- target >= 18 %
Avg Brand Sentiment (1 min) = CALCULATE(AVERAGE(brand_mentions[sentiment]), brand_mentions[event_ts] >= NOW() - TIME(0,1,0))
LTV Uplift % = DIVIDE([LTV Treated] - [LTV Control], [LTV Control])                                                     -- target >= +12 %
```
Set *Page refresh* = 1 minute (Premium / Fabric capacity) for the live sentiment tile.
