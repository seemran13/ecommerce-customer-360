```dax
Revenue            = SUMX(FILTER(fact_orders, fact_orders[order_status] <> "cancelled"), fact_orders[line_amount])
Orders             = DISTINCTCOUNT(fact_orders[order_id])
AOV                = DIVIDE([Revenue], [Orders])
Views              = CALCULATE(SUM(funnel_daily[view]))
Purchases          = CALCULATE(SUM(funnel_daily[purchase]))
Conversion %       = DIVIDE([Purchases], [Views])
Carts Abandoned    = DISTINCTCOUNT(abandoned_carts[alert_id])
Value At Risk      = SUM(abandoned_carts[cart_value])
P95 Alert Latency  = PERCENTILE.INC(abandoned_carts[alert_latency_s], 0.95)
Retention %        = DIVIDE(SUM(cohort_retention[active_customers]),
                      CALCULATE(SUM(cohort_retention[active_customers]), cohort_retention[month_index] = 0))
Avg Sentiment (1h) = CALCULATE(AVERAGE(brand_sentiment[avg_sentiment]),
                      brand_sentiment[window_start] >= NOW() - TIME(1,0,0))
```
