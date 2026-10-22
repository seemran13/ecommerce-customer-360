-- Ad-hoc queries on the Gold layer (database c360)
SELECT segment, COUNT(*) customers, ROUND(AVG(ltv),2) avg_ltv FROM c360.customer_360 GROUP BY segment ORDER BY avg_ltv DESC;
SELECT date_trunc('day', last_event_time) day, COUNT(*) abandoned, ROUND(SUM(cart_value),2) value_at_risk
FROM c360.abandoned_carts GROUP BY 1 ORDER BY 1 DESC LIMIT 30;
SELECT brand, window_start, mentions, avg_sentiment FROM c360.brand_sentiment ORDER BY window_start DESC LIMIT 60;
