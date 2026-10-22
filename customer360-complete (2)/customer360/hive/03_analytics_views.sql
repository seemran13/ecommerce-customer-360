USE c360;
-- Power BI page 1: Sales funnel (from clean web events)
CREATE OR REPLACE VIEW v_sales_funnel AS
SELECT event_date, count(DISTINCT CASE WHEN event_type='view' THEN session_id END) view_sessions,
  count(DISTINCT CASE WHEN event_type='add_to_cart' THEN session_id END) cart_sessions,
  count(DISTINCT CASE WHEN event_type='checkout' THEN session_id END) checkout_sessions,
  count(DISTINCT CASE WHEN event_type='purchase' THEN session_id END) purchase_sessions
FROM fact_cart_events GROUP BY event_date;
-- Page 2: cohort retention (monthly)
CREATE OR REPLACE VIEW v_cohort_retention AS
WITH f AS (SELECT customer_key, trunc(min(order_date) OVER (PARTITION BY customer_key),'MM') cohort, trunc(order_date,'MM') m FROM fact_orders)
SELECT cohort, months_between(m, cohort) AS month_idx, count(DISTINCT customer_key) active_customers FROM f GROUP BY cohort, months_between(m, cohort);
-- Page 3: abandoned carts (sessions with add_to_cart and no checkout/purchase)
CREATE OR REPLACE VIEW v_abandoned_carts AS
SELECT event_date, session_id, max(customer_id) customer_id, sum(CASE WHEN event_type='add_to_cart' THEN price*qty END) cart_value
FROM fact_cart_events GROUP BY event_date, session_id
HAVING sum(CASE WHEN event_type='add_to_cart' THEN 1 ELSE 0 END)>0 AND sum(CASE WHEN event_type IN ('checkout','purchase') THEN 1 ELSE 0 END)=0;
