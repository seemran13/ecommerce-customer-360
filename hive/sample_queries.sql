-- Revenue by category and month (partition pruning on order_date)
SELECT d.year, d.month, p.category, SUM(f.line_amount) revenue
FROM c360_dw.fact_orders f JOIN c360_dw.dim_product p ON f.product_key=p.product_key
JOIN c360_dw.dim_date d ON f.date_key=d.date_key
WHERE f.order_date >= '2026-03-01' AND f.order_status <> 'cancelled'
GROUP BY d.year, d.month, p.category ORDER BY d.year, d.month, revenue DESC;
-- Top cities by revenue
SELECT c.city, COUNT(DISTINCT f.order_id) orders, SUM(f.line_amount) revenue
FROM c360_dw.fact_orders f JOIN c360_dw.dim_customer c ON f.customer_key=c.customer_key GROUP BY c.city ORDER BY revenue DESC;
