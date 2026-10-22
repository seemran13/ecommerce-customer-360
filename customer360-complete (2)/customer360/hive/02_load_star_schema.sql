USE c360;
SET hive.exec.dynamic.partition=true; SET hive.exec.dynamic.partition.mode=nonstrict;

-- dim_date: 2025-01-01 .. 2027-12-31
INSERT OVERWRITE TABLE dim_date
SELECT CAST(date_format(d,'yyyyMMdd') AS INT), d, year(d), quarter(d), month(d), day(d), dayofweek(d), dayofweek(d) IN (1,7)
FROM (SELECT date_add('2025-01-01', pe.i) AS d FROM (SELECT posexplode(split(space(1094),' ')) AS (i,x)) pe) t;

-- dim_product (latest version of each product)
INSERT OVERWRITE TABLE dim_product
SELECT CAST(product_id AS BIGINT), product_id, product_name, category, brand, price
FROM (SELECT *, row_number() OVER (PARTITION BY product_id ORDER BY last_modified DESC) rn FROM stg_products) x WHERE rn=1;

-- dim_customer: SCD2 built from Sqoop's merged history is handled in Databricks Silver; here Type-1 current snapshot + validity columns
INSERT OVERWRITE TABLE dim_customer
-- Stable natural-key surrogate avoids shifting every customer's fact FK when new IDs arrive.
SELECT CAST(customer_id AS BIGINT), customer_id, full_name, email, city, state, address, tier, to_date(signup_date),
       CAST(last_modified AS TIMESTAMP), CAST('9999-12-31' AS TIMESTAMP), true
FROM (SELECT *, row_number() OVER (PARTITION BY customer_id ORDER BY last_modified DESC) rn FROM stg_customers) x WHERE rn=1;

-- Populate Hive's event fact from Spark's cleaned, deduplicated Parquet output.
MSCK REPAIR TABLE stg_cart_events;
INSERT OVERWRITE TABLE fact_cart_events PARTITION (event_date)
SELECT event_id, event_ts, session_id, customer_id, event_type, product_id, price, qty, order_id, device, event_date
FROM stg_cart_events
WHERE event_id IS NOT NULL AND event_ts IS NOT NULL AND customer_id IS NOT NULL;

-- fact_orders: Sqoop lastmodified imports append changed source rows, so pick the
-- latest copy of each order_id before joining its line items. This prevents an
-- updated order header from duplicating every fact line on subsequent nightly runs.
WITH latest_orders AS (
  SELECT order_id, customer_id, order_ts, total_amount, status, payment_type,
         row_number() OVER (
           PARTITION BY order_id
           ORDER BY CAST(last_modified AS TIMESTAMP) DESC, CAST(order_ts AS TIMESTAMP) DESC
         ) AS rn
  FROM stg_orders
)
INSERT OVERWRITE TABLE fact_orders PARTITION (order_date)
SELECT o.order_id, c.customer_key, p.product_key, CAST(date_format(o.order_ts,'yyyyMMdd') AS INT), i.quantity, i.unit_price,
       i.quantity*i.unit_price, o.total_amount, o.status, o.payment_type, to_date(o.order_ts)
FROM (SELECT order_id, customer_id, order_ts, total_amount, status, payment_type FROM latest_orders WHERE rn=1) o
JOIN stg_order_items i ON o.order_id=i.order_id
JOIN dim_customer c ON c.customer_id=o.customer_id JOIN dim_product p ON p.product_id=i.product_id;
