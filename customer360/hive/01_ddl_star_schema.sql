-- Hive star schema. fact_orders partitioned by order_date (dynamic partitions).
CREATE DATABASE IF NOT EXISTS c360;
USE c360;
SET hive.exec.dynamic.partition=true; SET hive.exec.dynamic.partition.mode=nonstrict;

CREATE EXTERNAL TABLE IF NOT EXISTS stg_orders (order_id BIGINT, customer_id INT, order_ts STRING, total_amount DOUBLE, status STRING, payment_type STRING, last_modified STRING)
STORED AS AVRO LOCATION '/landing/mysql/orders';
CREATE EXTERNAL TABLE IF NOT EXISTS stg_order_items (order_id BIGINT, product_id INT, quantity INT, unit_price DOUBLE) STORED AS AVRO LOCATION '/landing/mysql/order_items';
CREATE EXTERNAL TABLE IF NOT EXISTS stg_customers (customer_id INT, full_name STRING, email STRING, city STRING, state STRING, address STRING, tier STRING, signup_date STRING, last_modified STRING) STORED AS AVRO LOCATION '/landing/mysql/customers';
CREATE EXTERNAL TABLE IF NOT EXISTS stg_products (product_id INT, product_name STRING, category STRING, brand STRING, price DOUBLE, last_modified STRING) STORED AS AVRO LOCATION '/landing/mysql/products';

-- Spark batch_cleanse writes partitioned Parquet to /lake/silver/events.
-- Repair discovers event_date=YYYY-MM-DD partitions for the Hive load below.
CREATE EXTERNAL TABLE IF NOT EXISTS stg_cart_events (
  event_id STRING, event_ts TIMESTAMP, session_id STRING, customer_id INT, event_type STRING,
  product_id INT, price DOUBLE, qty INT, order_id BIGINT, device STRING, ip STRING,
  product_name STRING, category STRING, brand STRING, list_price DOUBLE
)
PARTITIONED BY (event_date DATE)
STORED AS PARQUET LOCATION '/lake/silver/events';

CREATE TABLE IF NOT EXISTS dim_date (date_key INT, full_date DATE, year INT, quarter INT, month INT, day INT, day_of_week INT, is_weekend BOOLEAN) STORED AS ORC;
CREATE TABLE IF NOT EXISTS dim_customer (customer_key BIGINT, customer_id INT, full_name STRING, email STRING, city STRING, state STRING, address STRING, tier STRING, signup_date DATE,
                                         effective_from TIMESTAMP, effective_to TIMESTAMP, is_current BOOLEAN) STORED AS ORC;
CREATE TABLE IF NOT EXISTS dim_product (product_key BIGINT, product_id INT, product_name STRING, category STRING, brand STRING, price DOUBLE) STORED AS ORC;
CREATE TABLE IF NOT EXISTS fact_orders (order_id BIGINT, customer_key BIGINT, product_key BIGINT, date_key INT, quantity INT, unit_price DOUBLE, line_amount DOUBLE,
                                        order_total DOUBLE, status STRING, payment_type STRING)
PARTITIONED BY (order_date DATE) STORED AS ORC TBLPROPERTIES ('orc.compress'='SNAPPY');
CREATE TABLE IF NOT EXISTS fact_cart_events (event_id STRING, event_ts TIMESTAMP, session_id STRING, customer_id INT, event_type STRING, product_id INT, price DOUBLE, qty INT, order_id BIGINT, device STRING)
PARTITIONED BY (event_date DATE) STORED AS ORC;
