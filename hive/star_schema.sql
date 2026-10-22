-- Reference DDL for the Hive star schema (hive_load.py creates the same tables via Spark).
CREATE DATABASE IF NOT EXISTS c360_dw;
CREATE TABLE IF NOT EXISTS c360_dw.dim_customer (customer_key BIGINT, customer_id STRING, name STRING, city STRING, state STRING, tier STRING) STORED AS PARQUET;
CREATE TABLE IF NOT EXISTS c360_dw.dim_product  (product_key BIGINT, product_id STRING, category STRING, name STRING, list_price DECIMAL(10,2)) STORED AS PARQUET;
CREATE TABLE IF NOT EXISTS c360_dw.dim_date (date_key INT, full_date DATE, year INT, month INT, day INT, weekday STRING, week INT, quarter INT, is_weekend BOOLEAN) STORED AS PARQUET;
CREATE TABLE IF NOT EXISTS c360_dw.fact_orders (
  order_id STRING, item_no INT, customer_key BIGINT, product_key BIGINT, date_key INT,
  quantity INT, unit_price DECIMAL(10,2), line_amount DECIMAL(14,2), order_status STRING)
PARTITIONED BY (order_date DATE) STORED AS PARQUET;
