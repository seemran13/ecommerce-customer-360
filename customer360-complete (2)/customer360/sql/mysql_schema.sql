-- Source OLTP schema (MySQL 8). Loaded from data/mysql/*.csv via ingestion/load_mysql.sh
CREATE DATABASE IF NOT EXISTS shop;
USE shop;

CREATE TABLE customers (
  customer_id INT PRIMARY KEY, full_name VARCHAR(100), email VARCHAR(120), city VARCHAR(50), state VARCHAR(5),
  address VARCHAR(200), tier VARCHAR(10), signup_date DATE, last_modified DATETIME,
  KEY idx_cust_lm (last_modified));
CREATE TABLE products (
  product_id INT PRIMARY KEY, product_name VARCHAR(120), category VARCHAR(40), brand VARCHAR(40),
  price DECIMAL(10,2), last_modified DATETIME, KEY idx_prod_lm (last_modified));
CREATE TABLE orders (
  order_id BIGINT PRIMARY KEY, customer_id INT, order_ts DATETIME, total_amount DECIMAL(12,2),
  status VARCHAR(15), payment_type VARCHAR(15), last_modified DATETIME,
  KEY idx_ord_cust (customer_id), KEY idx_ord_lm (last_modified));
CREATE TABLE order_items (
  order_id BIGINT, product_id INT, quantity INT, unit_price DECIMAL(10,2), PRIMARY KEY (order_id, product_id));
CREATE TABLE reviews (
  review_id INT PRIMARY KEY, customer_id INT, product_id INT, rating TINYINT, review_text VARCHAR(500), review_date DATE);
