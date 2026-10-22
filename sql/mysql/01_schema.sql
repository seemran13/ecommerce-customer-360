-- Operational source schema (MySQL). `updated_at` drives Sqoop/JDBC incremental `lastmodified` imports.
CREATE TABLE IF NOT EXISTS customers (
  customer_id CHAR(32) PRIMARY KEY, name VARCHAR(100), email VARCHAR(150),
  city VARCHAR(60), state VARCHAR(10), zip VARCHAR(10), tier VARCHAR(10),
  created_at DATETIME NOT NULL, updated_at DATETIME NOT NULL,
  KEY idx_cust_upd (updated_at)
);
CREATE TABLE IF NOT EXISTS products (
  product_id CHAR(32) PRIMARY KEY, category VARCHAR(40), price DECIMAL(10,2), name VARCHAR(120),
  updated_at DATETIME NOT NULL, KEY idx_prod_upd (updated_at)
);
CREATE TABLE IF NOT EXISTS orders (
  order_id VARCHAR(20) PRIMARY KEY, customer_id CHAR(32) NOT NULL, status VARCHAR(20),
  order_ts DATETIME NOT NULL, total_amount DECIMAL(12,2), updated_at DATETIME NOT NULL,
  KEY idx_ord_upd (updated_at), KEY idx_ord_cust (customer_id)
);
CREATE TABLE IF NOT EXISTS order_items (
  order_id VARCHAR(20) NOT NULL, item_no INT NOT NULL, product_id CHAR(32) NOT NULL,
  quantity INT, unit_price DECIMAL(10,2), updated_at DATETIME NOT NULL,
  PRIMARY KEY (order_id, item_no), KEY idx_oi_upd (updated_at)
);
