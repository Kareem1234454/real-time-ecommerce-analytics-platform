-- PostgreSQL Database initialization for Operational Metadata
-- Connect to Postgres database on startup and create schemas

CREATE DATABASE ecommerce_meta;
\c ecommerce_meta;

CREATE TABLE IF NOT EXISTS customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_unique_id VARCHAR(50),
    city VARCHAR(100),
    state_code VARCHAR(10),
    loyalty_tier VARCHAR(30),
    country VARCHAR(50),
    signup_year INTEGER
);

CREATE TABLE IF NOT EXISTS products (
    product_id VARCHAR(50) PRIMARY KEY,
    category VARCHAR(100),
    brand VARCHAR(100),
    unit_price NUMERIC(10, 2),
    currency VARCHAR(10),
    product_weight_g NUMERIC(10, 2),
    product_length_cm NUMERIC(10, 2),
    product_height_cm NUMERIC(10, 2),
    product_width_cm NUMERIC(10, 2)
);

CREATE TABLE IF NOT EXISTS sellers (
    seller_id VARCHAR(50) PRIMARY KEY,
    city VARCHAR(100),
    state VARCHAR(10),
    seller_name VARCHAR(100),
    country VARCHAR(50),
    rating NUMERIC(3, 1)
);

CREATE TABLE IF NOT EXISTS warehouses (
    warehouse_id INTEGER PRIMARY KEY,
    warehouse_name VARCHAR(100),
    city VARCHAR(100),
    region VARCHAR(50),
    capacity_units INTEGER
);

CREATE TABLE IF NOT EXISTS inventory (
    inventory_id VARCHAR(100) PRIMARY KEY,
    product_id VARCHAR(50) REFERENCES products(product_id) ON DELETE CASCADE,
    warehouse_id INTEGER REFERENCES warehouses(warehouse_id) ON DELETE CASCADE,
    quantity INTEGER,
    reserved_quantity INTEGER,
    last_updated TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fraud_alarms (
    alert_id VARCHAR(100) PRIMARY KEY,
    event_timestamp TIMESTAMP,
    customer_id VARCHAR(50),
    order_id VARCHAR(50),
    risk_score NUMERIC(5, 2),
    rule_violated VARCHAR(200),
    details TEXT
);

CREATE INDEX idx_products_category ON products(category);
CREATE INDEX idx_customers_loyalty ON customers(loyalty_tier);
CREATE INDEX idx_inventory_product ON inventory(product_id);
