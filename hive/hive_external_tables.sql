-- Apache Hive DDL Statements for External Tables over Medallion Data Lake
-- Execute in Hive Server / Beeline CLI to enable SQL reporting over HDFS files

CREATE DATABASE IF NOT EXISTS ecommerce_analytics;
USE ecommerce_analytics;

-- 1. Silver Layer: Enriched Orders External Table (Parquet format)
CREATE EXTERNAL TABLE IF NOT EXISTS silver_enriched_orders (
    order_id STRING,
    customer_id STRING,
    total_amount DOUBLE,
    item_count INT,
    currency STRING,
    loyalty_tier STRING,
    city STRING,
    state_code STRING
)
PARTITIONED BY (year STRING, month STRING, day STRING, hour STRING)
STORED AS PARQUET
LOCATION '/data-lake/silver/enriched-orders'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- 2. Gold Layer: Revenue KPI External Table
CREATE EXTERNAL TABLE IF NOT EXISTS gold_revenue_kpi (
    window_end TIMESTAMP,
    metric_type STRING,
    total_revenue_brl DOUBLE,
    total_orders INT,
    average_order_value DOUBLE
)
PARTITIONED BY (year STRING)
STORED AS PARQUET
LOCATION '/data-lake/gold/revenue_per_hour'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- 3. Gold Layer: Cart Abandonment Rate External Table
CREATE EXTERNAL TABLE IF NOT EXISTS gold_cart_abandonment (
    window_timestamp TIMESTAMP,
    total_carts_initiated INT,
    total_checkouts_completed INT,
    cart_abandonment_percentage DOUBLE
)
PARTITIONED BY (year STRING)
STORED AS PARQUET
LOCATION '/data-lake/gold/cart_abandonment'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- 4. Gold Layer: Fraud Security Alerts Table
CREATE EXTERNAL TABLE IF NOT EXISTS gold_fraud_alerts (
    alert_id STRING,
    event_timestamp TIMESTAMP,
    customer_id STRING,
    order_id STRING,
    risk_score DOUBLE,
    rule_violated STRING,
    details STRING
)
PARTITIONED BY (year STRING)
STORED AS PARQUET
LOCATION '/data-lake/gold/fraud_alerts_log'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- Repair partitions to discover new directory trees automatically
MSCK REPAIR TABLE silver_enriched_orders;
MSCK REPAIR TABLE gold_revenue_kpi;
MSCK REPAIR TABLE gold_cart_abandonment;
MSCK REPAIR TABLE gold_fraud_alerts;
