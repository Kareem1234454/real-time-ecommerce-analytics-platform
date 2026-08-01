# `docs/design/03_Data_Model.md`

# Data Model

> This document defines the logical data model of the Real-Time E-Commerce Analytics Platform. It describes the core business entities, their attributes, relationships, and how they support both streaming and analytical workloads.

---

# Table of Contents

1. Introduction
2. Data Modeling Approach
3. Core Business Entities
4. Entity Relationship Diagram
5. Entity Definitions
6. Master Data
7. Transactional Data
8. Analytical Data
9. Data Relationships
10. Data Enrichment
11. Naming Conventions
12. Data Governance
13. Future Extensions

---

# 1. Introduction

The platform separates data into three logical domains:

* Master Data
* Transactional Data
* Analytical Data

This separation simplifies maintenance, improves scalability, and enables both real-time and historical analytics.

---

# 2. Data Modeling Approach

The data model follows these principles:

* Normalize reference data.
* Keep streaming events immutable.
* Avoid duplicated master records.
* Use surrogate keys for business entities.
* Store analytical datasets separately from operational data.
* Support schema evolution.

---

# 3. Core Business Entities

The platform consists of the following entities:

| Entity     | Description                |
| ---------- | -------------------------- |
| Customer   | Registered platform user   |
| Product    | Product available for sale |
| Category   | Product classification     |
| Seller     | Marketplace seller         |
| Warehouse  | Product storage location   |
| Inventory  | Current stock level        |
| Order      | Customer purchase          |
| Order Item | Product inside an order    |
| Payment    | Payment transaction        |
| Review     | Customer feedback          |
| Session    | Customer browsing session  |

---

# 4. Entity Relationship Diagram (Logical)

```text
Customer
    │
    │ 1:N
    ▼
Order
    │
    │ 1:N
    ▼
OrderItem
    │
    │ N:1
    ▼
Product
    │
    │ N:1
    ▼
Category


Seller
    │
    │ 1:N
    ▼
Product


Warehouse
    │
    │ 1:N
    ▼
Inventory
    │
    │ N:1
    ▼
Product


Order
    │
    │ 1:1
    ▼
Payment


Customer
    │
    │ 1:N
    ▼
Review
    │
    │ N:1
    ▼
Product
```

---

# 5. Entity Definitions

## Customer

Represents a registered user.

Key attributes:

* customer_id
* first_name
* last_name
* email
* gender
* birth_date
* country
* city
* signup_date
* loyalty_tier

---

## Product

Represents an item offered for sale.

Attributes:

* product_id
* product_name
* category_id
* seller_id
* brand
* unit_price
* currency
* weight
* status

---

## Category

Groups similar products.

Attributes:

* category_id
* category_name
* parent_category_id

---

## Seller

Represents a merchant.

Attributes:

* seller_id
* seller_name
* city
* country
* rating

---

## Warehouse

Physical storage location.

Attributes:

* warehouse_id
* warehouse_name
* city
* country

---

## Inventory

Tracks available stock.

Attributes:

* inventory_id
* product_id
* warehouse_id
* quantity
* reserved_quantity
* updated_at

---

## Order

Represents a customer purchase.

Attributes:

* order_id
* customer_id
* order_timestamp
* status
* total_amount
* currency

---

## Order Item

Individual product within an order.

Attributes:

* order_item_id
* order_id
* product_id
* quantity
* unit_price
* subtotal

---

## Payment

Represents a payment attempt.

Attributes:

* payment_id
* order_id
* payment_method
* payment_status
* amount
* processed_at

---

## Review

Customer product review.

Attributes:

* review_id
* order_id
* customer_id
* product_id
* rating
* review_text
* review_timestamp

---

## Session

Represents a browsing session.

Attributes:

* session_id
* customer_id
* device_type
* traffic_source
* session_start
* session_end

---

# 6. Master Data

Master Data changes infrequently and is used to enrich streaming events.

Included entities:

* Customer
* Product
* Category
* Seller
* Warehouse

These datasets are loaded into Flink for reference lookups.

---

# 7. Transactional Data

Transactional data is continuously generated.

Examples:

* Orders
* Payments
* Reviews
* Inventory Updates
* Customer Sessions

These records are published to Kafka and processed in real time.

---

# 8. Analytical Data

Analytical datasets are derived from transactional data.

Examples:

* Daily Revenue
* Monthly Revenue
* Top Products
* Top Categories
* Customer Lifetime Value (CLV)
* Average Order Value (AOV)
* Conversion Rate
* Cart Abandonment Rate

These datasets are stored in HDFS as Parquet files and queried using Hive or Spark.

---

# 9. Data Relationships

Key business relationships:

* One Customer can create many Orders.
* One Order contains many Order Items.
* One Product belongs to one Category.
* One Seller can sell many Products.
* One Warehouse stores many Inventory records.
* One Order has one Payment record.
* One Customer can submit many Reviews.

These relationships support both operational processing and business analytics.

---

# 10. Data Enrichment

During stream processing, Apache Flink enriches incoming events using Master Data.

Examples:

* Add customer loyalty tier to payment events.
* Add product category to product view events.
* Add seller information to order events.
* Add warehouse location to inventory updates.

This reduces repeated lookups during downstream analytics.

---

# 11. Naming Conventions

The project follows consistent naming conventions:

* Entity names use singular form (Customer, Product, Order).
* Primary keys end with `_id`.
* Timestamp fields end with `_timestamp` or `_at`.
* Boolean fields begin with `is_` or `has_`.
* Monetary values use `amount` or `price`.

---

# 12. Data Governance

The platform follows these governance principles:

* Every entity has a primary key.
* Reference integrity is maintained.
* Raw events remain immutable.
* Historical data is never overwritten.
* Schema changes require version updates.
* Data quality validation occurs before enrichment.

---

# 13. Future Extensions

The data model is designed to support future entities such as:

* Coupons
* Promotions
* Campaigns
* Shipping
* Returns
* Refunds
* Loyalty Points
* Gift Cards
* Recommendation History
* Customer Support Tickets

These can be added without redesigning the existing model.

---

# Next Document

Continue with:

```text
docs/design/04_Kafka_Topic_Design.md
```

The next document defines the Kafka architecture in detail, including topic naming conventions, partition strategy, replication, retention policies, producer and consumer design, and message routing.
