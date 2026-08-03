# `docs/03_Data_Sources.md`

# Data Sources

> This document explains the complete data strategy of the **Real-Time E-Commerce Analytics Platform**, including where the data comes from, how it is generated, how it flows through the platform, and how it is stored for both real-time and historical analytics.

---

# Table of Contents

1. Introduction
2. Data Strategy
3. Why Multiple Data Sources?
4. Master Data
5. Streaming Data
6. Historical Data
7. Dataset Selection
8. Data Model
9. Entity Relationships
10. Event Schemas
11. Data Quality
12. Storage Strategy
13. Partitioning Strategy
14. Data Lifecycle
15. Future Improvements

---

# 1. Introduction

Modern e-commerce platforms operate on multiple categories of data rather than relying on a single dataset.

Some datasets rarely change, such as customer profiles and product catalogs. Other datasets are generated every second by user interactions. Finally, processed data is retained for historical analytics and reporting.

To accurately simulate a production environment, this project uses three different categories of data:

* Master Data
* Streaming Data
* Historical Data

This hybrid approach closely resembles the architecture used in modern online marketplaces.

---

# 2. Data Strategy

The platform combines static reference data with continuously generated streaming events.

```text
                    Data Sources

        ┌──────────────────────────────┐
        │        Master Data           │
        │ Customers                    │
        │ Products                     │
        │ Categories                   │
        │ Sellers                      │
        │ Warehouses                   │
        └──────────────┬───────────────┘
                       │
                       ▼
               Event Generator
                       │
                       ▼
                Streaming Events
                       │
                       ▼
                  Apache Kafka
                       │
                       ▼
                  Apache Flink
                       │
                       ▼
                 Medallion Parquet Lake
                       │
                       ▼
              Historical Data Layer
```

---

# 3. Why Multiple Data Sources?

Real e-commerce companies separate data into logical layers.

This project follows the same principle.

## Master Data

Reference information used for enrichment.

Examples:

* Customers
* Products
* Categories
* Sellers
* Warehouses

---

## Streaming Data

Business events generated continuously.

Examples:

* Product View
* Search
* Add to Cart
* Checkout
* Payment
* Review

---

## Historical Data

Processed data stored for analytics.

Examples:

* Sales History
* Revenue Trends
* Customer Behavior
* Inventory Reports

---

# 4. Master Data

Master Data contains relatively static business information.

These datasets provide additional context during stream processing.

## Customers

Contains customer profile information.

Example fields:

* customer_id
* first_name
* last_name
* gender
* age
* country
* city
* signup_date
* loyalty_level

---

## Products

Contains product catalog information.

Example fields:

* product_id
* product_name
* category
* brand
* price
* supplier
* weight
* dimensions

---

## Categories

Defines the product hierarchy.

Example:

* Electronics
* Fashion
* Home
* Sports
* Beauty
* Books
* Grocery

---

## Sellers

Contains marketplace seller information.

Example fields:

* seller_id
* seller_name
* country
* city
* rating

---

## Warehouses

Stores warehouse metadata.

Example fields:

* warehouse_id
* city
* region
* capacity

---

## Inventory

Tracks product availability.

Example fields:

* product_id
* warehouse_id
* quantity
* last_updated

---

# 5. Dataset Selection

The primary reference dataset used in this project is:

## Olist Brazilian E-Commerce Dataset

The dataset contains realistic e-commerce information including:

* Customers
* Orders
* Products
* Sellers
* Payments
* Reviews
* Geolocation

It serves as the foundation for the platform's master data.

Additional synthetic records can be generated using Python to increase the dataset size for performance testing.

---

# 6. Streaming Data

Unlike Master Data, streaming data never stops.

Every customer action becomes an event.

The platform generates realistic user activity continuously using a dedicated Event Generator service.

---

## Customer Journey

Each customer session follows a logical sequence rather than random event generation.

Example:

```text
Open Website

↓

Search Product

↓

View Product

↓

View Product

↓

Add to Cart

↓

Continue Shopping

↓

Remove Item

↓

Add Another Product

↓

Checkout

↓

Payment

↓

Order Completed

↓

Review Product
```

This approach produces realistic event streams suitable for analytics.

---

# 7. Event Generator

A Python service continuously creates user sessions.

Responsibilities include:

* Simulate customer behavior
* Generate timestamps
* Select realistic products
* Create payment events
* Publish events to Kafka

The event generation speed can be configured to support:

* 100 Events/Second
* 500 Events/Second
* 1,000 Events/Second
* 5,000 Events/Second
* Stress Testing

---

# 8. Streaming Event Types

The platform generates several event types.

## Search Event

Generated whenever a customer searches for a product.

---

## Product View Event

Generated when a customer opens a product page.

---

## Add To Cart Event

Generated when an item is added to the shopping cart.

---

## Remove From Cart Event

Generated when an item is removed.

---

## Wishlist Event

Generated when a product is saved.

---

## Checkout Event

Generated when checkout begins.

---

## Payment Event

Generated after payment processing.

---

## Order Event

Generated when the order is successfully completed.

---

## Review Event

Generated after product delivery.

---

## Inventory Event

Generated whenever inventory changes.

---

# 9. Event Schema

Every streaming event follows a common structure.

```json
{
  "event_id": "uuid",
  "event_type": "product_view",
  "timestamp": "2026-08-01T12:15:42Z",
  "customer_id": 1452,
  "session_id": "abc123",
  "product_id": 507,
  "source": "web",
  "country": "Egypt"
}
```

Each event type may contain additional fields specific to its business logic.

---

# 10. Data Quality

Before processing, every event passes through validation rules.

Checks include:

* Required fields
* Invalid timestamps
* Null values
* Duplicate events
* Invalid prices
* Invalid quantities
* Invalid customer IDs
* Schema validation

Invalid records are redirected to a dedicated Dead Letter Queue (DLQ) for further inspection.

---

# 11. Storage Strategy

The platform stores data in three logical layers.

## Raw Layer

Contains original events exactly as received.

No transformations are applied.

Purpose:

* Replay
* Auditing
* Backup

---

## Processed Layer

Contains validated and enriched events.

Purpose:

* Analytics
* Reporting
* Dashboards

---

## Analytics Layer

Contains aggregated datasets generated by Apache Flink and Apache Spark.

Examples:

* Revenue Per Hour
* Daily Sales
* Customer Metrics
* Product Rankings

---

# 12. Partitioning Strategy

All datasets stored in the Medallion Parquet Data Lake are partitioned using time-based directories.

Example:

```text
/raw/orders/

year=2026/
    month=08/
        day=01/
            hour=14/
```

Partitioning improves:

* Query Performance
* Storage Management
* Data Retention
* Batch Processing

---

# 13. Data Lifecycle

Every event follows the same lifecycle.

```text
Master Data

+

Generated Event

↓

Kafka

↓

Apache Flink

↓

Validation

↓

Enrichment

↓

Processing

↓

Medallion Lake (Parquet)

↓

PostgreSQL & Spark

↓

Apache Spark

↓

Dashboard
```

---

# 14. Design Decisions

The following design choices were made intentionally:

* Separate static and streaming data.
* Use Parquet for efficient storage.
* Store immutable raw events.
* Enrich streaming events using master data.
* Partition datasets by time.
* Preserve historical data for future analysis.
* Generate realistic customer behavior instead of random events.

These decisions improve scalability, maintainability, and analytical accuracy.

---

# 15. Future Improvements

Future enhancements may include:

* Real customer events from REST APIs
* CDC (Change Data Capture)
* Debezium Integration
* Apache Iceberg
* Delta Lake
* Apache Hudi
* Data Versioning
* Data Catalog Integration
* Cloud Object Storage (Amazon S3 / Azure Data Lake Storage / Google Cloud Storage)

---

# Next Document

Continue with:

```text
docs/04_Event_Generator.md
```

The next document explains how the Python Event Generator simulates realistic customer journeys, produces high-volume streaming events, and publishes them to Apache Kafka.
