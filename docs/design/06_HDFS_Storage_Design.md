# `docs/design/06_HDFS_Storage_Design.md`

# HDFS Storage Design

> This document defines the Data Lake architecture for the Real-Time E-Commerce Analytics Platform. It describes storage zones, directory layout, partitioning strategy, file formats, lifecycle management, retention policies, and optimization techniques.

---

# Table of Contents

1. Introduction
2. Storage Objectives
3. Data Lake Architecture
4. Storage Zones
5. Directory Structure
6. File Formats
7. Partitioning Strategy
8. Naming Conventions
9. Data Lifecycle
10. Retention Policy
11. Compression Strategy
12. Security Considerations
13. Optimization Techniques
14. Future Enhancements

---

# 1. Introduction

The platform stores all processed data in a Hadoop Distributed File System (HDFS) Data Lake.

The storage layer is designed to support:

* Raw event retention
* Stream processing outputs
* Historical analytics
* Business reporting
* Machine learning datasets

To simplify data management, the Data Lake is divided into logical zones.

---

# 2. Storage Objectives

The storage layer should:

* Preserve original events.
* Support efficient analytics.
* Minimize storage costs.
* Improve query performance.
* Enable historical replay.
* Scale horizontally.

---

# 3. Data Lake Architecture

The platform follows a three-layer Medallion Architecture.

```text id="bronze-silver-gold"
                  Kafka
                    │
                    ▼
        ┌──────────────────────┐
        │ Bronze (Raw Events)  │
        └──────────┬───────────┘
                   ▼
        ┌──────────────────────┐
        │ Silver (Validated &  │
        │ Enriched Events)     │
        └──────────┬───────────┘
                   ▼
        ┌──────────────────────┐
        │ Gold (Business KPIs  │
        │ & Aggregations)      │
        └──────────────────────┘
```

---

# 4. Storage Zones

## Bronze Layer

Purpose:

Store events exactly as received.

Characteristics:

* Immutable
* Append-only
* Original event schema
* Used for replay and auditing

Examples:

* Raw search events
* Raw payment events
* Raw order events

---

## Silver Layer

Purpose:

Store validated and enriched events.

Characteristics:

* Cleaned data
* Master data joined
* Invalid records removed
* Ready for analytics

Examples:

* Enriched order events
* Customer sessions
* Inventory updates

---

## Gold Layer

Purpose:

Store business-ready datasets.

Examples:

* Revenue per hour
* Daily sales
* Top-selling products
* Conversion rate
* Customer lifetime metrics

These datasets are consumed directly by Hive, Spark, and dashboards.

---

# 5. Directory Structure

Recommended layout:

```text id="lake-layout"
/data-lake/

├── bronze/
│   ├── search-events/
│   ├── product-view-events/
│   ├── cart-events/
│   ├── checkout-events/
│   ├── payment-events/
│   └── order-events/
│
├── silver/
│   ├── customer-events/
│   ├── enriched-orders/
│   ├── sessions/
│   └── inventory/
│
└── gold/
    ├── revenue/
    ├── sales/
    ├── customer-metrics/
    ├── product-metrics/
    └── executive-dashboard/
```

---

# 6. File Formats

| Layer  | Format  | Reason                              |
| ------ | ------- | ----------------------------------- |
| Bronze | JSON    | Preserve original event structure   |
| Silver | Parquet | Efficient analytics and compression |
| Gold   | Parquet | Optimized for reporting and BI      |

Future versions may adopt Apache Iceberg for advanced table management.

---

# 7. Partitioning Strategy

All datasets are partitioned by event time.

Example:

```text id="partition-layout"
/silver/order-events/

year=2026/
  month=08/
    day=15/
      hour=14/
```

Large datasets may also be partitioned by:

* country
* category
* warehouse

Partitioning improves scan performance and reduces query cost.

---

# 8. Naming Conventions

Guidelines:

* Directory names use lowercase.
* Words are separated with hyphens.
* Partition keys use `key=value` format.
* Dataset names describe business meaning.

Examples:

* `customer-metrics`
* `payment-events`
* `inventory-updates`

---

# 9. Data Lifecycle

Every dataset follows the same lifecycle.

```text id="data-lifecycle"
Kafka Event
      │
      ▼
Bronze
      │
Validation
      ▼
Silver
      │
Aggregation
      ▼
Gold
      │
Hive / Spark / Dashboard
```

This layered approach ensures traceability and simplifies debugging.

---

# 10. Retention Policy

Recommended retention:

| Layer  | Retention                                |
| ------ | ---------------------------------------- |
| Bronze | 90 days                                  |
| Silver | 365 days                                 |
| Gold   | 2 years (or according to business needs) |

Retention values should be configurable and aligned with organizational policies.

---

# 11. Compression Strategy

Compression reduces storage usage and improves read performance.

Recommended codecs:

| Layer  | Compression  |
| ------ | ------------ |
| Bronze | None or Gzip |
| Silver | Snappy       |
| Gold   | Snappy       |

Snappy provides a good balance between compression ratio and processing speed.

---

# 12. Security Considerations

The storage layer should support:

* Role-based access control (RBAC)
* Dataset-level permissions
* Encryption at rest (production)
* Audit logging
* Backup and recovery procedures

Sensitive fields (such as personally identifiable information) should be masked or tokenized where required.

---

# 13. Optimization Techniques

Recommended optimizations:

* Store analytical datasets in Parquet.
* Avoid many small files.
* Batch writes from Flink.
* Partition by frequently queried dimensions.
* Compact files periodically.
* Archive obsolete datasets.

These practices improve long-term performance and maintainability.

---

# 14. Future Enhancements

Potential improvements:

* Apache Iceberg tables
* Delta Lake compatibility
* Automatic compaction
* Tiered storage
* Object storage integration (Amazon S3, Azure Data Lake Storage, Google Cloud Storage)
* Data catalog integration

---

# Next Document

Continue with:

```text id="next-dashboard-design"
docs/design/07_Dashboard_Design.md
```

The next document defines the dashboard architecture, KPI catalog, visualization strategy, refresh intervals, user roles, and monitoring dashboards used by the platform.
