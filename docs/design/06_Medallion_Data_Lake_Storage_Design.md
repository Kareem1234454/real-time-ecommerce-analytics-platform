# `docs/design/06_Medallion_Data_Lake_Storage_Design.md`

# Modern Medallion Data Lake Storage Design

> This document defines the Data Lakehouse architecture for the Real-Time E-Commerce Analytics Platform. It details our intentional transition away from legacy HDFS toward a high-performance local Medallion Data Lakehouse utilizing Snappy-compressed Apache Parquet and immutable JSON-Lines storage tiers.

---

## Table of Contents

1. [Executive Summary & Storage Objectives](#1-executive-summary--storage-objectives)
2. [Architectural Evolution: Why We Bypassed Legacy HDFS](#2-architectural-evolution-why-we-bypassed-legacy-hdfs)
3. [Medallion Storage Tiers (Bronze → Silver → Gold)](#3-medallion-storage-tiers-bronze--silver--gold)
4. [Directory Hierarchy & Partitioning Strategy](#4-directory-hierarchy--partitioning-strategy)
5. [File Formats & Compression Standards](#5-file-formats--compression-standards)
6. [Data Lifecycle & Resilience Integration](#6-data-lifecycle--resilience-integration)

---

## 1. Executive Summary & Storage Objectives

The platform replaces complex traditional clustered storage engines with a lean, enterprise-grade **Medallion Data Lakehouse** structured within the `data_lake/` folder architecture.

The storage layer is engineered to satisfy five core operational requirements:
* **Zero-Latency Ingestion**: Buffer raw streaming JSON messages directly from Apache Kafka without schema evaluation bottlenecks.
* **High-Throughput Analytical Scanning**: Utilize columnar Apache Parquet files to power sub-second executive dashboard queries without relational database locking overhead.
* **ACID Master & Stream Hybridization**: Enable seamless inner hash-joins across static customer catalogs and dynamic real-time transaction streams.
* **Disaster Recovery & Immutable Replay**: Preserve unedited telemetry in the Bronze tier to allow continuous historical re-computation.
* **Cross-Environment Portability**: Execute cleanly on any operating system without Java NameNode/DataNode clustering complexities.

---

## 2. Architectural Evolution: Why We Bypassed Legacy HDFS

In standard 2010-era Hadoop deployments, Data Lakes relied heavily on the Hadoop Distributed File System (HDFS) and Apache Hive. However, modern analytical systems have largely converged on **Cloud Object Stores & Local Medallion Parquet Architectures**. 

| Architectural Dimension | Legacy HDFS Approach | Modern Medallion Lakehouse (Our Implementation) |
| :--- | :--- | :--- |
| **System Resource Footprint**| Extremely High. Requires running heavy Java memory daemons (NameNode, Secondary NameNode, DataNodes), frequently causing OOM crashes on Windows environments. | **Ultra-Lightweight**: Files are stored natively on disk using partitioned directory hierarchies (`data_lake/`), zero Java runtime daemons required. |
| **Query Engine Latency** | Slow Disk-Based MapReduce. Traditional Apache Hive queries require 30 to 60 seconds of compilation and startup time for simple SELECT aggregations. | **In-Memory Speed (10x-100x Faster)**: Apache Spark and Apache Flink execute vectorized real-time queries directly over compressed columnar Parquet blocks. |
| **Operational Maintenance** | Complex configuration XMLs, permissions issues, and persistent block synchronization errors. | **Zero-Touch Automation**: Controlled via clean Python scripts (`create_lake_directories.py`) and standard operating system storage drivers. |

---

## 3. Medallion Storage Tiers (Bronze → Silver → Gold)

```
                       [Apache Kafka Topic Channels]
                                     │
                                     ▼
         ┌──────────────────────────────────────────────────────┐
         │              BRONZE TIER (Raw Telemetry)             │
         │  Format: .jsonl (Immutable Append-Only Log Buffers)  │
         └───────────────────────────┬──────────────────────────┘
                                     │
                          (Flink Jobs 1 & 2 Joins)
                                     │
                                     ▼
         ┌──────────────────────────────────────────────────────┐
         │             SILVER TIER (Enriched Parquet)           │
         │  Format: .parquet (Snappy-Compressed Columnar Store) │
         └───────────────────────────┬──────────────────────────┘
                                     │
                         (Flink Jobs 3, 4 & Spark)
                                     │
                                     ▼
         ┌──────────────────────────────────────────────────────┐
         │          GOLD TIER (Executive BI & CEP Sinks)        │
         │    Format: .parquet (Pre-Computed Analytical Tables) │
         └──────────────────────────────────────────────────────┘
```

---

## 4. Directory Hierarchy & Partitioning Strategy

To eliminate full-table scanning bottlenecks, all layers enforce timestamped hive-style folder partitioning:

```text
data_lake/
├── bronze/
│   ├── cart-events/
│   ├── checkout-events/
│   ├── order-events/
│   ├── payment-events/
│   ├── product-view-events/
│   ├── review-events/
│   ├── search-events/
│   ├── seller-events/
│   └── shipment-events/
├── silver/
│   ├── enriched_orders/
│   ├── enriched_payments/
│   ├── enriched_reviews/
│   └── enriched_traffic/
└── gold/
    ├── category_metrics/
    ├── executive_dashboard/
    ├── fraud_alerts_log/
    ├── loyalty_demographics/
    ├── conversion_funnel/
    └── revenue_per_hour/
```
* **Partition Formula**: Within each domain directory, events are systematically segregated by insertion time: `year=YYYY/month=MM/day=DD/hour=HH/`. This allows analytical engines to prune unneeded historical date partitions instantly during querying.

---

## 5. File Formats & Compression Standards

* **Bronze Tier (`.jsonl`)**: Utilizes line-delimited JSON format. Each line represents a self-describing, complete event payload. Guaranteed compatibility with streaming generators and Kafka dump utilities.
* **Silver & Gold Tiers (`.parquet`)**: Implements **Apache Parquet with Snappy Compression**:
  * **Columnar Pruning**: Analytical queries reading `avg_unit_price` bypass scanning text fields or timestamps completely, drastically accelerating input/output throughput.
  * **Snappy Compression**: Achieves ~70% data volume compression while maintaining ultra-low decompression CPU cycles during live Streamlit rendering.

---

## 6. Data Lifecycle & Resilience Integration

* **Automated Factory Wiping**: By executing `scripts\clean_reset.bat`, the system purges all generated stream logs within `data_lake/` and recreates the pristine empty partition architecture using `scripts\create_lake_directories.py`.
* **Hybrid Failover Coupling**: While Flink Job 4 simultaneously pushes security incidents into PostgreSQL and `data_lake/gold/fraud_alerts_log/`, our Streamlit UI is engineered with resilient failover logic: if database services undergo reboot maintenance, dashboards immediately revert to querying these Gold Parquet files to prevent executive visibility downtime.

---
*End of Medallion Data Lake Storage Design Specification.*
