# `docs/design/06_Medallion_Data_Lake_Storage_Design.md`

# Enterprise Hadoop HDFS Medallion Data Lake Storage Design

> This document defines the Distributed Data Lake architecture for the Real-Time E-Commerce Analytics Platform. It details our implementation of a containerized **Apache Hadoop Distributed File System (HDFS)** paired with a high-performance Medallion Distributed Data Lake utilizing Snappy-compressed Apache Parquet and immutable JSON-Lines storage tiers.

---

## Table of Contents

1. [Executive Summary & Storage Objectives](#1-executive-summary--storage-objectives)
2. [Architectural Evolution: Containerized Apache Hadoop HDFS & WebHDFS Bridge](#2-architectural-evolution-containerized-apache-hadoop-hdfs--webhdfs-bridge)
3. [Medallion Storage Tiers (Bronze → Silver → Gold)](#3-medallion-storage-tiers-bronze--silver--gold)
4. [Directory Hierarchy & Partitioning Strategy](#4-directory-hierarchy--partitioning-strategy)
5. [File Formats & Compression Standards](#5-file-formats--compression-standards)
6. [Data Lifecycle & Resilience Integration](#6-data-lifecycle--resilience-integration)

---

## 1. Executive Summary & Storage Objectives

The platform combines distributed enterprise storage reliability with high-speed columnar analytical querying by implementing an **Enterprise Distributed Data Lake**. This architecture harmoniously integrates three foundational concepts:
1. **HDFS Infrastructure**: A containerized **Apache Hadoop Distributed File System** (`hadoop-namenode` and `hadoop-datanode`) handling physical file blocks, replication, and distributed cluster storage accessible via WebHDFS HTTP port `9870`.
2. **Medallion Data Lake Pattern**: An organized repository structuring raw, structured, and aggregated files into multi-hop quality refinement zones (**Bronze**, **Silver**, and **Gold**).
3. **Distributed Data Lake Operations**: The synergistic combination of our HDFS storage lake with columnar **Apache Parquet tables**, streaming **Apache Flink calculations**, deep **Apache Spark OLAP joins**, and relational **PostgreSQL metadata logs**, granting high-performance consistency without relinquishing cloud-scale file storage flexibility.

The storage layer is engineered to satisfy six core operational requirements:
* **Distributed HDFS Storage**: Persist immutable streaming logs and analytical tables inside dedicated HDFS blocks, inspectable live via the Hadoop NameNode UI (`http://localhost:9870`).
* **Zero-Latency Ingestion**: Buffer raw streaming JSON messages directly from Apache Kafka into HDFS Bronze partitions without schema evaluation bottlenecks.
* **High-Throughput Analytical Scanning**: Utilize columnar Apache Parquet files in HDFS Silver and Gold tiers to power sub-second executive dashboard queries without relational database locking overhead.
* **ACID Master & Stream Hybridization**: Enable seamless inner hash-joins across static customer catalogs and dynamic real-time transaction streams.
* **Frictionless Windows Compatibility**: Access HDFS blocks natively from Python engines via a memory-buffered WebHDFS REST connector (`utils/hdfs_client.py`) without ever requiring local Java Virtual Machine or WinUtils binaries on the Windows host.
* **Dual-Write Mirroring & Hybrid Fallback**: To guarantee absolute resilience on Windows development machines, all streaming event generators and Flink workers execute synchronous dual-writes to both containerized HDFS storage blocks and local filesystem Data Lake mirrors (`data_lake/`). Subsequent analytical consumers (Streamlit UI, Spark batch pipelines) execute instantaneous hybrid read fallbacks to local mirrored Parquet blocks should Docker network timeouts occur.

---

## 2. Architectural Evolution: Containerized Apache Hadoop HDFS & WebHDFS Bridge

While legacy 2010-era Hadoop deployments relied on disk-heavy MapReduce and Apache Hive (requiring 30 to 60 seconds of query compilation latency), our platform completely modernizes the HDFS ecosystem by integrating **Apache Parquet, Apache Spark, PyFlink, and WebHDFS**:

| Architectural Dimension | Legacy MapReduce / Hive Approach | Modern Containerized Hadoop HDFS & WebHDFS (Our Implementation) |
| :--- | :--- | :--- |
| **System Resource Footprint**| Requires installing massive Java daemons natively across host machines, causing complex winutils.exe failures on Windows environments. | **Containerized & Encapsulated**: Hadoop NameNode and DataNode run cleanly isolated in Docker microservices with named block volumes (`hadoop_namenode`, `hadoop_datanode`). Zero Java required on Windows host! |
| **Query Engine Latency** | Slow Disk-Based MapReduce jobs incurring heavy disk I/O and multirecord evaluation stalls. | **In-Memory Speed (10x-100x Faster)**: Apache Spark and PyFlink execute vectorized real-time queries directly over compressed columnar Parquet blocks stored in HDFS. |
| **Operational Maintenance** | Complex configuration XMLs and manual binary installations. | **Zero-Touch Automation**: Controlled via clean Python scripts (`utils/hdfs_client.py`, `create_lake_directories.py`) leveraging high-speed HTTP REST streaming over port `9870`. |

---

## 3. Medallion Storage Tiers (Bronze → Silver → Gold)

```
                       [Apache Kafka Topic Channels]
                                     │
                                     ▼
         ┌──────────────────────────────────────────────────────┐
         │              BRONZE TIER (Raw Telemetry)             │
         │     Location: HDFS /data_lake/bronze/ (Port 9870)    │
         │  Format: .jsonl (Immutable Append-Only Log Buffers)  │
         └───────────────────────────┬──────────────────────────┘
                                     │
                          (Flink Jobs 1 & 2 Joins)
                                     │
                                     ▼
         ┌──────────────────────────────────────────────────────┐
         │             SILVER TIER (Enriched Parquet)           │
         │     Location: HDFS /data_lake/silver/ (Port 9870)    │
         │  Format: .parquet (Snappy-Compressed Columnar Store) │
         └───────────────────────────┬──────────────────────────┘
                                     │
                         (Flink Jobs 3, 4 & Spark)
                                     │
                                     ▼
         ┌──────────────────────────────────────────────────────┐
         │          GOLD TIER (Executive BI & CEP Sinks)        │
         │     Location: HDFS /data_lake/gold/   (Port 9870)    │
         │    Format: .parquet (Pre-Computed Analytical Tables) │
         └──────────────────────────────────────────────────────┘
```

---

## 4. Directory Hierarchy & Partitioning Strategy

To eliminate full-table scanning bottlenecks, all layers enforce timestamped hive-style folder partitioning directly within Hadoop HDFS blocks (`http://localhost:9870` -> Utilities -> Browse the file system):

```text
HDFS Root -> /data_lake/
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

* **Bronze Tier (`.jsonl`)**: Utilizes line-delimited JSON format. Each line represents a self-describing, complete event payload. Guaranteed compatibility with streaming generators and WebHDFS log appends.
* **Silver & Gold Tiers (`.parquet`)**: Implements **Apache Parquet with Snappy Compression**:
  * **Columnar Pruning**: Analytical queries reading `avg_unit_price` bypass scanning text fields or timestamps completely, drastically accelerating HDFS block throughput.
  * **Snappy Compression**: Achieves ~70% data volume compression while maintaining ultra-low decompression CPU cycles during live Streamlit UI rendering from HDFS.

---

## 6. Data Lifecycle & Resilience Integration

* **Automated Factory Wiping**: By executing `scripts\clean_reset.bat`, the system purges all Docker database and HDFS named persistent volumes (`hadoop_namenode`, `hadoop_datanode`), and recreates pristine HDFS storage block structures using `scripts\create_lake_directories.py`.
* **Hybrid HDFS Failover Coupling**: While Flink Job 4 simultaneously pushes security incidents into PostgreSQL and HDFS `/data_lake/gold/fraud_alerts_log/`, our Streamlit UI is engineered with resilient failover logic: if database services undergo reboot maintenance, dashboards immediately revert to querying HDFS Gold Parquet files via `utils/hdfs_client.py`. If the HDFS Docker cluster itself is offline during offline development testing, operations cleanly fall back to local folder mirrors (`data_lake/`)!

---
*End of Enterprise Hadoop HDFS Medallion Data Lake Storage Design Specification.*ghput.
  * **Snappy Compression**: Achieves ~70% data volume compression while maintaining ultra-low decompression CPU cycles during live Streamlit rendering.

---

## 6. Data Lifecycle & Resilience Integration

* **Automated Factory Wiping**: By executing `scripts\clean_reset.bat`, the system purges all generated stream logs within `data_lake/` and recreates the pristine empty partition architecture using `scripts\create_lake_directories.py`.
* **Hybrid Failover Coupling**: While Flink Job 4 simultaneously pushes security incidents into PostgreSQL and `data_lake/gold/fraud_alerts_log/`, our Streamlit UI is engineered with resilient failover logic: if database services undergo reboot maintenance, dashboards immediately revert to querying these Gold Parquet files to prevent executive visibility downtime.

---
*End of Medallion Data Lake Storage Design Specification.*
