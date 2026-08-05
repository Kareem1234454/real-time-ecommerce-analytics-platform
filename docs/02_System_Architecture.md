# `docs/02_System_Architecture.md`

# System Architecture

> This document describes the complete architecture of the **Real-Time E-Commerce Analytics Platform**, including every component, its responsibilities, communication flow, and design decisions.

---

# Table of Contents

1. Architecture Overview
2. Design Goals
3. High-Level Architecture
4. System Components
5. End-to-End Data Flow
6. Data Lifecycle
7. Component Responsibilities
8. Storage Architecture
9. Processing Architecture
10. Monitoring Architecture
11. Deployment Architecture
12. Scalability Considerations
13. Fault Tolerance
14. Security Considerations
15. Future Architecture

---

# 1. Architecture Overview

The platform follows an **Enterprise Distributed Data Lake** paradigm where every customer interaction becomes a continuous streaming event. Instead of writing business transactions directly into traditional rigid databases, every interaction is first published to Apache Kafka channels, allowing multiple downstream processing consumers to ingest and enrich telemetry independently.

To harmonize scalable storage with sub-second analytical aggregations, our storage and processing layers unify three fundamental Big Data concepts:
1. **Apache Hadoop HDFS (Infrastructure)**: A containerized Hadoop cluster (`hadoop-namenode` port `9870` and `hadoop-datanode`) providing physical distributed file storage blocks, data replication, and high-throughput network availability.
2. **Medallion Data Lake (Organization Pattern)**: A structured storage engineering methodology dividing HDFS directories into quality refinement tiers: **Bronze** (raw immutable `.jsonl`), **Silver** (cleansed Snappy `.parquet`), and **Gold** (aggregated business executive reporting tables).
3. **Distributed Data Lake (Platform Ecosystem)**: The holistic unification of our Hadoop HDFS Data Lake repository with streaming **Apache Flink calculations**, deep **Apache Spark OLAP historical joins**, columnar **Apache Parquet compression**, and relational **PostgreSQL OLTP logs**, providing high-throughput querying on scalable distributed storage.

### Dual-Write Mirroring & Resilience
To ensure uninterrupted operational continuity across Windows development laptops and Docker network transitions, all analytical workers execute **Dual-Write Mirroring** (writing real-time events simultaneously to HDFS blocks and local Data Lake mirrored folders) coupled with automatic **Hybrid Read-Fallback** across all visualization interfaces.

This architecture is designed to provide:
* High Throughput & Sub-Second Latency
* Decoupled Horizontal Scalability
* Fault Tolerance & Dual-Storage Resilience
* Near Real-Time Executive BI Visualizations

---

# 2. Design Goals

The architecture was designed to satisfy the following requirements:

* Process thousands of events per second.
* Support real-time analytics.
* Preserve raw data for future processing.
* Separate streaming and batch workloads.
* Scale individual services independently.
* Provide reliable event delivery.
* Support future cloud deployment.
* Follow production-ready Data Engineering practices.

---

# 3. High-Level Architecture

```text
                           Users
                              │
                              ▼
                    Event Generator Service
                              │
                              ▼
                     Apache Kafka Cluster
                              │
      ┌───────────────┬───────────────┬───────────────┐
      ▼               ▼               ▼
 Product Events   Order Events   Payment Events
      │               │               │
      └───────────────┴───────────────┘
                      │
                      ▼
                 Apache Flink
                      │
      ┌───────────────┼────────────────┐
      ▼               ▼                ▼
 Validation      Enrichment      Aggregations
      │               │                │
      └───────────────┴────────────────┘
                      │
                      ▼
                 Processed Events
                      │
      ┌───────────────┼────────────────────────┐
      ▼               ▼                        ▼
  Medallion Lake   PostgreSQL             Alert Topic
      │
      ▼
 PostgreSQL & Spark
      │
      ▼
 Apache Spark
      │
      ▼
 Grafana Dashboard
```

---

# 4. System Components

## 4.1 Event Generator

The Event Generator simulates realistic customer behavior.

Instead of creating random events, it simulates complete customer journeys.

Example:

```
Search Product

↓

View Product

↓

View Product

↓

Add To Cart

↓

Checkout

↓

Payment

↓

Review
```

Responsibilities:

* Generate customer sessions
* Simulate user behavior
* Publish Kafka events
* Control event rate
* Generate realistic timestamps

---

## 4.2 Apache Kafka

Kafka is the central messaging layer.

Every business event is published into a Kafka topic before processing.

Topics include:

* product_views
* searches
* carts
* orders
* payments
* inventory
* reviews
* alerts

Kafka provides:

* Event buffering
* Decoupled services
* Scalability
* Reliable message delivery

---

## 4.3 Apache Flink

Apache Flink is responsible for stream processing.

Main responsibilities:

* Read Kafka events
* Validate records
* Remove invalid events
* Enrich business data
* Calculate KPIs
* Detect fraud
* Produce processed events

Flink performs processing continuously with very low latency.

---

## 4.4 Medallion Distributed Data Lake

The Distributed Data Lake stores all business events across three logical tiers:

```
bronze/ (Raw immutable JSONL events)

silver/ (Cleansed & enriched Parquet)

gold/ (Aggregated analytical executive tables)
```

Each dataset is partitioned by Year, Month, Day, and Hour to improve query performance and simplify storage.

---

## 4.5 PostgreSQL & Streamlit Operational Analytics

PostgreSQL houses structured reference metadata catalogs and instant, sub-second security fraud alarms.

Streamlit connects directly to PostgreSQL (with fail-safe fallback to Gold Parquet files) for uninterrupted real-time visual monitoring.

---

## 4.6 Apache Spark

Spark performs batch analytics on historical data.

Examples:

* Daily Revenue
* Weekly Revenue
* Monthly Reports
* Customer Lifetime Value
* Best Selling Products
* Sales Trends
* Seasonal Analysis

Spark complements Flink by processing historical rather than live data.

---

## 4.7 PostgreSQL

PostgreSQL stores operational metadata such as:

* Product Catalog
* Customer Profiles
* Warehouse Information
* Configuration Tables

These datasets are used during data enrichment.

---

## 4.8 Grafana

Grafana provides live dashboards for both technical and business users.

Business metrics include:

* Revenue
* Orders
* Active Users
* Conversion Rate
* Inventory Status

Operational metrics include:

* Kafka Lag
* Flink Throughput
* Checkpoint Health
* CPU Usage
* Memory Usage

---

# 5. End-to-End Data Flow

The complete lifecycle of an event is shown below.

```
Customer

↓

Search Product

↓

Event Generator

↓

Kafka Producer

↓

Kafka Topic

↓

Apache Flink

↓

Validation

↓

Cleaning

↓

Enrichment

↓

Business Rules

↓

Window Aggregations

↓

Processed Stream

↓

Medallion Distributed Data Lake

↓

PostgreSQL & Spark

↓

Spark

↓

Grafana
```

---

# 6. Data Lifecycle

Each event passes through multiple stages.

## Stage 1

Generated

↓

## Stage 2

Published to Kafka

↓

## Stage 3

Validated

↓

## Stage 4

Enriched

↓

## Stage 5

Aggregated

↓

## Stage 6

Stored in Medallion Distributed Data Lake

↓

## Stage 7

Queried through PostgreSQL & Spark

↓

## Stage 8

Analyzed by Spark

↓

## Stage 9

Visualized in Grafana

---

# 7. Storage Architecture

The project separates storage into multiple logical layers.

## Raw Layer

Stores events exactly as received.

No modifications are applied.

Purpose:

* Backup
* Replay
* Auditing

---

## Processed Layer

Contains validated and cleaned events.

Purpose:

* Analytics
* Reporting
* Dashboards

---

## Analytics Layer

Contains aggregated datasets generated by Flink and Spark.

Examples:

* Revenue Per Hour
* Top Products
* Customer Metrics

---

## Archive Layer

Stores historical datasets for long-term retention.

---

# 8. Processing Architecture

The platform combines two processing models.

## Streaming Processing

Technology:

Apache Flink

Responsibilities:

* Real-Time KPIs
* Alerts
* Fraud Detection
* Continuous Aggregations

---

## Batch Processing

Technology:

Apache Spark

Responsibilities:

* Historical Reports
* Long-Term Analytics
* Trend Detection

---

# 9. Monitoring Architecture

Monitoring covers every major component.

Kafka

* Broker Health
* Consumer Lag
* Topic Throughput

Flink

* Checkpoints
* Backpressure
* Job Health

Distributed Data Lake

* Storage Usage
* Partition Status

Spark

* Job Duration
* Failed Jobs

Docker

* CPU
* Memory
* Containers

---

# 10. Deployment Architecture

All services run as Docker containers.

```
Docker Compose

├── Kafka

├── Flink JobManager

├── Flink TaskManager

├── Medallion Parquet Data Lake

├── Streamlit Executive Analytics UI

├── PostgreSQL

├── Spark

├── Grafana

├── Prometheus

└── Event Generator
```

---

# 11. Scalability Considerations

The architecture supports horizontal scaling.

Examples:

* Add Kafka Brokers
* Add Flink TaskManagers
* Increase Kafka Partitions
* Add Spark Workers
* Expand Distributed Data Lake Partition Storage

Each service can scale independently.

---

# 12. Fault Tolerance

The platform includes several fault-tolerance mechanisms.

Kafka

* Topic Replication
* Persistent Logs

Flink

* Checkpointing
* Stateful Recovery

Medallion Distributed Data Lake

* Snappy Columnar Compression & Partition Resilience

Docker

* Automatic Restart Policies

These mechanisms minimize data loss and reduce downtime.

---

# 13. Security Considerations

Future production deployments should include:

* Kafka Authentication (SASL)
* TLS Encryption
* Role-Based Access Control (RBAC)
* Secret Management
* Network Isolation
* Audit Logging

---

# 14. Future Architecture

The platform is designed to evolve over time.

Potential improvements include:

* Apache Airflow
* Apache Iceberg
* Delta Lake
* Kubernetes
* MinIO
* Elasticsearch
* Redis
* Machine Learning Pipeline
* Recommendation Engine
* Cloud Deployment (AWS, Azure, GCP)

---

# Next Document

Continue with:

```
docs/03_Data_Sources.md
```

This document explains where the project data comes from, how master data differs from streaming data, how realistic customer events are generated, and how historical datasets are stored for analytics.
