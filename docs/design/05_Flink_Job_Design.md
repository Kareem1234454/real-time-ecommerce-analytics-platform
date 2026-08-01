# `docs/design/05_Flink_Job_Design.md`

# Apache Flink Job Design

> This document defines the stream processing architecture implemented using Apache Flink. It describes the processing pipeline, individual jobs, state management, windowing strategies, checkpointing, fault tolerance, and sink architecture.

---

# Table of Contents

1. Introduction
2. Processing Objectives
3. High-Level Pipeline
4. Flink Jobs Overview
5. Job 1 – Event Validation
6. Job 2 – Event Enrichment
7. Job 3 – Session Analytics
8. Job 4 – Real-Time KPIs
9. Job 5 – Fraud Detection
10. State Management
11. Windowing Strategy
12. Checkpointing
13. Fault Tolerance
14. Sink Design
15. Performance Considerations
16. Future Enhancements

---

# 1. Introduction

Apache Flink is the real-time processing engine of the platform.

Its responsibilities include:

* Reading events from Kafka.
* Validating event structure.
* Enriching events with master data.
* Computing business metrics.
* Detecting anomalies.
* Writing processed data to downstream storage systems.

The platform uses **independent Flink jobs**, allowing each processing pipeline to evolve and scale independently.

---

# 2. Processing Objectives

The Flink layer is designed to:

* Process events with low latency.
* Maintain state across event streams.
* Handle out-of-order events.
* Perform real-time aggregations.
* Detect business anomalies.
* Produce datasets for analytics.

---

# 3. High-Level Pipeline

```text
Kafka Topics
      │
      ▼
Event Validation
      │
      ▼
Data Enrichment
      │
      ▼
Business Logic
      │
      ├─────────────┐
      ▼             ▼
Real-Time KPIs   Fraud Detection
      │             │
      └──────┬──────┘
             ▼
     Processed Events
             │
             ▼
       HDFS / Hive
```

---

# 4. Flink Jobs Overview

| Job                   | Purpose                    |
| --------------------- | -------------------------- |
| Validation Job        | Validate event schema      |
| Enrichment Job        | Join with master data      |
| Session Analytics Job | Analyze customer sessions  |
| KPI Aggregation Job   | Compute business metrics   |
| Fraud Detection Job   | Detect suspicious behavior |

Each job consumes one or more Kafka topics and publishes results to storage or downstream consumers.

---

# 5. Job 1 – Event Validation

### Purpose

Ensure incoming events conform to the expected schema.

Validation checks include:

* Required fields.
* Data types.
* Timestamp format.
* Event version.
* Business rule validation.

### Output

* Valid events → Enrichment Job.
* Invalid events → `dead-letter-events`.

---

# 6. Job 2 – Event Enrichment

### Purpose

Augment streaming events with reference data.

Examples:

* Customer loyalty tier.
* Product category.
* Seller information.
* Warehouse location.

### Data Sources

* Customer Master Data
* Product Catalog
* Seller Directory
* Inventory Reference

### Output

Enriched events stored in Kafka or written directly to HDFS.

---

# 7. Job 3 – Session Analytics

### Purpose

Analyze user browsing sessions.

Metrics include:

* Session duration.
* Pages viewed.
* Products viewed.
* Cart additions.
* Checkout rate.

Example output:

| Metric                   | Value |
| ------------------------ | ----: |
| Average Session Duration | 7 min |
| Average Products Viewed  |    12 |
| Cart Abandonment Rate    |   18% |

---

# 8. Job 4 – Real-Time KPIs

### Purpose

Continuously calculate business metrics.

Examples:

* Revenue per minute.
* Orders per minute.
* Active users.
* Top-selling products.
* Top categories.
* Conversion rate.

Results are written to analytical storage and exposed to dashboards.

---

# 9. Job 5 – Fraud Detection

### Purpose

Identify suspicious transactions.

Initial rule-based examples:

* Multiple failed payments within a short interval.
* High-value purchases from new accounts.
* Unusual purchasing frequency.
* Multiple orders from different countries using the same account.

Detected events are published to the `fraud-alerts` Kafka topic.

---

# 10. State Management

Several jobs require stateful processing.

Examples:

* Session tracking.
* Cart state.
* Running totals.
* Fraud counters.

The platform uses Flink managed state to ensure consistency during failures and restarts.

---

# 11. Windowing Strategy

Different KPIs require different window types.

| Window Type       | Use Case            |
| ----------------- | ------------------- |
| Tumbling Window   | Revenue per minute  |
| Sliding Window    | Active users        |
| Session Window    | Customer behavior   |
| Event Time Window | Historical accuracy |

Late-arriving events are handled using watermarks.

---

# 12. Checkpointing

Checkpointing protects processing state.

Recommended configuration:

* Interval: 30 seconds
* Mode: Exactly-once
* Storage: HDFS
* Externalized checkpoints: Enabled

This allows jobs to recover from failures without losing progress.

---

# 13. Fault Tolerance

Recovery strategy:

* Automatic restart on transient failures.
* State restoration from latest checkpoint.
* Invalid records redirected to DLQ.
* Retry transient sink failures.

These mechanisms minimize downtime and prevent data loss.

---

# 14. Sink Design

Processed data is written to multiple destinations.

| Sink               | Purpose              |
| ------------------ | -------------------- |
| HDFS               | Long-term storage    |
| Hive               | SQL analytics        |
| Kafka              | Downstream consumers |
| Fraud Alerts Topic | Security monitoring  |

This multi-sink architecture supports both operational and analytical workloads.

---

# 15. Performance Considerations

Recommended settings:

* Parallelism based on available CPU cores.
* Event-time processing enabled.
* Efficient serialization.
* Batched writes to storage.
* Partition-aware processing.

Performance should be monitored continuously using Prometheus and Grafana.

---

# 16. Future Enhancements

Future versions may include:

* CEP (Complex Event Processing).
* Machine Learning inference.
* Dynamic rule engine.
* Real-time recommendation engine.
* Stateful feature engineering.
* Integration with Apache Iceberg.

---

# Next Document

Continue with:

```text
docs/design/06_HDFS_Storage_Design.md
```

The next document defines the Data Lake layout, storage zones, partitioning strategy, file formats, retention policies, and lifecycle management for all datasets stored in HDFS.
