# `docs/design/05_Flink_Job_Design.md`

# Apache Flink & Streaming Processing Architecture

> This document defines the stream processing architecture implemented in our platform. It details our intentional operational design: implementing high-performance Event-Driven Python Micro-Batch workers for dependable local execution on Windows, while preserving an active containerized Apache Flink cluster to demonstrate production distributed scalability.

---

## Table of Contents
1. [Current Streaming Implementation](#1-current-streaming-implementation)
2. [Apache Flink Infrastructure](#2-apache-flink-infrastructure)
3. [Streaming Architecture Flowchart](#3-streaming-architecture-flowchart)
4. [Streaming Processing Pipeline](#4-streaming-processing-pipeline)
   - [Job 1 – Validation & Dead Letter Queue](#job-1--validation--dead-letter-queue)
   - [Job 2 – Stream Enrichment](#job-2--stream-enrichment)
   - [Job 3 – KPI Aggregation](#job-3--kpi-aggregation)
   - [Job 4 – Fraud Detection](#job-4--fraud-detection)
5. [Future Enhancements](#5-future-enhancements)

---

## 1. Current Streaming Implementation

The current streaming layer is implemented using an **Event-Driven Python Micro-Batch Worker Loop** (`run_streaming_workers.py`) rather than native PyFlink jobs.

The worker continuously executes four streaming processing stages every **5 seconds**, providing near real-time analytics while maintaining reliable execution on Windows development environments.

This design was chosen because native PyFlink deployment on Windows introduces significant compatibility challenges, including Python package availability, Java configuration, filesystem path differences, and dependency management.

The micro-batch architecture provides a stable development experience while preserving the logical structure of a streaming data pipeline.

---

## 2. Apache Flink Infrastructure

An Apache Flink cluster (JobManager and TaskManager) is provisioned through Docker and remains available as part of the platform infrastructure.

In the current implementation, the streaming jobs are **not executed by the Flink runtime**. Instead, the processing logic runs through the Python micro-batch workers.

The containerized Flink cluster is maintained to:
* Demonstrate production-ready distributed infrastructure.
* Support future migration to native Flink jobs.
* Provide an architecture consistent with enterprise streaming platforms.

This allows the project to evolve toward fully distributed Flink execution on Linux or cloud environments without redesigning the overall architecture.

---

## 3. Streaming Architecture Flowchart

```text
                Event Generator
                       │
                       ▼
               Apache Kafka
                       │
                       ▼
      Python Event-Driven Micro-Batch Worker
             (5-second processing cycle)
                       │
      ┌────────┬────────────┬───────────┐
      ▼        ▼            ▼           ▼
 Validation  Enrichment   KPI Engine  Fraud Detection
      │        │            │           │
      └────────┴────────────┴───────────┘
                       │
        ┌──────────────┴──────────────┐
        ▼                             ▼
  Medallion Lakehouse            PostgreSQL
 (Bronze/Silver/Gold)         Fraud Alerts
        │                             │
        └──────────────┬──────────────┘
                       ▼
                  Streamlit Dashboard
```

---

## 4. Streaming Processing Pipeline

The streaming layer consists of four processing stages executed sequentially by the Python worker.

### Job 1 – Validation & Dead Letter Queue
* Validate incoming events against strict schema protocols.
* Detect malformed records or corrupted timestamps.
* Route invalid events directly to the Dead Letter Queue (`data_lake/bronze/dead-letter-events/`).

### Job 2 – Stream Enrichment
* Join streaming events with static customer master data (`customers.parquet`).
* Join streaming events with product catalog data (`products.parquet`).
* Produce enriched business events saved as columnar Parquet files in the Silver Lakehouse tier.

### Job 3 – KPI Aggregation
* Compute streaming business KPIs over stateful tumbling time windows.
* Aggregate running Gross Merchandise Value (GMV) revenue velocities.
* Aggregate interactive payment method statistics and conversion funnel metrics.
* Generate analytical executive dashboard tables in the Gold Lakehouse tier.

### Job 4 – Fraud Detection
* Detect suspicious payment behavior (such as rapid consecutive failed payments on a single shopper account).
* Calculate dynamic risk severity ranging from **75.00 to 99.90** based on financial exposure amount and behavioral variance:
  $$\text{Risk Score} = \min\left(99.90, \max\left(75.00, 78.0 + \frac{\text{Amount}}{80.0} + \text{Variance}(2.1, 12.8)\right)\right)$$
* Store fraud alerts instantly via SQL in PostgreSQL operational tables (`localhost:5432/ecommerce_meta -> fraud_alarms`).
* Archive fraud alerts concurrently as Parquet tables in the Gold Lakehouse layer (`data_lake/gold/fraud_alerts_log/`).

---

## 5. Future Enhancements

The current implementation focuses on reliable local execution using Python micro-batch workers.

Future versions of the platform may migrate the streaming layer to native Apache Flink jobs running directly inside the containerized Flink cluster or on Linux/cloud environments while preserving the existing data pipeline architecture.

---
*End of Apache Flink & Streaming Processing Architecture Specification.*
