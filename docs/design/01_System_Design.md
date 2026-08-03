# System Design Document

> This document describes the system design of the **Real-Time E-Commerce Analytics Platform**, including design principles, architectural decisions, component interactions, scalability strategies, reliability mechanisms, and deployment considerations.

---

# Table of Contents

1. Introduction
2. Business Requirements
3. Functional Requirements
4. Non-Functional Requirements
5. Design Principles
6. High-Level Architecture
7. Core Components
8. Service Responsibilities
9. Data Flow
10. Communication Patterns
11. Scalability
12. Reliability
13. Performance
14. Deployment Strategy
15. Design Decisions

---

# 1. Introduction

The Real-Time E-Commerce Analytics Platform is designed to simulate a modern production-grade analytics platform capable of processing high volumes of streaming events.

The platform demonstrates how distributed systems can be combined to ingest, process, store, analyze, and visualize e-commerce events with minimal latency.

The architecture is inspired by real-world Data Engineering platforms used by large-scale online retailers.

---

# 2. Business Requirements

The system should answer business questions in near real time.

Examples include:

- How many active users are currently browsing?
- What is the current revenue?
- Which products are trending?
- Which customers abandoned their carts?
- Which payments failed?
- Which products require restocking?
- Are any fraudulent transactions occurring?

The platform should process these events continuously rather than relying on scheduled batch jobs.

---

# 3. Functional Requirements

The platform shall:

- Generate realistic customer activity.
- Stream events continuously.
- Process events in real time.
- Validate incoming records.
- Enrich business events.
- Detect fraudulent behavior.
- Compute business KPIs.
- Store raw events.
- Store processed events.
- Support SQL analytics.
- Produce dashboards.
- Support historical reporting.

---

# 4. Non-Functional Requirements

The system should provide:

### Scalability

Support horizontal scaling.

---

### Reliability

Prevent data loss whenever possible.

---

### Availability

Services should recover automatically after failures.

---

### Maintainability

Every component should have a single responsibility.

---

### Extensibility

New event types should be added without redesigning the system.

---

### Observability

Every service should expose logs and metrics.

---

# 5. Design Principles

The architecture follows several engineering principles.

## Event-Driven Architecture

Every business action becomes an immutable event.

---

## Loose Coupling

Services communicate through Kafka rather than directly.

---

## Single Responsibility Principle

Each service performs one business responsibility.

---

## Immutable Data

Raw events are never modified.

---

## Horizontal Scalability

Additional nodes increase processing capacity.

---

## Fault Isolation

A failure in one service should not stop the entire platform.

---

# 6. High-Level Architecture

```text
                    Customer Activity
                           │
                           ▼
                  Event Generator
                           │
                           ▼
                     Apache Kafka
                           │
         ┌─────────────────┼──────────────────┐
         ▼                 ▼                  ▼
   Apache Flink      Alert Engine      Audit Consumer
         │
         ▼
 Medallion Parquet Lake
         │
         ▼
    PostgreSQL & DBs
         │
         ▼
     Apache Spark
         │
         ▼
 Dashboard & Reports
```

---

# 7. Core Components

The platform is composed of independent services.

| Service | Responsibility |
|----------|----------------|
| Event Generator | Creates customer events |
| Kafka | Event transport |
| Flink & Workers | Stream processing |
| Medallion Lake | Columnar Data Lake |
| PostgreSQL | Operational DB & Reference data |
| Spark | Historical batch analytics |
| Grafana | Dashboards |
| Prometheus | Monitoring |

---

# 8. Service Responsibilities

## Event Generator

Produces customer sessions.

---

## Kafka

Buffers and distributes events.

---

## Flink

Processes business logic.

---

## Medallion Data Lakehouse

Stores immutable JSONL & Parquet datasets.

---

## PostgreSQL & Streamlit

Provides real-time SQL alerting & UI dashboards.

---

## Spark

Runs analytical workloads.

---

## Grafana

Visualizes metrics.

---

# 9. Data Flow

Every event follows the same lifecycle.

```text
Customer

↓

Generate Event

↓

Kafka

↓

Flink Validation

↓

Business Logic

↓

Aggregation

↓

Storage

↓

Analytics

↓

Dashboard
```

---

# 10. Communication Patterns

The platform uses asynchronous communication.

```
Producer

↓

Kafka Topic

↓

Consumers
```

Advantages:

- Decoupled services
- Better scalability
- Fault tolerance
- Replay capability

---

# 11. Scalability Strategy

Each service scales independently.

## Kafka

Increase Brokers

Increase Partitions

---

## Flink

Increase TaskManagers

Increase Parallelism

---

## Spark

Increase Workers

---

## Medallion Data Lake

Expand Local & Cloud Object Storage

---

## Event Generator

Launch additional generator instances.

---

# 12. Reliability Strategy

Reliability mechanisms include:

Kafka

- Topic replication
- Persistent log

Flink

- Checkpointing
- State recovery

Medallion Lakehouse

- Columnar Parquet compression & durability

Docker

- Restart policies

---

# 13. Performance Targets

| Metric | Target |
|---------|--------|
| Event Throughput | 5,000+ events/sec |
| Processing Latency | < 2 seconds |
| Kafka Availability | 99.9% |
| Dashboard Refresh | Every 5 seconds |
| Batch Processing | Hourly |

---

# 14. Deployment Strategy

All services run in Docker containers.

The project is designed for local development but can later be deployed to Kubernetes or cloud platforms such as AWS, Azure, or Google Cloud.

---

# 15. Design Decisions

### Why Kafka?

Reliable event streaming.

---

### Why Flink?

Low-latency stateful stream processing.

---

### Why Medallion Data Lakehouse?

High-performance columnar storage for large datasets without JVM filesystem bloat.

---

### Why PostgreSQL & Spark?

Sub-second relational queries for alerts & high-throughput historical analytical engines.

---

### Why Spark?

Historical analytics and large-scale reporting.

---

### Why Docker?

Reproducible environments and simplified deployment.

---

# Next Document

Continue with:

docs/design/02_Event_Model.md

The next document defines every event exchanged between services, including schemas, lifecycle, validation rules, and business semantics.