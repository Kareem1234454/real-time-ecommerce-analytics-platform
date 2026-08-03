# `docs/design/04_Kafka_Topic_Design.md`

# Kafka Topic Design

> This document defines the Apache Kafka architecture used by the Real-Time E-Commerce Analytics Platform. It covers topic organization, partitioning strategy, replication, retention, naming conventions, message routing, producer and consumer design, and operational best practices.

---

# Table of Contents

1. Introduction
2. Kafka Architecture
3. Design Objectives
4. Topic Naming Convention
5. Kafka Topics
6. Topic Configuration
7. Partition Strategy
8. Replication Strategy
9. Message Keys
10. Producer Design
11. Consumer Groups
12. Message Ordering
13. Retention Policy
14. Dead Letter Queue (DLQ)
15. Monitoring
16. Best Practices
17. Future Enhancements

---

# 1. Introduction

Apache Kafka serves as the central event streaming platform for the Real-Time E-Commerce Analytics Platform.

All business events are published to Kafka before being processed by downstream services.

Kafka decouples producers from consumers, allowing multiple services to process the same event independently.

---

# 2. Kafka Architecture

```text
                    Event Generator
                           │
                           ▼
                    Kafka Producers
                           │
                           ▼
                  Apache Kafka Cluster
                           │
      ┌──────────────┬──────────────┬──────────────┐
      ▼              ▼              ▼
 Apache Flink   Fraud Service   Monitoring
      │
      ▼
     Data Lakehouse
```

---

# 3. Design Objectives

The Kafka layer is designed to:

* Support high event throughput.
* Preserve event ordering where required.
* Scale horizontally.
* Prevent data loss.
* Support replay of historical events.
* Isolate event categories.
* Simplify consumer development.

---

# 4. Topic Naming Convention

Topic names follow the format:

```text
<domain>-events
```

Examples:

* search-events
* product-view-events
* cart-events
* checkout-events
* payment-events
* order-events
* review-events
* inventory-events

System topics:

* fraud-alerts
* dead-letter-events
* system-metrics

This convention improves readability and simplifies operational management.

---

# 5. Kafka Topics

| Topic               | Description             | Producer            | Primary Consumer |
| ------------------- | ----------------------- | ------------------- | ---------------- |
| search-events       | Customer search actions | Event Generator     | Flink            |
| product-view-events | Product page views      | Event Generator     | Flink            |
| cart-events         | Cart operations         | Event Generator     | Flink            |
| checkout-events     | Checkout lifecycle      | Event Generator     | Flink            |
| payment-events      | Payment events          | Payment Simulator   | Flink            |
| order-events        | Order lifecycle         | Payment Service     | Flink            |
| review-events       | Customer reviews        | Review Service      | Flink            |
| inventory-events    | Inventory changes       | Inventory Service   | Flink            |
| fraud-alerts        | Fraud detection results | Flink               | Dashboard        |
| system-metrics      | Platform metrics        | Monitoring Services | Grafana          |
| dead-letter-events  | Invalid events          | Flink               | Operations       |

---

# 6. Topic Configuration

Default configuration for business topics:

| Property           |  Value |
| ------------------ | -----: |
| Partitions         |     12 |
| Replication Factor |      3 |
| Cleanup Policy     | delete |
| Compression        |   zstd |
| Retention          | 7 days |

Notes:

* Production deployments may increase the number of partitions.
* Compression reduces network traffic and storage requirements.
* Retention should align with replay and recovery needs.

---

# 7. Partition Strategy

Choosing the correct partition key is essential for preserving event order.

| Topic               | Partition Key |
| ------------------- | ------------- |
| search-events       | customer_id   |
| product-view-events | customer_id   |
| cart-events         | customer_id   |
| checkout-events     | order_id      |
| payment-events      | order_id      |
| order-events        | order_id      |
| review-events       | product_id    |
| inventory-events    | product_id    |

Using these keys ensures related events remain in order while distributing load across brokers.

---

# 8. Replication Strategy

Each topic uses a replication factor of **3**.

Benefits:

* Broker failure tolerance.
* Improved availability.
* Reduced risk of data loss.

Replication should be adjusted according to cluster size in production environments.

---

# 9. Message Keys

Message keys are selected to balance ordering and scalability.

Examples:

* Customer browsing events use `customer_id`.
* Payment and order events use `order_id`.
* Inventory events use `product_id`.

This approach preserves logical ordering for related events without forcing all messages into a single partition.

---

# 10. Producer Design

Each producing service is responsible for a specific event domain.

General producer responsibilities:

* Validate payloads before publishing.
* Include standard event metadata.
* Use idempotent producers where supported.
* Retry transient failures.
* Log publishing errors.

Future improvements may include a Schema Registry for centralized schema validation.

---

# 11. Consumer Groups

Independent consumer groups allow multiple services to process the same data.

| Consumer Group          | Purpose                   |
| ----------------------- | ------------------------- |
| flink-stream-processing | Real-time transformations |
| analytics-service       | Live business metrics     |
| fraud-detection         | Fraud analysis            |
| monitoring-service      | Operational metrics       |
| audit-service           | Event archiving           |

Because each consumer group maintains its own offsets, services can evolve independently.

---

# 12. Message Ordering

Ordering is guaranteed only within a single partition.

By selecting appropriate partition keys, related events (such as all events for one order) are processed in sequence.

Cross-partition ordering is not guaranteed and should not be relied upon.

---

# 13. Retention Policy

Retention determines how long Kafka stores messages.

Recommended defaults:

| Topic Type        | Retention |
| ----------------- | --------- |
| Business Events   | 7 days    |
| Fraud Alerts      | 30 days   |
| Dead Letter Queue | 30 days   |
| System Metrics    | 3 days    |

Longer retention enables replay for debugging and recovery but increases storage requirements.

---

# 14. Dead Letter Queue (DLQ)

Events that cannot be processed are redirected to:

```text
dead-letter-events
```

Typical causes:

* Invalid JSON
* Missing required fields
* Schema mismatch
* Unknown event type
* Business rule violation

The DLQ allows investigation without interrupting the main processing pipeline.

---

# 15. Monitoring

Operational metrics should include:

* Broker availability
* Topic throughput
* Consumer lag
* Partition distribution
* Failed publishes
* Failed consumptions
* Disk usage
* Network throughput

These metrics are collected by Prometheus and visualized in Grafana.

---

# 16. Best Practices

The platform follows these Kafka best practices:

* Use dedicated topics for major event categories.
* Keep event payloads immutable.
* Use meaningful partition keys.
* Avoid oversized messages.
* Configure retries for transient failures.
* Monitor consumer lag continuously.
* Isolate failed events using a DLQ.
* Enable compression to reduce bandwidth usage.

---

# 17. Future Enhancements

Future versions of the platform may include:

* Confluent Schema Registry
* Apache Avro serialization
* Protobuf support
* Kafka Connect
* Debezium (CDC)
* Tiered Storage
* Multi-region Kafka clusters
* Exactly-once end-to-end processing

---

# Next Document

Continue with:

```text
docs/design/05_Flink_Job_Design.md
```

The next document defines every Apache Flink job, including data sources, transformation pipelines, windowing strategy, state management, checkpointing, sink design, and fault recovery.
