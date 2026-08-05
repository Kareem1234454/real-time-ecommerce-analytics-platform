# `docs/design/02_Event_Model.md`

# Event Model

> This document defines the event-driven data model used throughout the Real-Time E-Commerce Analytics Platform. It specifies the structure, lifecycle, naming conventions, validation rules, and Kafka topic mapping for every business event.

---

# Table of Contents

1. Introduction
2. Event-Driven Design
3. Event Lifecycle
4. Event Categories
5. Common Event Structure
6. Event Metadata
7. Event Types
8. Kafka Topic Mapping
9. Event Schemas
10. Event Versioning
11. Event Validation
12. Partition Key Strategy
13. Dead Letter Queue (DLQ)
14. Design Decisions

---

# 1. Introduction

The platform follows an **Event-Driven Architecture (EDA)** where every business action is represented as an immutable event.

Events are the primary method of communication between services. Producers publish events to Apache Kafka, and downstream consumers process them independently.

This design enables loose coupling, scalability, and replayability.

---

# 2. Event-Driven Design

Every user interaction creates exactly one business event.

Example:

```text
Customer Opens Website
        │
        ▼
Search Product
        │
        ▼
Search Event
        │
        ▼
Kafka Topic
        │
        ▼
Apache Flink
```

Events are append-only and are never modified after publication.

---

# 3. Event Lifecycle

Every event follows the same lifecycle.

```text
Generated
    │
    ▼
Validated
    │
    ▼
Published to Kafka
    │
    ▼
Consumed by Flink
    │
    ▼
Enriched
    │
    ▼
Stored in Medallion Distributed Data Lake
    │
    ▼
Queried by PostgreSQL & Spark
    │
    ▼
Analyzed by Spark
```

---

# 4. Event Categories

The platform generates three categories of events.

## Customer Events

Represent customer interactions.

Examples:

* Search
* Product View
* Add to Cart
* Remove from Cart
* Checkout

---

## Transaction Events

Represent financial operations.

Examples:

* Payment Started
* Payment Completed
* Payment Failed
* Refund

---

## System Events

Represent internal platform activities.

Examples:

* Inventory Updated
* Fraud Alert
* System Metrics

---

# 5. Common Event Structure

Every event contains a common header.

```json
{
  "event_id": "550e8400-e29b-41d4-a716-446655440000",
  "event_version": "1.0",
  "event_type": "product_view",
  "event_timestamp": "2026-08-01T14:22:10Z",
  "producer": "event-generator",
  "source": "web"
}
```

This metadata is shared across all event types.

---

# 6. Event Metadata

| Field           | Type      | Description             |
| --------------- | --------- | ----------------------- |
| event_id        | UUID      | Unique event identifier |
| event_version   | String    | Schema version          |
| event_type      | String    | Business event name     |
| event_timestamp | Timestamp | Event creation time     |
| producer        | String    | Producing service       |
| source          | String    | Web, Mobile, API        |

---

# 7. Event Types

| Event             | Kafka Topic         |
| ----------------- | ------------------- |
| search            | search-events       |
| product_view      | product-view-events |
| add_to_cart       | cart-events         |
| remove_from_cart  | cart-events         |
| checkout_started  | checkout-events     |
| payment_completed | payment-events      |
| payment_failed    | payment-events      |
| order_completed   | order-events        |
| review_submitted  | review-events       |
| inventory_updated | inventory-events    |
| fraud_detected    | fraud-alerts        |

---

# 8. Kafka Topic Mapping

Each event type is published to a dedicated Kafka topic.

| Topic               | Producer          | Consumer        |
| ------------------- | ----------------- | --------------- |
| search-events       | Event Generator   | Flink           |
| product-view-events | Event Generator   | Flink           |
| cart-events         | Event Generator   | Flink           |
| checkout-events     | Event Generator   | Flink           |
| payment-events      | Payment Simulator | Flink           |
| order-events        | Payment Service   | Flink           |
| review-events       | Review Service    | Flink           |
| inventory-events    | Inventory Service | Flink           |
| fraud-alerts        | Flink             | Dashboard       |
| dead-letter-events  | Flink             | Operations Team |

---

# 9. Event Schemas

## Product View Event

```json
{
  "event_id": "...",
  "event_type": "product_view",
  "customer_id": 1024,
  "session_id": "S-78291",
  "product_id": 540,
  "category": "Electronics",
  "device": "mobile",
  "country": "Egypt",
  "timestamp": "2026-08-01T14:25:18Z"
}
```

---

## Add to Cart Event

```json
{
  "event_id": "...",
  "event_type": "add_to_cart",
  "customer_id": 1024,
  "product_id": 540,
  "quantity": 1,
  "cart_value": 1250.00,
  "timestamp": "2026-08-01T14:27:10Z"
}
```

---

## Payment Event

```json
{
  "event_id": "...",
  "event_type": "payment_completed",
  "customer_id": 1024,
  "order_id": 90018,
  "payment_method": "Credit Card",
  "payment_status": "SUCCESS",
  "amount": 1250.00,
  "currency": "EGP",
  "timestamp": "2026-08-01T14:30:42Z"
}
```

---

## Inventory Event

```json
{
  "event_id": "...",
  "event_type": "inventory_updated",
  "product_id": 540,
  "warehouse_id": 4,
  "old_quantity": 120,
  "new_quantity": 119,
  "timestamp": "2026-08-01T14:31:02Z"
}
```

---

# 10. Event Versioning

To support schema evolution, every event contains an `event_version`.

Example:

| Version | Description              |
| ------- | ------------------------ |
| 1.0     | Initial release          |
| 1.1     | Added device information |
| 2.0     | Added campaign metadata  |

Consumers should remain backward compatible whenever possible.

---

# 11. Event Validation

Every event is validated before processing.

Validation rules include:

* Required fields must exist.
* UUID format must be valid.
* Timestamp must use ISO 8601.
* Quantity must be greater than zero.
* Amount must be non-negative.
* Customer ID must exist in Master Data.
* Product ID must exist in Product Catalog.

Events that fail validation are redirected to the Dead Letter Queue (DLQ).

---

# 12. Partition Key Strategy

Choosing the correct Kafka partition key is essential for scalability and ordering.

| Topic               | Partition Key | Reason                                |
| ------------------- | ------------- | ------------------------------------- |
| search-events       | customer_id   | Preserve search order per customer    |
| product-view-events | customer_id   | Keep browsing sequence together       |
| cart-events         | customer_id   | Ensure cart operations remain ordered |
| checkout-events     | order_id      | Preserve checkout flow                |
| payment-events      | order_id      | Keep payment lifecycle in order       |
| order-events        | order_id      | Maintain order event sequence         |
| review-events       | product_id    | Distribute reviews evenly             |
| inventory-events    | product_id    | Maintain inventory consistency        |

---

# 13. Dead Letter Queue (DLQ)

Invalid or malformed events are written to a dedicated Kafka topic:

```text
dead-letter-events
```

Typical reasons include:

* Missing required fields
* Invalid JSON
* Unknown event type
* Schema mismatch
* Invalid timestamp
* Business rule violations

This allows the main processing pipeline to continue without interruption while preserving failed events for investigation.

---

# 14. Design Decisions

The following decisions guide the event model:

* Events are immutable.
* Every event includes standard metadata.
* Business data is separated from metadata.
* Dedicated Kafka topics are used for major event categories.
* Partition keys are selected to preserve ordering where required.
* Versioning supports future schema evolution.
* Invalid events are isolated using a DLQ rather than discarded.

---

# Next Document

Continue with:

```text
docs/design/03_Data_Model.md
```

The next document defines the logical data model, entity relationships, master data structure, and how business entities are connected across the platform.
