# Event Generator Service

> This document describes the architecture, implementation, and configuration of the Event Generator Service responsible for simulating realistic customer behavior in the Real-Time E-Commerce Analytics Platform.

---

# Table of Contents

1. Overview
2. Why an Event Generator?
3. Design Goals
4. Architecture
5. Components
6. Customer Journey Simulation
7. User Behavior Model
8. Event Types
9. Session Management
10. Scenario Engine
11. Event Scheduling
12. Randomization Strategy
13. Configuration
14. Performance
15. Scaling
16. Future Improvements

---

# 1. Overview

The Event Generator Service is responsible for producing realistic customer activity.

Instead of replaying historical datasets repeatedly, the generator continuously creates new customer sessions that resemble real interactions occurring in a production e-commerce platform.

The generated events are published directly to Apache Kafka and become the primary input for the streaming pipeline.

---

# 2. Why an Event Generator?

Using a static CSV dataset does not represent how modern e-commerce systems operate.

In production environments:

- Customers browse products continuously.
- Users create shopping carts.
- Orders are placed every second.
- Payments occur in parallel.
- Reviews arrive hours or days later.

To simulate this behavior, a dedicated Event Generator continuously produces realistic events.

---

# 3. Design Goals

The generator is designed to:

- Simulate realistic customer behavior.
- Produce configurable event rates.
- Support thousands of concurrent sessions.
- Generate deterministic event sequences.
- Mimic production traffic patterns.
- Support stress testing.
- Be horizontally scalable.

---

# 4. High-Level Architecture

```text
                    Scenario Manager
                           │
                           ▼
                  Session Controller
                           │
        ┌──────────────────┼──────────────────┐
        ▼                  ▼                  ▼
 Customer Generator   Product Selector   Payment Simulator
        │                  │                  │
        └──────────────────┼──────────────────┘
                           ▼
                    Event Builder
                           ▼
                     Kafka Producer
                           ▼
                      Apache Kafka
```

---

# 5. Internal Components

## Scenario Manager

Controls the simulation mode.

Examples:

- Normal Day
- Weekend
- Black Friday
- Flash Sale
- Ramadan Sale
- Christmas Sale

Each scenario changes:

- User arrival rate
- Purchase probability
- Search frequency
- Average basket size
- Payment volume

---

## Customer Generator

Creates virtual customers.

Each customer has attributes such as:

- Customer ID
- Country
- City
- Age Group
- Loyalty Level
- Preferred Categories
- Device Type

Customers may come from the master dataset or be generated dynamically using Python's Faker library.

---

## Product Selector

Chooses products based on weighted probabilities.

Example:

Electronics may be viewed more frequently than Books.

Popular products receive more traffic than low-demand products.

This creates realistic browsing behavior.

---

## Session Controller

Responsible for creating user sessions.

Each session has:

- Session ID
- Start Time
- End Time
- Device
- Traffic Source
- Geographic Location

The Session Controller determines which events occur during a session.

---

# 6. Customer Journey Simulation

A customer session follows a realistic sequence.

Example:

```text
Open Website
      │
Search Product
      │
View Product
      │
View Product
      │
Add to Cart
      │
Continue Shopping
      │
Checkout
      │
Payment
      │
Order Completed
      │
Leave Review
```

Not every session ends with a purchase.

Possible outcomes include:

- Session abandoned
- Cart abandoned
- Payment failed
- Successful order
- Wishlist only

---

# 7. Event Types

The service generates multiple event types.

| Event | Description |
|--------|-------------|
| search | Customer searches for a product |
| product_view | Product page viewed |
| add_to_cart | Item added to cart |
| remove_from_cart | Item removed |
| wishlist | Product added to wishlist |
| checkout | Checkout initiated |
| payment | Payment processed |
| order_completed | Order successfully placed |
| review | Customer review submitted |
| inventory_update | Inventory changed |

---

# 8. Event Schema

Every event contains common metadata.

```json
{
  "event_id": "...",
  "event_type": "...",
  "timestamp": "...",
  "session_id": "...",
  "customer_id": "...",
  "product_id": "...",
  "country": "...",
  "device": "...",
  "traffic_source": "...",
  "scenario": "black_friday"
}
```

Each event type may include additional fields.

---

# 9. Scenario Engine

The Scenario Engine modifies user behavior.

### Example Scenarios

| Scenario | Behavior |
|----------|----------|
| Normal Day | Balanced traffic |
| Black Friday | Massive traffic spike |
| Flash Sale | Short-term traffic burst |
| Ramadan | Increased evening activity |
| Christmas | Higher purchase rate |
| New Product Launch | Heavy product views |

---

# 10. Randomization Strategy

Randomness is controlled rather than purely random.

Examples:

- Electronics receive more views.
- Returning customers purchase more often.
- Loyal customers abandon carts less frequently.
- Premium products have lower purchase frequency.

Weighted probabilities produce realistic datasets.

---

# 11. Event Scheduling

The generator controls event timing.

Example timeline:

```text
12:00:00 Search

12:00:03 View

12:00:08 View

12:00:18 Add To Cart

12:00:42 Checkout

12:00:50 Payment
```

Delays between actions are configurable.

---

# 12. Configuration

The service can be configured using a YAML file.

Example parameters:

- Events Per Second
- Concurrent Sessions
- Number of Customers
- Number of Products
- Scenario
- Payment Success Rate
- Cart Abandonment Rate
- Review Probability

---

# 13. Performance Targets

Target throughput:

| Stage | Events/sec |
|--------|-----------:|
| Development | 100 |
| Demo | 500 |
| Benchmark | 2,000 |
| Stress Test | 5,000+ |

---

# 14. Scaling Strategy

The Event Generator can be horizontally scaled by running multiple generator instances.

Each instance is responsible for a subset of customer sessions and publishes events independently to Apache Kafka.

---

# 15. Future Improvements

Potential enhancements include:

- REST API for runtime control
- Web dashboard for simulation management
- AI-driven customer behavior
- Seasonal demand forecasting
- A/B testing simulation
- Multi-region traffic generation
- Mobile vs Desktop traffic modeling
- Bot traffic simulation
- Marketing campaign simulation

---

# Next Document

Continue with:

```
docs/05_Apache_Kafka.md
```

The next document explains the complete Kafka architecture, topic design, partitioning strategy, producer/consumer workflow, message schemas, and reliability mechanisms used throughout the platform.