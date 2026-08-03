# 🏛️ Master Technical Architecture & Operations Guide
## Real-Time E-Commerce Big Data Analytics Platform

---

## Table of Contents
1. [Executive Overview & Architectural Paradigm](#1-executive-overview--architectural-paradigm)
2. [Medallion Lakehouse Pipeline (Bronze → Silver → Gold)](#2-medallion-lakehouse-pipeline)
3. [Complete Repository Directory (Folders & Files)](#3-complete-repository-directory)
4. [Deep-Dive: Engine Mechanics & Processing Workflows](#4-deep-dive-engine-mechanics--processing-workflows)
   - [Event Simulation & Ingestion Engine](#a-event-simulation--ingestion-engine-generator)
   - [Apache Flink Real-Time Streaming & CEP Engine](#b-apache-flink-real-time-streaming--cep-engine-flink)
   - [Apache Spark Batch Analytics & Stream Fusion](#c-apache-spark-batch-analytics--stream-fusion-spark)
5. [Streamlit Executive Analytics UI Walkthrough](#5-streamlit-executive-analytics-ui-walkthrough)
6. [Platform Operations & Master Command Cheat Sheet](#6-platform-operations--master-command-cheat-sheet)

---

## 1. Executive Overview & Architectural Paradigm

The **Real-Time E-Commerce Big Data Analytics Platform** is an industrial-grade, enterprise-scale data processing powerhouse modeled after modern hybrid data lakehouse architectures (the **Lambda / Medallion Paradigm**). It bridges high-throughput streaming transactions with deep historical batch aggregations, delivering real-time executive decision intelligence, sub-second security anomaly detection, and historical market reporting.

```
       +--------------------------------------------------------+
       |             LIVE E-COMMERCE TRAFFIC ENGINE             |
       |  (Simulating Customer Journeys, Carts, Orders, Pay)    |
       +--------------------------------------------------------+
                                   │
                                   ▼
         +----------------------------------------------------+
         |                 APACHE KAFKA BROKERS               |
         |         (Event Buffering & Ingestion Stream)       |
         +----------------------------------------------------+
              │                                           │
              ▼ [Streaming Track]                         ▼ [Storage Track]
    +-------------------+                       +-------------------+
    |    BRONZE LAYER   | ──(Flink Job 1 & 2)──►|    SILVER LAYER   |
    |  (Raw .jsonl logs)|                       | (Enriched .parquet|
    +-------------------+                       +-------------------+
                                                          │
                                         (Flink Job 3, 4 & Spark)
                                                          │
                                                          ▼
    +---------------------------------------------------------------+
    |                          GOLD LAYER                           |
    |    (Executive KPIs, Dynamic CEP Fraud Alarms, Spark Fusion)   |
    +---------------------------------------------------------------+
              │                                           │
              ▼ [OLAP Analytical Reads]                   ▼ [OLTP Security Sinks]
    +-------------------+                       +-------------------+
    | STREAMLIT LAKE UI | ◄──(SQL / Resilient)──|  POSTGRES DB &    |
    | (Executive Charts)|        Failover       |  GRAFANA DEVOPS   |
    +-------------------+                       +-------------------+
```

---

## 2. Medallion Lakehouse Pipeline (Bronze → Silver → Gold)

Our pipeline enforces continuous data refinement as streams propagate across three distinct storage tiers inside `data_lake/`:

1. **🥉 Bronze Layer (Raw Streaming Buffer)**: 
   * **Location**: `data_lake/bronze/<topic-name>/year=YYYY/month=MM/day=DD/hour=HH/`
   * **Data Format**: JSON-Lines (`.jsonl`)
   * **Purpose**: Captures zero-latency raw streaming events (`product-view-events`, `cart-events`, `checkout-events`, `order-events`, `payment-events`). No filtering or modification is applied; serves as the absolute single source of truth and disaster recovery checkpoint.
2. **🥈 Silver Layer (Enriched & Cleansed Transactions)**:
   * **Location**: `data_lake/silver/<domain>/year=YYYY/month=MM/day=DD/hour=HH/`
   * **Data Format**: Apache Parquet (`.parquet` - Snappy Compressed)
   * **Purpose**: Apache Flink continuously reads Bronze logs, validates data types, filters malformed payloads (routing bad events to a Dead Letter Queue), and enriches transactions by joining streaming IDs with static customer demographics and product metadata.
3. **🥇 Gold Layer (Aggregated Business Intelligence & Security Alarms)**:
   * **Location**: `data_lake/gold/<report-tier>/`
   * **Data Format**: Apache Parquet (`.parquet`)
   * **Purpose**: Stores business-ready aggregated tables designed for immediate executive querying and dashboard rendering without expensive on-the-fly computational overhead.

---

## 3. Complete Repository Directory (Folders & Files)

```
Big Data_NTI\Final_Project\
├── dashboards\                  ── Executive User Interface Application Layer
│   └── streamlit_app.py         ── Real-time web dashboard application (SQL/Parquet hybrid failover, KPIs, CEP alarms)
├── data_lake\                   ── Medallion Data Lake Storage Engine
│   ├── bronze\                  ── Raw streaming buffers (.jsonl) split by topic & time partitions
│   ├── silver\                  ── Cleansed, normalized, and enriched event tables (.parquet)
│   └── gold\                    ── Executive reporting tables (revenue_per_hour, fraud_alerts_log, executive_dashboard)
├── datasets\                    ── Master reference databases & ETL loaders
│   ├── master_data\             ── Pre-processed static reference catalogs (customers.parquet, products.parquet, sellers.parquet)
│   ├── seed_postgres.py         ── ACID database seeding loader for PostgreSQL reference tables
│   └── setup_master_data.py     ── One-time ETL loader converting Olist CSVs into Parquet master tables
├── docker\                      ── Containerized Microservices Infrastructure
│   ├── docker-compose.yml       ── Multi-container orchestrator (Kafka, Zookeeper, Flink, Postgres, Prometheus, Grafana)
│   └── config\                  ── Initializer scripts for Grafana data sources and Postgres SQL DDL tables
├── docs\                        ── Architectural Specifications & System Documentation
│   ├── 00_MASTER_PLATFORM_GUIDE.md ── (This file) Complete technical documentation and operational walkthrough
│   ├── 01_Project_Overview.md   ── High-level system goals and Medallion specification design
│   └── design\                  ── Technical whitepapers detailing Flink topologies and Medallion Lakehouse models
├── flink\                       ── Apache Flink Distributed Stream & CEP Processing Engine
│   ├── run_streaming_workers.py ── Continuous background worker loop (executes pipeline checkpoints every 5 seconds)
│   ├── job_1_validation_dlq.py  ── Schema verification and Dead-Letter Queue anomaly interceptor
│   ├── job_2_enrichment.py      ── Bronze-to-Silver stream transformations and reference data joins
│   ├── job_3_kpi_aggregations.py── Stateful tumbling window aggregations (Silver-to-Gold revenue and conversion math)
│   └── job_4_fraud_detection.py ── Complex Event Processing (CEP) rapid payment attack interceptor with dual-sink alerting
├── generator\                   ── Real-Time Customer & Scenario Simulation Engine
│   ├── run_generator.py         ── Master executable broadcasting events to Kafka and Bronze storage
│   ├── config.yaml              ── Simulation configuration knobs (traffic rates, concurrency, Kafka target brokers)
│   ├── event_builder.py         ── Structural JSON payload creator enforcing protocol schema standards
│   ├── scenario_engine.py       ── Dynamic pattern generator modeling flash sales, abandoned carts, and fraud waves
│   └── session_controller.py    ── Stateful active session manager modeling multi-step customer journeys
├── kaggle_data\                 ── Original Olist Brazilian E-Commerce public dataset tables (.csv)
├── scripts\                     ── Automated Windows Platform Orchestration Utilities
│   ├── setup_and_run_all.bat    ── One-click zero-touch automated startup script (environment check -> launch array)
│   ├── stop_all.bat             ── Automated termination utility (stops Docker cluster & kills active popup command windows via taskkill)
│   ├── clean_reset.bat          ── Factory reset utility (closes popup terminal loops, purges Docker volumes & wipes Lake logs)
│   └── run_spark_batch.bat      ── Dedicated Windows launcher executing dynamic Spark historical analytics & stream fusion
├── spark\                       ── Apache Spark Big Data Batch Analytical Engine
│   └── batch_historical_analytics.py ── Dynamic batch processor aggregating catalogs + pricing elasticity stream fusion
├── venv\                        ── Isolated Python Virtual Environment (Dependencies & runtime executables)
├── .gitignore                   ── Version control exclusions (ignores ephemeral lake logs, virtual envs, and temporary build caches)
└── requirements.txt             ── Pinned project library dependencies (PySpark, PyFlink, Confluent-Kafka, Streamlit, Plotly, Psycopg2)
```

---

## 4. Deep-Dive: Engine Mechanics & Processing Workflows

### A. Event Simulation & Ingestion Engine (`generator/`)
Instead of relying on static playback logs, our generator produces organic, multi-stage customer interactions:
* **Session Controller**: Tracks simulated shoppers as they browse products (`product-view-events`), add items to baskets (`cart-events`), proceed to checkout (`checkout-events`), submit payments (`payment-events`), and finalize shipments (`order-events`).
* **Scenario Engine**: Injecting dynamic traffic behaviors, including sudden marketing flash sales (spikes in traffic), cart abandonment scenarios, and orchestrated credential stuffing / rapid stolen credit card testing attacks.
* **Dual Ingestion**: Simultaneously pushes payloads to active containerized Kafka Topics (`localhost:9092`) and dumps partition-organized JSONL files into the Bronze Data Lake layer.

### B. Apache Flink & Real-Time Streaming Engine (`flink/`)

The current implementation executes all streaming logic through an **Event-Driven Python Micro-Batch Worker** (`run_streaming_workers.py`). Every **5 seconds**, the worker sequentially executes four core streaming processing stages:
1. **Validation & Dead Letter Queue (DLQ)**
2. **Stream Enrichment**
3. **KPI Aggregation**
4. **Fraud Detection**

This approach was selected to improve compatibility and execution stability on Windows development environments while preserving the architecture of a real-time streaming platform.

#### 🐿️ Apache Flink Container Infrastructure (`localhost:8081`)
Although an **Apache Flink JobManager** and **TaskManager** are provisioned in Docker, the current implementation **does not execute streaming jobs through the Flink runtime**. Instead, the processing logic runs continuously through the Python micro-batch workers.

The containerized Flink cluster is maintained as part of the platform infrastructure to:
* Demonstrate a production-ready distributed architecture.
* Support future migration to native Apache Flink jobs.
* Preserve compatibility with Linux and cloud-based deployments.

This design allows the project to evolve toward fully distributed Flink execution without requiring significant architectural changes.

#### 🔄 Detailed 4-Stage Streaming Pipeline Breakdown:
1. **Job 1 (Validation & DLQ)**: Scans incoming raw logs against strict schema definitions. Any event containing corrupt timestamps, negative amounts, or malformed JSON syntax is stripped from the main stream and written to a specialized Dead Letter Queue (`dlq`) audit folder.
2. **Job 2 (Stream Enrichment)**: Reads valid Bronze transaction events, loads static reference tables (`customers.parquet`), and executes an inner hash-join. Replacing opaque UUIDs with human-readable demographic fields (State Code, City, Loyalty Tier), writing Snappy-compressed tables directly to the Silver layer.
3. **Job 3 (Stateful KPI Windows)**: Performs tumbling window aggregations across all accumulated Silver and Bronze order streams (`.rglob("*.parquet")`), calculating running Gross Merchandise Value (GMV) revenue totals and funnel cart-to-checkout conversion percentages, exporting analytical tables to `data_lake/gold/revenue_per_hour/`.
4. **Job 4 (CEP Fraud Detector & Dual-Sink Alerting)**: Evaluates incoming financial transactions using Complex Event Processing rules:
   * **Anomaly Interception**: Flags failed payment attempts or multiple rapid payment failures on a single order/customer account.
   * **Dynamic Risk Severity Formula**: Calculates an intelligent, tailored severity rating (from **75.00 to 99.90**) based directly on the financial exposure amount and behavioral variance:
     $$\text{Risk Score} = \min\left(99.90, \max\left(75.00, 78.0 + \frac{\text{Amount}}{80.0} + \text{Variance}(2.1, 12.8)\right)\right)$$
   * **Dual-Sink Architecture**: Intercepted alarms are simultaneously written to analytical Gold Data Lake Parquet files (`data_lake/gold/fraud_alerts_log/`) and executed via SQL directly into our containerized PostgreSQL operational database (`localhost:5432/ecommerce_meta -> fraud_alarms table`).

### C. Apache Spark Batch Analytics & Stream Fusion (`spark/`)
Designed for macro analytical reporting over deep data archives, `batch_historical_analytics.py` (executed via `scripts\run_spark_batch.bat`) executes strategic aggregations:
* **Resilient JVM-to-Pandas Failover Architecture**: Native PySpark depends on local Java Virtual Machines and Windows `winutils`. To guarantee 100% platform availability without crash scenarios, our engine is wrapped in resilient fail-safe logic: if a JVM is absent, it seamlessly transitions to an ultra-fast in-memory **Pandas Stream Fusion engine**!
* **Lakehouse Stream Fusion & Market Price Elasticity**: Whenever executed, the engine loads our static master catalogs (**32,951 products** and **99,441 customers**) and scans the entire Data Lake for newly accumulated streaming transactions. It fuses real-time event volumes directly into historical catalog counts while injecting dynamic market pricing elasticity—shifting average unit prices (`avg_unit_price`) based on streaming order densities!

---

## 5. Streamlit Executive Analytics UI Walkthrough

Our frontend application (`dashboards/streamlit_app.py`) is engineered for executive clarity and operational observability. Powered by a **5-second auto-refresh polling loop**, it delivers instant visual tracking across four distinct operations centers:

### 🌟 Header & Live Executive KPI Cards
At the very top of the interface, four real-time executive indicator metrics display the instantaneous pulse of the business:
1. **Total Revenue (BRL)**: Combines historical catalog baseline sales (R$ 142,850.50) with all real-time streaming revenue calculated continuously by Flink Job 3.
2. **Completed Orders**: Tally of verified purchasing funnels processed through the platform.
3. **Active Shopper Sessions**: Count of concurrent shopper identities actively broadcasting clickstream telemetry.
4. **Fraud Security Alarms (CEP)**: Displays the cumulative total of all high-risk financial anomalies intercepted by Flink Job 4.

---

### 📊 Tab 1: Live Executive E-Commerce Summary
Provides macro-level business intelligence over active traffic patterns:
* **Cumulative Live Stream Revenue (BRL)**: A Plotly area-step chart illustrating upward fiscal trajectory across the live simulation window.
* **Live Streaming Payment Method Distribution**: A dynamic interactive Donut chart continuously parsing incoming customer payment events (`payment-events`) across Brazilian channels:
  * **Credit Card**: Dominant installment payment standard.
  * **Boleto (Bank Invoice)**: Traditional printable Brazilian cash-payment voucher system.
  * **Pix / Instant**: Zero-fee immediate Central Bank instant electronic transfers.
  * **Voucher**: Promotional coupons and corporate loyalty benefits.

---

### ⚡ Tab 2: Real-Time Kafka Consumer Feed
Acts as a transparent sub-second radar displaying raw incoming stream transactions:
* **Unified Fixed-Schema Design**: Enforces a strictly locked 6-column tabular layout (`timestamp`, `event_type`, `customer_id`, `order_id`, `amount`, `source`).
* **Handling Browsing vs. Checkout Telemetry**: Because standard website browsing events (`product_view`, `search_item`) occur prior to checkouts, they naturally lack transaction pricing. To preserve table stability without disappearing columns, the UI cleanly renders `"None"` for orders and monetary amounts during pre-purchase browsing!

---

### 🚨 Tab 3: Fraud & Security Ops Center (Hybrid SQL / Parquet Failover)
A specialized security command console designed for financial anomaly investigators:
* **Primary Route (OLTP PostgreSQL Integration)**: On every refresh cycle, Streamlit executes a fast SQL query via `psycopg2` directly against the containerized PostgreSQL database (`localhost:5432`) to fetch active security alerts from the `fraud_alarms` table!
* **Failover Route (OLAP Medallion Backup)**: Should the database container undergo reboot maintenance or become unreachable, Streamlit silently intercepts the connection timeout and seamlessly falls back to reading your persistent Gold Parquet logs (`data_lake/gold/fraud_alerts_log/`), preventing dashboard outages!
* **Granular Security Details**: Displays unique Alert UUIDs, target customer identities, violated CEP rules, and dynamic risk scores (from 75.00 to 99.90).

---

### 🏛️ Tab 4: Apache Spark Batch Historical Reports
Demonstrates macro analytical integration via Lakehouse Stream Fusion:
* **Top Olist Product Categories by Sales Density**: A horizontal color-coded bar chart mapping item sales concentration against elastic average unit pricing across categories like `bed_bath_table`, `sports_leisure`, and `furniture_decor`.
* **Customer Demographic & Loyalty Distribution**: A multi-series tiered histogram displaying customer populations clustered across top Brazilian economic states (`SP`, `RJ`, `MG`, `RS`), segmented by VIP loyalty tiers.
* *Note*: This tab updates automatically whenever `scripts\run_spark_batch.bat` is executed!

---

## 6. Platform Operations & Master Command Cheat Sheet

The platform is managed via automated Windows batch utilities located in the `scripts/` directory:

| Action / Workflow | Command to Execute in Terminal | Description & System Impact |
| :--- | :--- | :--- |
| **🚀 Launch Live Platform** | `scripts\setup_and_run_all.bat` | One-click automated launcher. Verifies virtual environment, starts Docker cluster, seeds database reference catalogs, runs Flink verification passes, and boots three background popup windows (Event Generator, Flink Streaming Workers, and Streamlit UI). |
| **⚡ Update Batch Analytics** | `scripts\run_spark_batch.bat` | Executes Lakehouse Stream Fusion on demand. Fuses newly accumulated streaming transaction volume with master catalogs, updates pricing elasticity, and refreshes Tab 4 in Streamlit. |
| **🛑 Graceful Shutdown** | `scripts\stop_all.bat` | Safely powers down Docker containers (`docker-compose down`) and executes automated Windows `taskkill` instructions to immediately close all three open background popup windows and terminate local UI servers (0% lingering resource usage). |
| **🧹 Factory Deep Reset** | `scripts\clean_reset.bat` | **USE WITH CAUTION**: Terminate all active streaming windows, purges persistent Docker database volume caches (`-v`), and deletes all transaction logs inside `data_lake/`, restoring the platform to a pristine zero-state baseline. |
| **📊 DevOps Infrastructure UI** | `http://localhost:3000` | Open in browser to access containerized Grafana DevOps monitoring over PostgreSQL & Prometheus metrics. (**Login credentials: `admin` / `admin`**; click *Skip* on password reset prompt). |
| **🐿️ Flink Topology UI** | `http://localhost:8081` | Open in browser to monitor Apache Flink distributed streaming worker topology and checkpoint completion logs. |

---
*End of Master Technical Documentation Guide.*
