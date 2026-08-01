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
                                   │
                                   ▼
    +---------------------------------------------------------------+
    |            STREAMLIT LIVE EXECUTIVE DASHBOARD (UI)            |
    |  (Auto-Refreshing KPIs, Streaming Feeds, Security Ops Center) |
    +---------------------------------------------------------------+
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
│   └── streamlit_app.py         ── Real-time web dashboard application (KPIs, Plotly graphs, CEP alarms)
├── data_lake\                   ── Medallion Data Lake Storage Engine
│   ├── bronze\                  ── Raw streaming buffers (.jsonl) split by topic & time partitions
│   ├── silver\                  ── Cleansed, normalized, and enriched event tables (.parquet)
│   └── gold\                    ── Executive reporting tables (revenue_per_hour, fraud_alerts_log, executive_dashboard)
├── datasets\                    ── Master reference databases & raw ingestion seed datasets
│   ├── master_data\             ── Pre-processed static reference catalogs (customers.parquet, products.parquet, sellers.parquet)
│   └── raw\                     ── Original Olist Brazilian E-Commerce public dataset tables (.csv)
├── docker\                      ── Containerized Microservices Infrastructure
│   ├── docker-compose.yml       ── Multi-container orchestrator (Kafka, Zookeeper, Flink, Postgres, Prometheus, Grafana)
│   └── init_db.sql              ── Automated PostgreSQL DDL schema installer for metadata and fraud alarm syncing
├── docs\                        ── Architectural Specifications & System Documentation
│   ├── 00_MASTER_PLATFORM_GUIDE.md ── (This file) Complete technical documentation and operational walkthrough
│   ├── 01_Project_Overview.md   ── High-level system goals and Medallion specification design
│   └── design\                  ── Technical whitepapers detailing Flink topologies and Spark schema models
├── flink\                       ── Apache Flink Distributed Stream & CEP Processing Engine
│   ├── run_streaming_workers.py ── Continuous background worker loop (executes pipeline checkpoints every 5 seconds)
│   ├── job_1_validation_dlq.py  ── Schema verification and Dead-Letter Queue anomaly interceptor
│   ├── job_2_enrichment.py      ── Bronze-to-Silver stream transformations and reference data joins
│   ├── job_3_kpi_aggregations.py── Stateful tumbling window aggregations (Silver-to-Gold revenue and conversion math)
│   └── job_4_fraud_detection.py ── Complex Event Processing (CEP) rapid payment attack interceptor with dynamic risk scoring
├── generator\                   ── Real-Time Customer & Scenario Simulation Engine
│   ├── run_generator.py         ── Master executable broadcasting events to Kafka and Bronze storage
│   ├── config.yaml              ── Simulation configuration knobs (traffic rates, concurrency, Kafka target brokers)
│   ├── event_builder.py         ── Structural JSON payload creator enforcing protocol schema standards
│   ├── scenario_engine.py       ── Dynamic pattern generator modeling flash sales, abandoned carts, and fraud waves
│   └── session_controller.py    ── Stateful active session manager modeling multi-step customer journeys
├── scripts\                     ── Automated Windows Platform Orchestration Utilities
│   ├── setup_and_run_all.bat    ── One-click zero-touch automated startup script (environment check -> launch array)
│   ├── stop_all.bat             ── Graceful cluster termination and container shutdown utility
│   ├── clean_reset.bat          ── Factory reset utility (purges Docker volumes and Data Lake streams for fresh runs)
│   └── setup_master_data.py     ── One-time ETL loader converting Olist CSVs into Parquet master tables
├── spark\                       ── Apache Spark Big Data Batch Analytical Engine
│   └── batch_historical_analytics.py ── Heavy batch processor aggregating static master tables + Lakehouse Stream Fusion
├── venv\                        ── Isolated Python Virtual Environment (Dependencies & runtime executables)
├── .gitignore                   ── Version control exclusions (ignores ephemeral lake logs, virtual envs, and temporary build caches)
└── requirements.txt             ── Pinned project library dependencies (PySpark, PyFlink, Confluent-Kafka, Streamlit, Plotly)
```

---

## 4. Deep-Dive: Engine Mechanics & Processing Workflows

### A. Event Simulation & Ingestion Engine (`generator/`)
Instead of relying on static playback logs, our generator produces organic, multi-stage customer interactions:
* **Session Controller**: Tracks simulated shoppers as they browse products (`product-view-events`), add items to baskets (`cart-events`), proceed to checkout (`checkout-events`), submit payments (`payment-events`), and finalize shipments (`order-events`).
* **Scenario Engine**: Injecting dynamic traffic behaviors, including sudden marketing flash sales (spikes in traffic), cart abandonment scenarios, and orchestrated credential stuffing / rapid stolen credit card testing attacks.
* **Dual Ingestion**: Simultaneously pushes payloads to active containerized Kafka Topics (`localhost:9092`) and dumps partition-organized JSONL files into the Bronze Data Lake layer.

### B. Apache Flink Real-Time Streaming & CEP Engine (`flink/`)
Orchestrated by `run_streaming_workers.py`, our Flink processing worker loop cycles continuously **every 5 seconds**, executing a four-stage pipeline:
1. **Job 1 (Validation & DLQ)**: Scans incoming raw logs against strict schema definitions. Any event containing corrupt timestamps, negative amounts, or malformed JSON syntax is stripped from the main stream and written to a specialized Dead Letter Queue (`dlq`) audit folder.
2. **Job 2 (Stream Enrichment)**: Reads valid Bronze transaction events, loads static reference tables (`customers.parquet`), and executes an inner hash-join. Replacing opaque UUIDs with human-readable demographic fields (State Code, City, Loyalty Tier), writing Snappy-compressed tables directly to the Silver layer.
3. **Job 3 (Stateful KPI Windows)**: Performs tumbling window aggregations across all accumulated Silver and Bronze order streams (`.rglob("*.parquet")`), calculating running Gross Merchandise Value (GMV) revenue totals and funnel cart-to-checkout conversion percentages, exporting analytical tables to `data_lake/gold/revenue_per_hour/`.
4. **Job 4 (CEP Fraud Detector & Dynamic Risk Scoring)**: Evaluates incoming financial transactions using Complex Event Processing rules:
   * **Anomaly Interception**: Flags failed payment attempts or multiple rapid payment failures on a single order/customer account.
   * **Dynamic Risk Severity Formula**: Calculates an intelligent, tailored severity rating (from **75.00 to 99.90**) based directly on the financial exposure amount and behavioral variance:
     $$\text{Risk Score} = \min\left(99.90, \max\left(75.00, 78.0 + \frac{\text{Amount}}{80.0} + \text{Variance}(2.1, 12.8)\right)\right)$$
   * **Dual Sync Preservation**: Intercepted alarms are simultaneously recorded into Gold Data Lake Parquet files (`data_lake/gold/fraud_alerts_log/`) and pushed directly via JDBC into our persistent relational PostgreSQL database table (`postgres-meta -> fraud_alarms`).

### C. Apache Spark Batch Analytics & Stream Fusion (`spark/`)
Designed for historical processing over deep data archives, `batch_historical_analytics.py` executes strategic aggregations:
* **Lakehouse Stream Fusion**: Whenever executed, Spark loads our static master catalogs (**32,951 products** and **99,441 customers**) and systematically scans the entire Data Lake for all newly accumulated real-time streaming transaction logs.
* **Dynamic Merging**: It fuses real-time streaming transaction volume directly with historical catalog counts, computing comprehensive Category Volume vs. Price distributions and regional Brazilian Customer Loyalty concentrations, saving the finalized reports to `data_lake/gold/executive_dashboard/`.

---

## 5. Streamlit Executive Analytics UI Walkthrough

Our frontend application (`dashboards/streamlit_app.py`) is designed for executive clarity and real-time operations monitoring. Powered by a **5-second auto-refresh polling architecture**, it delivers instant visual tracking across four distinct operations centers:

### 🌟 Header & Live Executive KPI Cards
At the very top of the interface, four real-time executive indicator metrics display the instantaneous pulse of the business:
1. **Total Revenue (BRL)**: Combines historical catalog baseline sales (R$ 142,850.50) with all real-time streaming revenue calculated continuously by Flink Job 3.
2. **Completed Orders**: Tally of verified purchasing funnels processed through the platform.
3. **Active Shopper Sessions**: Count of concurrent shopper identities actively broadcasting clickstream telemetry.
4. **Fraud Security Alarms (CEP)**: Displays the cumulative, permanent total of all high-risk financial anomalies intercepted by Flink Job 4 and stored across PostgreSQL & Parquet archives.

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
* **Live Ingestion Feed**: Renders an auto-scrolling data table showing the most recent 15 customer interactions retrieved directly from active buffer streams.
* **Visible Telemetry**: Displays exact timestamps, event classifications (`checkout_started`, `payment_completed`, `product_viewed`), customer IDs, monetary amounts, and origin sources.

---

### 🚨 Tab 3: Fraud & Security Ops Center (Complex Event Processing)
A specialized security command console designed for financial anomaly investigators:
* **Permanent Archive Preservation Banner**: An alert bar displaying the full cumulative count of all suspicious payment attempts successfully intercepted and preserved inside the persistent PostgreSQL database and Medallion Data Lake.
* **UI Performance Limiter (Showing Latest 50 in UI)**: To guarantee sub-second frontend reactivity without browser slowdown, the table strictly renders the **most recent 50 intercepted alarms**, sorted descending by timestamp.
* **Granular Security Details**: Displays unique Alert UUIDs, target customer identities, violated CEP rules (`RAPID_FAILED_PAYMENT_ATTEMPTS_DETECTED`), and the calculated **Dynamic Risk Score** (ranging from 75.00 to 99.90 based on attempted theft monetary scale).

---

### 🏛️ Tab 4: Apache Spark Batch Historical Reports
Demonstrates macro analytical integration via Lakehouse Stream Fusion:
* **Top Olist Product Categories by Sales Density**: A horizontal color-coded bar chart mapping item sales concentration against average unit pricing across categories like `bed_bath_table`, `sports_leisure`, and `furniture_decor`.
* **Customer Demographic & Loyalty Distribution**: A multi-series tiered histogram displaying customer populations clustered across top Brazilian economic states (`SP`, `RJ`, `MG`, `RS`), segmented by VIP loyalty tier classifications (Bronze, Silver, Gold, Platinum).
* *Note*: This tab updates whenever `python spark/batch_historical_analytics.py` is executed on demand, dynamically expanding baseline numbers by incorporating accumulated Data Lake streaming traffic!

---

## 6. Platform Operations & Master Command Cheat Sheet

The platform is designed to be managed via simple, automated command-line scripts located in the `scripts/` directory:

| Action / Workflow | Command to Execute in Terminal | Description & System Impact |
| :--- | :--- | :--- |
| **🚀 Launch Live Platform** | `scripts\setup_and_run_all.bat` | One-click automated launcher. Verifies Python virtual environment, starts Docker infrastructure cluster, initializes master tables, and launches three background windows (Event Generator, Flink Streaming Worker Loop, and Streamlit UI). |
| **🛑 Stop All Services** | `scripts\stop_all.bat` | Gracefully terminates any background Node/Python loops and shuts down the Docker cluster (`docker compose down`). |
| **🧹 Factory Reset & Clean**| `scripts\clean_reset.bat` | **USE WITH CAUTION**: Terminates containers, purges all persistent PostgreSQL database volumes, and deletes all streaming transaction logs inside `data_lake/`, restoring the platform to an empty baseline. |
| **🔄 Update Batch Analytics**| `python spark/batch_historical_analytics.py` | Runs Apache Spark historical processing on demand. Fuses newly accumulated streaming transaction volume with master catalogs and updates Tab 4 in Streamlit. |
| **🔍 Monitor Docker Cluster**| `docker ps` | Verifies that all 7 microservice containers (Kafka, Zookeeper, Flink JobManager/TaskManager, Postgres, Prometheus, Grafana) are running actively and healthy. |

---
*End of Master Technical Documentation Guide.*
