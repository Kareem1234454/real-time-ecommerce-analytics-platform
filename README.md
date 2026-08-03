# Real-Time E-Commerce Analytics Platform

A production-inspired **Big Data & Data Engineering Platform** designed for continuous ingestion, low-latency processing, storage, historical analytics, and real-time visualization of e-commerce events.

---

## 🏗️ Streaming & Lakehouse Architecture

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

## ⚡ Current Streaming Implementation

The current streaming layer is implemented using an **Event-Driven Python Micro-Batch Worker Loop** (`run_streaming_workers.py`) rather than native PyFlink jobs.

The worker continuously executes four streaming processing stages every **5 seconds**, providing near real-time analytics while maintaining reliable execution on Windows development environments.

This design was chosen because native PyFlink deployment on Windows introduces significant compatibility challenges, including Python package availability, Java configuration, filesystem path differences, and dependency management. The micro-batch architecture provides a stable development experience while preserving the logical structure of a streaming data pipeline.

---

## 🐿️ Apache Flink Infrastructure

An Apache Flink cluster (JobManager and TaskManager) is provisioned through Docker and remains available as part of the platform infrastructure (`http://localhost:8081`).

In the current implementation, the streaming jobs are **not executed by the Flink runtime**. Instead, the processing logic runs through the Python micro-batch workers.

The containerized Flink cluster is maintained to:
- Demonstrate production-ready distributed infrastructure.
- Support future migration to native Flink jobs.
- Provide an architecture consistent with enterprise streaming platforms.

This allows the project to evolve toward fully distributed Flink execution on Linux or cloud environments without redesigning the overall architecture.

---

## 📦 Core Processing Layers

1. **Event Generator (`generator/`)**: Python simulation engine synthetically broadcasting realistic customer clickstream sessions, shopping behaviors, and orchestrated scenarios (Flash Sales, Cart Abandonment, and Payment Fraud testing) using real Olist reference entity IDs.
2. **Streaming Ingestion (`kafka/` & `docker/`)**: Active Apache Kafka brokers and Zookeeper cluster (`localhost:9092`) routing live transaction telemetry across decoupled operational topics.
3. **Stream Processing Pipeline (`flink/`)**:
   - **Job 1 (Validation & DLQ)**: Detects malformed payloads and routes corrupt events to the Dead Letter Queue.
   - **Job 2 (Stream Enrichment)**: Joins streaming UUIDs with master customer demographics and product catalogs.
   - **Job 3 (KPI Aggregation)**: Computes running Gross Merchandise Value (GMV), payment distributions, and conversion metrics over tumbling time windows.
   - **Job 4 (Fraud Detection)**: Identifies rapid failed payment sequences, calculates dynamic risk severity (75.00 to 99.90), and synchronously writes alerts to both PostgreSQL operational databases and Gold Parquet archives.
4. **Medallion Data Lakehouse (`data_lake/`)**: Modern disk-based partitioned Data Lake structured into **Bronze** (raw immutable `.jsonl`), **Silver** (cleansed & enriched Snappy `.parquet`), and **Gold** (aggregated business intelligence tables).
5. **Batch Historical Analytics (`spark/`)**: Dedicated historical batch engine (`scripts\run_spark_batch.bat`) executing Lakehouse Stream Fusion—merging static master reference counts directly with newly accumulated live streaming transaction volumes while modeling dynamic market price elasticity.
6. **Executive Operations UI (`dashboards/`)**: Interactive **Streamlit Web Application** (`http://localhost:8501`) featuring a 4-tab command console with sub-second polling refreshes and hybrid SQL/Parquet database failover, alongside containerized **Grafana DevOps Surveillance** (`http://localhost:3000`).

---

## 🚀 Quickstart & Setup (Windows)

### 1. Download Master Datasets
Download the official **Olist Brazilian E-Commerce Dataset** from Kaggle:
👉 [https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

Extract the downloaded CSV files into your local repository's **`kaggle_data/`** directory.

### 2. Launch Platform & Infrastructure
1. Ensure **Docker Desktop** is active and running.
2. Open terminal in the root directory and run our automated execution script:
   ```cmd
   scripts\setup_and_run_all.bat
   ```
3. Open your web browser to view your live operational consoles:
   - **Streamlit Real-Time Dashboard**: `http://localhost:8501`
   - **Grafana DevOps Surveillance**: `http://localhost:3000` *(Login: `admin` / `admin`)*
   - **Apache Flink Infrastructure UI**: `http://localhost:8081`

---

## 📂 Project Repository Structure

```
d:\Big Data_NTI\Final_Project/
│
├── dashboards/       # Streamlit real-time interactive dashboard application (SQL/Parquet failover)
├── data_lake/        # Medallion Data Lakehouse structure (Bronze/Silver/Gold storage layers)
├── datasets/         # Olist processing scripts, Parquet staging catalogs, and database ACID loaders
├── docker/           # Docker Compose microservice orchestration (Kafka, Zookeeper, Flink, Postgres, Grafana)
├── docs/             # Master Platform Guide and detailed structural specifications
├── flink/            # Python event-driven micro-batch worker loops and streaming job architectures
├── generator/        # Synthetic interactive customer session generator & scenario modeling engine
├── kaggle_data/      # Raw public Olist Brazilian E-Commerce seed dataset CSV archives (~120MB)
├── scripts/          # Automated Windows lifecycle orchestration utilities (.bat scripts)
├── spark/            # Dynamic historical batch analytical reporting and pricing elasticity stream fusion
├── requirements.txt  # Pinned python library dependency specifications
└── README.md         # Master project architecture overview and usage instruction manual
```
