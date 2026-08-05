# Real-Time E-Commerce Analytics Platform

A production-inspired **Big Data & Data Engineering Platform** designed for continuous ingestion, low-latency processing, storage, historical analytics, and real-time visualization of e-commerce events backed by an enterprise containerized **Apache Hadoop Distributed File System (HDFS)**.

---

## 🏗️ Streaming & Distributed Data Lake Architecture

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
  Apache Hadoop HDFS             PostgreSQL
  Distributed Data Lake         Fraud Alerts
 (Bronze/Silver/Gold)                 │
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

## 🐘 Apache Hadoop HDFS & Distributed Data Lake Architecture

Our platform employs an **Enterprise Distributed Data Lake** architecture that combines scalable distributed storage, structured Medallion data organization, and real-time analytics for continuous stream processing.
1. **Apache Hadoop HDFS (`http://localhost:9870`) — The Storage Infrastructure**: The physical containerized cluster (NameNode & DataNode) that provides resilient distributed block file storage and network accessibility.
2. **Medallion Data Lake (`data_lake/`) — The Storage Organizational Design**: A structured multi-hop data refinement design pattern organizing files within HDFS into three strict analytical tiers:
   - 🥉 **Bronze**: Raw, immutable JSON-Lines (`.jsonl`) streaming telemetry directly from Kafka channels.
   - 🥈 **Silver**: Cleansed, schema-validated, and customer-enriched columnar **Snappy Apache Parquet** (`.parquet`) tables.
   - 🥇 **Gold**: Aggregated business intelligence reporting tables, tumbling 1-hour KPI metrics, and security logs.
3. **Distributed Analytics Architecture**: The overall platform combines Apache Hadoop HDFS for distributed storage, the Medallion Data Lake design pattern for data organization, Apache Spark for historical analytics, Apache Flink infrastructure for stream-processing deployment, Apache Parquet for efficient columnar storage, and PostgreSQL for operational fraud metadata and alert management.

### 🔄 Dual-Write Mirroring & Hybrid Read-Fallback Reliability
To guarantee 100% operational availability on Windows host laptops and during Docker networking resets, our entire pipeline implements **Enterprise Dual-Write Mirroring**:
* **Synchronous Dual-Writes**: All streaming workers and generators write real-time events and Parquet tables simultaneously to both the distributed **Hadoop HDFS cluster blocks** (`http://localhost:9870`) and a local filesystem **Data Lake mirror** (`data_lake/`).
* **Hybrid Read-Fallback**: Analytical visualization layers (Streamlit UI and Spark historical batch jobs) prioritize WebHDFS block extraction; if HDFS network timeouts occur, engines instantly and transparently fall back to querying the local Data Lake mirrors without interrupting operations or dropping charts!

---

## 📦 Core Processing Layers

1. **Event Generator (`generator/`)**: Python simulation engine synthetically broadcasting realistic customer clickstream sessions, shopping behaviors, and orchestrated scenarios (Flash Sales, Cart Abandonment, and Payment Fraud testing) using real Olist reference entity IDs directly into Kafka and HDFS Bronze buffers.
2. **Streaming Ingestion (`kafka/` & `docker/`)**: Active Apache Kafka brokers and Zookeeper cluster (`localhost:9092`) routing live transaction telemetry across decoupled operational topics.
3. **Stream Processing Pipeline (`flink/`)**:
   - **Job 1 (Validation & DLQ)**: Detects malformed payloads from HDFS and routes corrupt events to the Dead Letter Queue.
   - **Job 2 (Stream Enrichment)**: Joins streaming UUIDs with master customer demographics and product catalogs, serializing Snappy Parquet tables into HDFS Silver blocks.
   - **Job 3 (KPI Aggregation)**: Computes running Gross Merchandise Value (GMV), payment distributions, and conversion metrics over tumbling time windows into HDFS Gold blocks.
   - **Job 4 (Fraud Detection)**: Identifies rapid failed payment sequences, calculates dynamic risk severity (75.00 to 99.90), and synchronously writes alerts to both PostgreSQL operational databases and HDFS Gold Parquet archives.
4. **Apache Hadoop HDFS Distributed Data Lake (`data_lake/`, `docker/` & `utils/`)**: Enterprise containerized Apache Hadoop Distributed File System (HDFS NameNode port `9870`) implementing a Medallion Data Lake architecture with Bronze (raw immutable JSONL), Silver (cleansed and enriched Snappy Parquet), and Gold (aggregated business intelligence datasets). All streaming, batch analytics, and visualization services interact directly with HDFS through a unified WebHDFS client layer.
5. **Batch Historical Analytics (`spark/`)**: Dedicated historical batch engine (`scripts\run_spark_batch.bat`) performing historical analytics by merging static master reference datasets with newly accumulated live HDFS streaming transaction volumes while modeling dynamic market price elasticity.
6. **Executive Operations UI (`dashboards/`)**: Interactive **Streamlit Web Application** (`http://localhost:8501`) featuring a 4-tab command console with sub-second polling refreshes, HDFS block monitoring, and hybrid SQL/Parquet database failover, alongside containerized **Grafana DevOps Surveillance** (`http://localhost:3000`).

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
   - **Apache Hadoop HDFS NameNode Web UI**: `http://localhost:9870` *(Browse `/data_lake/` HDFS blocks live under Utilities -> Browse the file system)*
   - **Grafana DevOps Surveillance**: `http://localhost:3000` *(Login: `admin` / `admin`)*
   - **Apache Flink Infrastructure UI**: `http://localhost:8081`

---

## 📂 Project Repository Structure

```
d:\Big Data_NTI\Final_Project/
│
├── dashboards/       # Streamlit real-time interactive dashboard application (HDFS & SQL monitors)
├── data_lake/        # Local fallback Medallion Data Lake mirror structure (Bronze/Silver/Gold)
├── datasets/         # Olist processing scripts, Parquet staging catalogs, and database ACID loaders
├── docker/           # Docker Compose microservice orchestration (Hadoop HDFS, Kafka, Flink, Postgres, Grafana)
├── docs/             # Master Platform Guide and detailed structural specifications
├── flink/            # Python event-driven micro-batch worker loops and HDFS streaming architectures
├── generator/        # Synthetic interactive customer session generator & scenario modeling engine
├── kaggle_data/      # Raw public Olist Brazilian E-Commerce seed dataset CSV archives (~120MB)
├── scripts/          # Automated Windows lifecycle orchestration & HDFS initialization utilities (.bat / .py)
├── spark/            # Dynamic historical batch analytical reporting and HDFS pricing stream fusion
├── utils/            # Unified HDFS client bridge (WebHDFS REST in-memory buffers & failover management)
├── requirements.txt  # Pinned python library dependency specifications
└── README.md         # Master project architecture overview and usage instruction manual
```
