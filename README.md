# Real-Time E-Commerce Analytics Platform

A production-inspired **Big Data & Data Engineering Platform** designed for continuous ingestion, low-latency processing, storage, historical analytics, and real-time visualization of e-commerce events.

---

## 🏗️ Architecture Overview

```text
       [ Olist Master Data ]          [ Live Users & Scenarios ]
                 │                                 │
                 ▼                                 ▼
      Postgres Operational DB              Event Generator
                 │                                 │
                 ▼                                 ▼
         Enrichment Tables           Apache Kafka Streaming Layer
                 │                                 │
                 └───────────────┬─────────────────┘
                                 ▼
                      Apache Flink Engine
            ┌────────────────────┼────────────────────┐
            ▼                    ▼                    ▼
       Validation           Enrichment         Fraud Detection
            │                    │                    │
            └────────────────────┼────────────────────┘
                                 ▼
                 Hdfs Data Lake (Medallion Architecture)
            [ Bronze: Raw JSON ] ➔ [ Silver: Parquet ] ➔ [ Gold: KPIs ]
                                 │
                                 ▼
                    Apache Hive & Apache Spark
                                 │
                                 ▼
                    Streamlit Real-Time Dashboard & Grafana
```

---

## 📦 Key Components

1. **Event Generator (`generator/`)**: Python simulation service generating realistic customer sessions, shopping behavior, orders, reviews, and fraud attempts across multiple scenarios (Normal Day, Black Friday, Flash Sale).
2. **Streaming Layer (`kafka/` & `docker/`)**: Apache Kafka and Zookeeper cluster routing 11 decoupled business and operational event topics.
3. **Stream Processing (`flink/`)**: Apache Flink real-time streaming jobs performing UUID schema validation, DLQ redirection, Postgres master dataset enrichment, stateful Tumbling/Sliding window analytics, and rule-based fraud detection.
4. **Data Lake Storage (`datasets/` & `hive/`)**: Medallion Data Lake architecture storing raw JSON events (Bronze), cleaned & enriched Parquet files (Silver), and aggregated business metrics (Gold) formatted for Apache Hive external table queries.
5. **Batch Processing (`spark/`)**: PySpark applications computing daily/monthly historical growth, Customer Lifetime Value (CLV), conversion rates, and sales funnels.
6. **Live Dashboard (`dashboards/`)**: State-of-the-art interactive **Streamlit Web Dashboard** (`streamlit_app.py`) displaying live streaming Kafka metrics, Executive KPI ticker cards, interactive Plotly analytical charts, and real-time Fraud alarms.

---

## 🚀 One-Click Startup (Windows)

1. Ensure **Docker Desktop** is active and running.
2. Double-click or run the automated execution script in PowerShell/Command Prompt:
   ```cmd
   scripts\setup_and_run_all.bat
   ```
3. Open your web browser to view the interactive Live Dashboard:
   - **Streamlit Real-Time App**: `http://localhost:8501`
   - **Grafana Container Metrics**: `http://localhost:3000` (User: `admin`, Pass: `admin`)

---

## 📂 Project Structure

```
d:\Big Data_NTI\Final_Project/
│
├── kaggle_data/      # Raw Olist E-Commerce dataset CSV files
├── docs/             # Comprehensive technical design documentation
├── docker/           # Docker Compose infrastructure & database configs
├── kafka/            # Kafka topic definitions & automated setup scripts
├── generator/        # Python event generator simulation engine
├── flink/            # PyFlink streaming transformation & fraud detection jobs
├── hive/             # Hive external Data Lake DDL scripts
├── spark/            # PySpark historical & batch analytics reporting
├── dashboards/       # Streamlit real-time interactive dashboard application
├── scripts/          # Automation, dataset seeding, and folder initialization
├── datasets/         # Olist processing, Parquet staging, and database seeds
├── requirements.txt  # Python package dependency specifications
└── README.md         # Architecture documentation and usage guide
```
