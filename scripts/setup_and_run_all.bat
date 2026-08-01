@echo off
setlocal
title Real-Time E-Commerce Big Data Platform Master Launcher
color 0B

echo ==============================================================================
echo       REAL-TIME E-COMMERCE BIG DATA ANALYTICS PLATFORM LAUNCHER
echo ==============================================================================

cd /d "%~dp0\.."

:: 1. Initialize Python Environment
echo [1/8] Verifying Python Environment and Dependencies...
python -m venv venv
call venv\Scripts\activate.bat
pip install --upgrade pip
pip install -r requirements.txt
echo.

:: 2. Initialize Medallion Data Lake Storage Zones
echo [2/8] Setting up Medallion Data Lake Directory Structure (Bronze/Silver/Gold)...
python scripts/create_lake_directories.py
echo.

:: 3. Process Olist Kaggle Master Datasets
echo [3/8] Processing Olist Brazilian Master Datasets into Parquet & JSON...
python datasets/setup_master_data.py
echo.

:: 4. Launch Docker Infrastructure
echo [4/8] Launching Distributed Big Data Docker Cluster (Kafka, Flink, Postgres, Grafana)...
cd docker
docker-compose up -d
cd ..
echo Waiting 10 seconds for Kafka and PostgreSQL containers to stabilize...
timeout /t 10 /nobreak >nul
echo.

:: 5. Seed PostgreSQL Operational Database
echo [5/8] Seeding PostgreSQL Operational Database with Olist Master Catalogs...
python datasets/seed_postgres.py
echo.

:: 6. Provision Kafka Topics
echo [6/8] Provisioning Apache Kafka Streaming Topics...
python kafka/create_topics.py
echo.

:: 7. Execute Apache Spark Batch Historical Analytics & Flink Pass
echo [7/8] Running Apache Spark Batch Historical Reporting & Flink Verification...
python spark/batch_historical_analytics.py
python flink/job_1_validation_dlq.py
python flink/job_2_enrichment.py
python flink/job_3_kpi_aggregations.py
python flink/job_4_fraud_detection.py
echo.

:: 8. Start Live Generator and Streamlit Real-Time Dashboard
echo [8/8] Starting Real-Time Event Simulation and Interactive Web Dashboard...
start "Live E-Commerce Event Generator Engine" cmd /k "call venv\Scripts\activate.bat && python generator/run_generator.py"
start "Streamlit Live Analytics Dashboard" cmd /c "call venv\Scripts\activate.bat && streamlit run dashboards/streamlit_app.py"

echo.
echo ==============================================================================
echo    SUCCESS! BIG DATA PLATFORM IS FULLY OPERATIONAL AND STREAMING LIVE!
echo ==============================================================================
echo  - Streamlit Real-Time Dashboard: http://localhost:8501
echo  - Grafana Infrastructure Dashboard: http://localhost:3000 (admin/admin)
echo  - Flink JobManager Web UI: http://localhost:8081
echo  - Kafka Broker Address: localhost:9092
echo ==============================================================================
pause
