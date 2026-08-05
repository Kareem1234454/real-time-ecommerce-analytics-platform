@echo off
title Real-Time E-Commerce Big Data Platform Deep Factory Reset
color 0E

echo ==============================================================================
echo    WARNING: PERFORMING TOTAL DEEP FACTORY RESET OF ALL PROCESSED DATA
echo ==============================================================================
echo  This will wipe clean all:
echo    1. Docker databases and Kafka message topic volumes
echo    2. Medallion Data Lake stream records (Bronze, Silver, and Gold logs)
echo    3. Processed Olist reference Parquet tables
echo.
echo  YOUR RAW KAGGLE CSV DATA AND PYTHON CODE WILL REMAIN COMPLETELY SAFE!
echo ==============================================================================
echo.
echo Press any key to start the clean wipe, or close this window to cancel...
pause >nul

cd /d "%~dp0\.."

:: 1. Terminate background popup windows automatically
echo [1/5] Terminating active streaming command windows (Generator, Flink Workers, Streamlit)...
taskkill /FI "WINDOWTITLE eq Live E-Commerce Event Generator Engine*" /F /T >nul 2>&1
taskkill /FI "WINDOWTITLE eq Apache Flink Live Streaming & CEP Workers*" /F /T >nul 2>&1
taskkill /FI "WINDOWTITLE eq Streamlit Live Analytics Dashboard*" /F /T >nul 2>&1
echo [SUCCESS] All streaming loops and UI windows shut down!
echo.

:: 2. Stop Docker and destroy persistent volume mounts (-v wipes DB & HDFS volumes)
echo [2/5] Stopping Docker containers and wiping PostgreSQL & Hadoop HDFS volume caches...
cd docker
docker-compose down -v --remove-orphans
cd ..
echo [SUCCESS] Docker cluster stopped and HDFS/Postgres database volumes purged!
echo.

:: 2. Clean up Medallion Data Lake streams
echo [2/4] Wiping processed streaming logs in Medallion Data Lake...
if exist data_lake\bronze rmdir /s /q data_lake\bronze
if exist data_lake\silver rmdir /s /q data_lake\silver
if exist data_lake\gold rmdir /s /q data_lake\gold
echo [SUCCESS] Bronze, Silver, and Gold analytics storage zones cleared!
echo.

:: 3. Clean up processed master data staging
echo [3/4] Clearing staging master tables in datasets\master_data...
if exist datasets\master_data rmdir /s /q datasets\master_data
mkdir datasets\master_data
echo [SUCCESS] Master dataset cache purged!
echo.

:: 4. Rebuild fresh folder structures
echo [4/4] Re-initializing pristine Medallion storage directory architecture...
if exist venv\Scripts\activate.bat (
    call venv\Scripts\activate.bat
    python scripts/create_lake_directories.py
) else (
    python scripts/create_lake_directories.py
)
echo.
echo ==============================================================================
echo             DEEP FACTORY RESET COMPLETED SUCCESSFULLY!
echo ==============================================================================
echo  Your project environment is now completely clean and zero-state.
echo  To start fresh from the very beginning, simply execute:
echo     scripts\setup_and_run_all.bat
echo ==============================================================================
echo.
pause
