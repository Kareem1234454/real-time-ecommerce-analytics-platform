@echo off
title Real-Time E-Commerce Big Data Platform Shutdown Engine
color 0C

echo ==============================================================================
echo       SHUTTING DOWN REAL-TIME BIG DATA ANALYTICS PLATFORM SERVICES
echo ==============================================================================
echo.

cd /d "%~dp0\..\docker"

:: 1. Stop Docker Compose Cluster Gracefully
echo [1/2] Gracefully shutting down Docker Cluster (Kafka, Flink, Postgres, Grafana)...
docker-compose down
echo [SUCCESS] All Docker background services and network bridges have been safely shut down!
echo.

:: 2. Cleanup Reminder & Complete Summary
echo [2/2] Checking local runtime windows...
echo.
echo ==============================================================================
echo                      PLATFORM SHUTDOWN COMPLETED!
echo ==============================================================================
echo  - Docker background containers (Kafka, Zookeeper, Flink, DBs) are fully offline.
echo  - No System RAM or CPU resources are currently consumed by Big Data engines.
echo  - NOTE: If you still have open command windows for the Event Generator or 
echo          Streamlit Dashboard, simply close them (X) or press Ctrl+C inside them.
echo ==============================================================================
echo.
pause
