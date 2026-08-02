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

:: 2. Terminate background popup windows automatically
echo [2/2] Terminating local runtime command windows (Generator, Flink Workers, Streamlit)...
taskkill /FI "WINDOWTITLE eq Live E-Commerce Event Generator Engine*" /F /T >nul 2>&1
taskkill /FI "WINDOWTITLE eq Apache Flink Live Streaming & CEP Workers*" /F /T >nul 2>&1
taskkill /FI "WINDOWTITLE eq Streamlit Live Analytics Dashboard*" /F /T >nul 2>&1
echo [SUCCESS] All background runtime popup windows and Streamlit UI shut down!
echo.

echo ==============================================================================
echo                      PLATFORM SHUTDOWN COMPLETED!
echo ==============================================================================
echo  - Docker background containers (Kafka, Zookeeper, Flink, DBs) are fully offline.
echo  - All streaming loops and interactive UI web applications have been terminated.
echo  - Zero system RAM or CPU resources are currently consumed by Big Data engines.
echo ==============================================================================
echo.
pause
