@echo off
echo ==============================================================================
echo        [APACHE SPARK] EXECUTING HISTORICAL BATCH ANALYTICS ENGINE...
echo ==============================================================================

:: Ensure working directory is set to the main project root
cd /d %~dp0\..

:: Activate Virtual Environment if available, or call python executable directly
if exist "venv\Scripts\python.exe" (
    echo [INFO] Executing within dedicated virtual environment...
    venv\Scripts\python.exe spark\batch_historical_analytics.py
) else (
    echo [WARNING] Virtual environment not found! Executing with system python...
    python spark\batch_historical_analytics.py
)

echo ==============================================================================
echo [SUCCESS] Batch analytics complete. Check Streamlit Tab 4 for updated charts!
echo ==============================================================================
pause
