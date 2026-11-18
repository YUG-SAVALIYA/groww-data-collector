@echo off
setlocal enabledelayedexpansion

echo ===============================================================================
echo   GROWW DAILY CANDLE DATA COLLECTION SYSTEM - NEW DEVICE LAUNCHER
echo ===============================================================================

:: 1. Verify Python availability
where py >nul 2>nul
if %ERRORLEVEL% equ 0 (
    set PY_CMD=py
    goto python_found
)

where python >nul 2>nul
if %ERRORLEVEL% equ 0 (
    set PY_CMD=python
    goto python_found
)

echo [ERROR] Python is not installed or not added to PATH.
echo Please install Python 3.10+ from python.org and check "Add Python to PATH".
pause
exit /b 1

:python_found
echo [*] Using Python: %PY_CMD%

:: 2. Install / verify dependencies
echo [*] Checking and installing required dependencies...
%PY_CMD% -m pip install -r "%~dp0requirements.txt" --quiet

if %ERRORLEVEL% neq 0 (
    echo [ERROR] Failed to install dependencies. Check your internet connection.
    pause
    exit /b 1
)

:: 3. Launch the collection service in background daemon mode
echo [*] Starting Groww Collection System in Continuous 24/7 Daemon Mode...
echo [*] Automated Schedule: 3:55 PM (Sync), 4:00 PM (Fetch), 6:00 AM (Final Correction)
echo [*] Desktop Folder: NSE Equity Companies
echo ===============================================================================

%PY_CMD% "%~dp0groww_collector_service.py" --daemon

pause
