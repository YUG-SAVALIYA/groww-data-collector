#!/usr/bin/env bash
set -e

echo "==============================================================================="
echo "  GROWW DAILY CANDLE DATA COLLECTION SYSTEM - NEW DEVICE LAUNCHER (Linux/macOS)"
echo "==============================================================================="

# Determine python command
if command -v python3 &>/dev/null; then
    PY_CMD=python3
elif command -v python &>/dev/null; then
    PY_CMD=python
else
    echo "[ERROR] Python 3 is not installed or not in PATH."
    exit 1
fi

echo "[*] Using Python: $PY_CMD"

# Install dependencies
echo "[*] Checking and installing dependencies from requirements.txt..."
$PY_CMD -m pip install -r "$(dirname "$0")/requirements.txt" --quiet

# Launch daemon
echo "[*] Launching Groww Collection System in Continuous Daemon Mode..."
$PY_CMD "$(dirname "$0")/groww_collector_service.py" --daemon
