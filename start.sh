#!/bin/bash
# Quick start script for Put Option Screener Web UI

echo "=================================="
echo "Put Option Screener"
echo "=================================="
echo ""

if ! command -v python3 &> /dev/null; then
    echo "Error: Python 3 is not installed"
    exit 1
fi

echo "Checking dependencies..."
python3 -c "import flask, yfinance, pandas, numpy" 2>/dev/null
if [ $? -ne 0 ]; then
    echo "Installing dependencies..."
    pip3 install -q -r requirements.txt
fi

echo "Starting web server..."
echo ""
python3 web_app.py
