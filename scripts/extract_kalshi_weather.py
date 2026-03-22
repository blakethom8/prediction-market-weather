#!/usr/bin/env python3
"""Extract weather markets and trades from the Kalshi parquet archive.

Usage:
    PYTHONPATH=src python scripts/extract_kalshi_weather.py

Set KALSHI_DATA_DIR to override the default parquet location.
"""
from weatherlab.ingest.kalshi_history import main

if __name__ == '__main__':
    main()
