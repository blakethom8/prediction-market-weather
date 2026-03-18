from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / 'data'
WAREHOUSE_DIR = DATA_DIR / 'warehouse'
WAREHOUSE_PATH = WAREHOUSE_DIR / 'weather_markets.duckdb'
CONFIG_DIR = ROOT / 'config'
SQL_DIR = ROOT / 'sql'
