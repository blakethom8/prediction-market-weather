import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / 'data'
CONFIG_DIR = ROOT / 'config'
SQL_DIR = ROOT / 'sql'

_DEFAULT_WAREHOUSE_PATH = DATA_DIR / 'warehouse' / 'weather_markets.duckdb'
WAREHOUSE_PATH = Path(os.environ.get('WEATHER_WAREHOUSE_PATH', str(_DEFAULT_WAREHOUSE_PATH))).expanduser()
WAREHOUSE_DIR = WAREHOUSE_PATH.parent
