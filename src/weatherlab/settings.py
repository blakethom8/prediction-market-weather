import os
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / 'data'
CONFIG_DIR = ROOT / 'config'
SQL_DIR = ROOT / 'sql'


def _load_local_env(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        key, value = line.split('=', 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
            os.environ.setdefault(key, value)


_load_local_env(ROOT / '.env')

_DEFAULT_WAREHOUSE_PATH = DATA_DIR / 'warehouse' / 'weather_markets.duckdb'
WAREHOUSE_PATH = Path(os.environ.get('WEATHER_WAREHOUSE_PATH', str(_DEFAULT_WAREHOUSE_PATH))).expanduser()
WAREHOUSE_DIR = WAREHOUSE_PATH.parent

OPEN_METEO_BASE_URL = os.environ.get('OPEN_METEO_BASE_URL', 'https://api.open-meteo.com')
NWS_API_BASE_URL = os.environ.get('NWS_API_BASE_URL', 'https://api.weather.gov')
KALSHI_API_KEY_ID = os.environ.get('KALSHI_API_KEY_ID', '')
KALSHI_API_PRIVATE_KEY_PATH = Path(
    os.environ.get('KALSHI_API_PRIVATE_KEY_PATH', '.kalshi_private_key.pem')
)
KALSHI_API_BASE_URL = os.environ.get(
    'KALSHI_API_BASE_URL',
    'https://trading-api.kalshi.com/trade-api/v2',
)


def _parse_focus_city_ids(raw: str | None) -> tuple[str, ...]:
    if not raw:
        return ('nyc', 'chi')
    return tuple(part.strip().lower() for part in raw.split(',') if part.strip())


FOCUS_CITY_IDS = _parse_focus_city_ids(os.environ.get('WEATHER_FOCUS_CITIES'))
