"""Extract weather-only markets and trades from historical Kalshi parquet files.

Scans the full Kalshi dataset, filters to weather-related tickers, and loads
the results into the DuckDB warehouse raw layer.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path

import duckdb

from ..db import connect

logger = logging.getLogger(__name__)

# Default location of the prediction-market-analysis Kalshi parquet archive.
_DEFAULT_KALSHI_DIR = Path('/home/dataops/prediction-market-analysis/data/kalshi')
KALSHI_DATA_DIR = Path(
    os.environ.get('KALSHI_DATA_DIR', str(_DEFAULT_KALSHI_DIR))
)

# ---------------------------------------------------------------------------
# Weather ticker identification
# ---------------------------------------------------------------------------

# Ticker prefixes that correspond to weather markets.  Ordered longest-first
# so that prefix matching is unambiguous.
WEATHER_TICKER_PREFIXES: tuple[str, ...] = (
    'KXCITIESWEATHER',
    'KXHMONTHRANGE',
    'KXHIGHTBOS',
    'KXHIGHTATL',
    'KXHIGHTDAL',
    'KXHIGHTDC',
    'KXHIGHTHOU',
    'KXHIGHTSEA',
    'KXHIGHPHIL',
    'KXHIGHMIA',
    'KXHIGHCHI',
    'KXHIGHHOU',
    'KXHIGHLAX',
    'KXHIGHDEN',
    'KXHIGHNY',
    'KXHIGHBOS',
    'KXHIGHATL',
    'KXHIGHDAL',
    'KXHIGHDC',
    'KXHIGHSEA',
    'KXHIGHAUS',
    'KXRAINNYCM',
    'KXRAINNYC',
    'KXTORNADO',
    'KXSNOWNYM',
    'HIGHNY',
    'HIGHCHI',
    'HIGHMIA',
    'HIGHAUS',
    'HIGHUS',
    'RAINNYC',
    'RAINSEA',
    'SNOWNYM',
    'HURCAT',
)


def _build_weather_filter_sql(ticker_col: str = 'ticker') -> str:
    """Return a SQL WHERE clause fragment matching weather tickers."""
    conditions = [f"{ticker_col} LIKE '{prefix}%'" for prefix in WEATHER_TICKER_PREFIXES]
    return ' OR '.join(conditions)


def is_weather_ticker(ticker: str) -> bool:
    """Return True if *ticker* belongs to a known weather market family."""
    return any(ticker.startswith(prefix) for prefix in WEATHER_TICKER_PREFIXES)


# ---------------------------------------------------------------------------
# Extraction results
# ---------------------------------------------------------------------------

@dataclass
class ExtractionResult:
    markets_loaded: int
    trades_loaded: int
    weather_tickers_found: int


# ---------------------------------------------------------------------------
# Core extraction
# ---------------------------------------------------------------------------

def extract_weather_history(
    *,
    kalshi_dir: Path | None = None,
    db_path: str | Path | None = None,
) -> ExtractionResult:
    """Scan Kalshi parquet archive and load weather data into the warehouse.

    1. Reads all market parquet files, filters to weather tickers.
    2. Reads all trade parquet files, joins against the weather ticker set.
    3. Inserts into ``raw.kalshi_markets`` and ``raw.kalshi_market_snapshots``.

    Returns an :class:`ExtractionResult` with counts.
    """
    source_dir = kalshi_dir or KALSHI_DATA_DIR
    markets_glob = str(source_dir / 'markets' / '*.parquet')
    trades_glob = str(source_dir / 'trades' / '*.parquet')

    if not (source_dir / 'markets').exists():
        raise FileNotFoundError(f'Markets directory not found: {source_dir / "markets"}')
    if not (source_dir / 'trades').exists():
        raise FileNotFoundError(f'Trades directory not found: {source_dir / "trades"}')

    weather_filter = _build_weather_filter_sql('ticker')

    warehouse = connect(db_path=db_path)
    try:
        # Use a scratch connection to scan parquet efficiently, then copy
        # filtered results into the warehouse.

        logger.info('Scanning markets from %s ...', markets_glob)
        markets_loaded = _load_weather_markets(warehouse, markets_glob, weather_filter)
        logger.info('Loaded %d weather market rows.', markets_loaded)

        # Build ticker set from loaded markets for trade filtering.
        ticker_count = warehouse.execute(
            'SELECT count(DISTINCT market_ticker) FROM raw.kalshi_markets'
        ).fetchone()[0]

        logger.info('Scanning trades from %s ...', trades_glob)
        trades_loaded = _load_weather_trades(warehouse, trades_glob, weather_filter)
        logger.info('Loaded %d weather trade rows.', trades_loaded)

        return ExtractionResult(
            markets_loaded=markets_loaded,
            trades_loaded=trades_loaded,
            weather_tickers_found=ticker_count,
        )
    finally:
        warehouse.close()


def _load_weather_markets(
    con: duckdb.DuckDBPyConnection,
    glob_pattern: str,
    weather_filter: str,
) -> int:
    """Filter and insert weather markets into raw.kalshi_markets."""
    con.execute(f'''
        INSERT INTO raw.kalshi_markets (
            fetched_at_utc,
            source_file,
            market_ticker,
            event_ticker,
            title,
            subtitle,
            status,
            open_time_utc,
            close_time_utc,
            result
        )
        SELECT
            _fetched_at            AS fetched_at_utc,
            NULL                   AS source_file,
            ticker                 AS market_ticker,
            event_ticker,
            title,
            yes_sub_title          AS subtitle,
            status,
            open_time              AS open_time_utc,
            close_time             AS close_time_utc,
            result
        FROM read_parquet('{glob_pattern}')
        WHERE {weather_filter}
    ''')
    row = con.execute('SELECT count(*) FROM raw.kalshi_markets').fetchone()
    return row[0]


def _load_weather_trades(
    con: duckdb.DuckDBPyConnection,
    glob_pattern: str,
    weather_filter: str,
) -> int:
    """Filter and insert weather trades into raw.kalshi_market_snapshots.

    Each trade is stored as a market snapshot row: it captures the execution
    price at a specific moment, which is the closest we have to a time-series
    of market prices from the historical archive.
    """
    con.execute(f'''
        INSERT INTO raw.kalshi_market_snapshots (
            market_ticker,
            ts_utc,
            yes_bid,
            yes_ask,
            no_bid,
            no_ask,
            last_price,
            volume
        )
        SELECT
            ticker                AS market_ticker,
            created_time          AS ts_utc,
            NULL                  AS yes_bid,
            NULL                  AS yes_ask,
            NULL                  AS no_bid,
            NULL                  AS no_ask,
            yes_price             AS last_price,
            count                 AS volume
        FROM read_parquet('{glob_pattern}')
        WHERE {weather_filter}
    ''')
    row = con.execute('SELECT count(*) FROM raw.kalshi_market_snapshots').fetchone()
    return row[0]


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
    )
    result = extract_weather_history()
    print(
        f'Extraction complete: '
        f'{result.markets_loaded:,} market rows, '
        f'{result.trades_loaded:,} trade rows, '
        f'{result.weather_tickers_found:,} distinct weather tickers.'
    )


if __name__ == '__main__':
    main()
