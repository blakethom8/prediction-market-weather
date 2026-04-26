"""Wrappers around existing ingest modules for the Chief CLI sync command."""
from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def sync_kalshi(db_path: str | Path | None = None) -> dict[str, Any]:
    """Sync live Kalshi weather markets.  Returns a summary dict."""
    try:
        from ..ingest.kalshi_live_sync import sync_live_weather_markets
        result = sync_live_weather_markets(db_path=db_path)
        return {
            "contracts_synced": result.get("contracts_synced", 0),
            "new_contracts": result.get("new_contracts", 0),
            "updated_contracts": result.get("updated_contracts", 0),
            "snapshots_synced": result.get("snapshots_synced", 0),
        }
    except Exception as exc:
        logger.warning("Kalshi sync failed: %s", exc)
        return {"error": str(exc)}


def sync_forecasts(db_path: str | Path | None = None) -> dict[str, Any]:
    """Sync weather forecasts for upcoming contract dates.

    Falls back to historical backfill if live forecast sync is unavailable.
    """
    try:
        from ..ingest.historical_forecasts import backfill_historical_forecasts
        from ..settings import FOCUS_CITY_IDS
        from ..db import connect

        # Determine which city_ids have contracts in the next 7 days
        today = date.today()
        con = connect(read_only=True, db_path=db_path)
        try:
            rows = con.execute(
                """
                SELECT DISTINCT city_id
                FROM core.weather_contracts
                WHERE market_date_local >= ?
                  AND market_date_local <= ? + INTERVAL '7 days'
                  AND city_id IS NOT NULL
                """,
                [today, today],
            ).fetchall()
            city_ids = [row[0] for row in rows] if rows else list(FOCUS_CITY_IDS)
        except Exception:
            city_ids = list(FOCUS_CITY_IDS)
        finally:
            con.close()

        cities_updated = backfill_historical_forecasts(
            city_ids=city_ids,
            db_path=db_path,
        )
        return {"cities_updated": cities_updated if isinstance(cities_updated, int) else len(city_ids)}
    except Exception as exc:
        logger.warning("Forecast sync failed: %s", exc)
        return {"error": str(exc)}


def rematerialize_board(db_path: str | Path | None = None) -> dict[str, Any]:
    """Rematerialize training rows and return board size."""
    try:
        from ..build.training_rows import materialize_training_rows
        from ..ingest.kalshi_live_sync import _fetch_board_size

        materialize_training_rows(db_path=db_path)
        board_size = _fetch_board_size(db_path=db_path)
        return {
            "board_size": board_size,
            "date": date.today().isoformat(),
        }
    except Exception as exc:
        logger.warning("Board rematerialization failed: %s", exc)
        return {"error": str(exc)}


def run_full_sync(db_path: str | Path | None = None) -> dict[str, Any]:
    """Run Kalshi sync + forecast sync + board rematerialization in sequence."""
    return {
        "kalshi": sync_kalshi(db_path=db_path),
        "forecasts": sync_forecasts(db_path=db_path),
        "board": rematerialize_board(db_path=db_path),
    }
