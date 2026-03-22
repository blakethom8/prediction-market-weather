"""Backfill historical weather data from Open-Meteo archive API.

Fetches actual observed high/low temperatures for all contract dates
and stores them as forecast snapshots with probability distributions.
This enables the training view to compute fair_prob and edge.

Uses the Open-Meteo archive endpoint (not the forecast endpoint)
to retrieve reanalysis data for historical dates.
"""
from __future__ import annotations

import logging
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import requests

from ..db import connect
from ..utils.ids import new_id
from .open_meteo import build_threshold_distribution

logger = logging.getLogger(__name__)

ARCHIVE_URL = 'https://archive-api.open-meteo.com/v1/archive'


def _fetch_archive(
    *,
    latitude: float,
    longitude: float,
    start_date: date,
    end_date: date,
    timezone_name: str,
) -> dict[str, Any]:
    """Fetch daily historical weather from Open-Meteo archive API."""
    resp = requests.get(
        ARCHIVE_URL,
        params={
            'latitude': latitude,
            'longitude': longitude,
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat(),
            'daily': 'temperature_2m_max,temperature_2m_min,precipitation_sum',
            'temperature_unit': 'fahrenheit',
            'timezone': timezone_name,
        },
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()


def backfill_historical_forecasts(
    *,
    db_path: str | Path | None = None,
    sigma_f: float = 3.0,
) -> int:
    """Fetch historical weather for all contract cities/dates and load forecasts.

    For each city, fetches the full date range from Open-Meteo archive,
    then creates forecast_snapshot + forecast_distribution rows for every
    contract threshold on those dates.

    Returns the total number of forecast snapshots inserted.
    """
    con = connect(db_path=db_path)
    try:
        # Get cities with their date ranges and coordinates
        cities = con.execute('''
            SELECT
                c.city_id, ci.lat, ci.lon, ci.timezone_name,
                min(c.market_date_local) AS start_date,
                max(c.market_date_local) AS end_date
            FROM core.weather_contracts c
            JOIN core.cities ci ON ci.city_id = c.city_id
            WHERE c.parse_status = 'parsed'
              AND c.market_date_local IS NOT NULL
            GROUP BY c.city_id, ci.lat, ci.lon, ci.timezone_name
            ORDER BY c.city_id
        ''').fetchall()

        # Get all contract thresholds grouped by city + date.
        # For 'between' contracts, also include threshold_high_f + 1 so we
        # can compute bracket probability = P(>=low) - P(>=high+1).
        thresholds_raw = con.execute('''
            SELECT city_id, market_date_local, threshold_low_f, threshold_high_f, operator
            FROM core.weather_contracts
            WHERE parse_status = 'parsed'
              AND threshold_low_f IS NOT NULL
              AND city_id IS NOT NULL
              AND market_date_local IS NOT NULL
        ''').fetchall()

        # Build lookup: (city_id, date) → set of thresholds
        thresholds_by_city_date: dict[tuple[str, date], set[float]] = {}
        for city_id, mkt_date, threshold_low, threshold_high, operator in thresholds_raw:
            key = (city_id, mkt_date)
            if key not in thresholds_by_city_date:
                thresholds_by_city_date[key] = set()
            thresholds_by_city_date[key].add(float(threshold_low))
            if operator == 'between' and threshold_high is not None:
                thresholds_by_city_date[key].add(float(threshold_high) + 1.0)

        total_inserted = 0

        for city_id, lat, lon, tz, start_date, end_date in cities:
            logger.info(
                'Fetching %s: %s to %s (lat=%.4f, lon=%.4f)',
                city_id, start_date, end_date, lat, lon,
            )

            try:
                data = _fetch_archive(
                    latitude=lat,
                    longitude=lon,
                    start_date=start_date,
                    end_date=end_date,
                    timezone_name=tz,
                )
            except requests.RequestException as exc:
                logger.error('Failed to fetch %s: %s', city_id, exc)
                continue

            daily = data.get('daily', {})
            times = daily.get('time', [])
            highs = daily.get('temperature_2m_max', [])
            lows = daily.get('temperature_2m_min', [])

            snapshot_batch = []
            dist_batch = []
            now_utc = datetime.now(timezone.utc)

            for i, date_str in enumerate(times):
                target_date = date.fromisoformat(date_str)
                key = (city_id, target_date)

                if key not in thresholds_by_city_date:
                    continue  # No contracts for this date

                high_f = float(highs[i]) if highs[i] is not None else None
                low_f = float(lows[i]) if lows[i] is not None else None

                if high_f is None:
                    continue

                forecast_id = new_id('forecast')
                snapshot_batch.append((
                    forecast_id,
                    'open-meteo-archive',
                    city_id,
                    now_utc,       # issued_at_utc
                    now_utc,       # available_at_utc (backfill — use current time)
                    target_date,
                    high_f,
                    low_f,
                    None,          # pred_precip_prob
                    'Open-Meteo archive reanalysis',
                    None,          # raw_ref
                ))

                # Build probability distribution for each contract threshold
                thresholds = thresholds_by_city_date[key]
                distribution = build_threshold_distribution(
                    point_temp_f=high_f,
                    thresholds=thresholds,
                    sigma_f=sigma_f,
                )
                for threshold_f, prob_ge in distribution.items():
                    dist_batch.append((forecast_id, threshold_f, prob_ge))

            # Bulk insert
            if snapshot_batch:
                con.executemany(
                    '''
                    INSERT INTO core.forecast_snapshots (
                        forecast_snapshot_id, source, city_id, issued_at_utc,
                        available_at_utc, target_date_local, pred_high_temp_f,
                        pred_low_temp_f, pred_precip_prob, summary_text, raw_ref
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''',
                    snapshot_batch,
                )
            if dist_batch:
                con.executemany(
                    '''
                    INSERT INTO core.forecast_distributions (
                        forecast_snapshot_id, threshold_f, prob_ge_threshold
                    ) VALUES (?, ?, ?)
                    ''',
                    dist_batch,
                )

            city_count = len(snapshot_batch)
            total_inserted += city_count
            logger.info(
                '%s: %d forecast snapshots, %d distribution rows.',
                city_id, city_count, len(dist_batch),
            )

            # Be polite to the free API
            time.sleep(1)

        return total_inserted
    finally:
        con.close()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
    )
    count = backfill_historical_forecasts()
    print(f'Backfill complete: {count:,} forecast snapshots inserted.')


if __name__ == '__main__':
    main()
