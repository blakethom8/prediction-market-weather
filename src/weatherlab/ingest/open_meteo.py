from __future__ import annotations

import math
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Iterable

import requests

from ..settings import OPEN_METEO_BASE_URL
from .forecast_snapshots import ingest_forecast_snapshot


def _normal_cdf(x: float, mean: float, sigma: float) -> float:
    z = (x - mean) / (sigma * math.sqrt(2))
    return 0.5 * (1 + math.erf(z))


def build_threshold_distribution(*, point_temp_f: float, thresholds: Iterable[float], sigma_f: float = 3.0) -> dict[float, float]:
    if sigma_f <= 0:
        raise ValueError('sigma_f must be positive')
    distribution: dict[float, float] = {}
    for threshold in thresholds:
        prob_ge = 1 - _normal_cdf(float(threshold), point_temp_f, sigma_f)
        distribution[float(threshold)] = max(0.0, min(1.0, prob_ge))
    return distribution


def parse_open_meteo_daily_payload(*, payload: dict, city_id: str, target_date_local: date, thresholds: Iterable[float], fetched_at_utc: datetime | None = None, sigma_f: float = 3.0) -> dict:
    daily = payload.get('daily', {})
    times = daily.get('time', [])
    highs = daily.get('temperature_2m_max', [])
    lows = daily.get('temperature_2m_min', [])
    precip_probs = daily.get('precipitation_probability_max', [])

    target_key = target_date_local.isoformat()
    if target_key not in times:
        raise ValueError(f'target date {target_key} not present in Open-Meteo payload')

    idx = times.index(target_key)
    pred_high_temp = float(highs[idx]) if idx < len(highs) and highs[idx] is not None else None
    pred_low_temp = float(lows[idx]) if idx < len(lows) and lows[idx] is not None else None
    pred_precip_prob = float(precip_probs[idx]) / 100 if idx < len(precip_probs) and precip_probs[idx] is not None else None

    available_at = fetched_at_utc or datetime.now(UTC)
    distribution = {}
    if pred_high_temp is not None:
        distribution = build_threshold_distribution(
            point_temp_f=pred_high_temp,
            thresholds=thresholds,
            sigma_f=sigma_f,
        )

    return {
        'source': 'open-meteo',
        'city_id': city_id,
        'issued_at_utc': available_at,
        'available_at_utc': available_at,
        'target_date_local': target_date_local,
        'pred_high_temp_f': pred_high_temp,
        'pred_low_temp_f': pred_low_temp,
        'pred_precip_prob': pred_precip_prob,
        'summary_text': 'Open-Meteo daily forecast',
        'distribution': distribution,
    }


def ingest_open_meteo_daily_payload(*, payload: dict, city_id: str, target_date_local: date, thresholds: Iterable[float], fetched_at_utc: datetime | None = None, sigma_f: float = 3.0, db_path: str | Path | None = None) -> str:
    parsed = parse_open_meteo_daily_payload(
        payload=payload,
        city_id=city_id,
        target_date_local=target_date_local,
        thresholds=thresholds,
        fetched_at_utc=fetched_at_utc,
        sigma_f=sigma_f,
    )
    return ingest_forecast_snapshot(db_path=db_path, **parsed)


def fetch_open_meteo_daily_forecast(*, latitude: float, longitude: float, start_date: date, end_date: date) -> dict:
    response = requests.get(
        f'{OPEN_METEO_BASE_URL}/v1/forecast',
        params={
            'latitude': latitude,
            'longitude': longitude,
            'temperature_unit': 'fahrenheit',
            'daily': 'temperature_2m_max,temperature_2m_min,precipitation_probability_max',
            'timezone': 'auto',
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat(),
        },
        timeout=30,
    )
    response.raise_for_status()
    return response.json()
