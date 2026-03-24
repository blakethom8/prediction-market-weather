from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
import logging
from typing import Any
from zoneinfo import ZoneInfo

import requests

from ..settings import NWS_API_BASE_URL

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT_SECONDS = 8.0
NWS_HEADERS = {
    'Accept': 'application/geo+json',
    'User-Agent': 'prediction-market-weather/0.1 (operator console; contact local repo)',
}


@dataclass(frozen=True)
class StationMetadata:
    city_key: str
    city_id: str
    city_name: str
    station_id: str
    timezone_name: str
    latitude: float
    longitude: float
    settlement_verified: bool = True
    note: str = ''


STATION_REGISTRY: dict[str, StationMetadata] = {
    'miami': StationMetadata(
        city_key='miami',
        city_id='mia',
        city_name='Miami',
        station_id='KMIA',
        timezone_name='America/New_York',
        latitude=25.7959,
        longitude=-80.2870,
    ),
    'dc': StationMetadata(
        city_key='dc',
        city_id='dc',
        city_name='DC',
        station_id='KDCA',
        timezone_name='America/New_York',
        latitude=38.8521,
        longitude=-77.0377,
    ),
    'phl': StationMetadata(
        city_key='phl',
        city_id='phl',
        city_name='Philly',
        station_id='KPHL',
        timezone_name='America/New_York',
        latitude=39.8721,
        longitude=-75.2411,
    ),
    'bos': StationMetadata(
        city_key='bos',
        city_id='bos',
        city_name='Boston',
        station_id='KBOS',
        timezone_name='America/New_York',
        latitude=42.3601,
        longitude=-71.0105,
    ),
    'nyc': StationMetadata(
        city_key='nyc',
        city_id='nyc',
        city_name='NYC',
        station_id='KNYC',
        timezone_name='America/New_York',
        latitude=40.7789,
        longitude=-73.9692,
        note='Central Park observation station.',
    ),
    'chi': StationMetadata(
        city_key='chi',
        city_id='chi',
        city_name='Chicago',
        station_id='KMDW',
        timezone_name='America/Chicago',
        latitude=41.7868,
        longitude=-87.7522,
    ),
    'lax': StationMetadata(
        city_key='lax',
        city_id='lax',
        city_name='Los Angeles',
        station_id='KLAX',
        timezone_name='America/Los_Angeles',
        latitude=33.9425,
        longitude=-118.4081,
    ),
    'hou': StationMetadata(
        city_key='hou',
        city_id='hou',
        city_name='Houston',
        station_id='KHOU',
        timezone_name='America/Chicago',
        latitude=29.6454,
        longitude=-95.2789,
        settlement_verified=False,
        note='KHOU is assumed; verify current Kalshi settlement rules before trading.',
    ),
    'sea': StationMetadata(
        city_key='sea',
        city_id='sea',
        city_name='Seattle',
        station_id='KSEA',
        timezone_name='America/Los_Angeles',
        latitude=47.4447,
        longitude=-122.3136,
    ),
    'atl': StationMetadata(
        city_key='atl',
        city_id='atl',
        city_name='Atlanta',
        station_id='KATL',
        timezone_name='America/New_York',
        latitude=33.6367,
        longitude=-84.4281,
    ),
    'den': StationMetadata(
        city_key='den',
        city_id='den',
        city_name='Denver',
        station_id='KDEN',
        timezone_name='America/Denver',
        latitude=39.8561,
        longitude=-104.6737,
    ),
    'dal': StationMetadata(
        city_key='dal',
        city_id='dal',
        city_name='Dallas',
        station_id='KDAL',
        timezone_name='America/Chicago',
        latitude=32.8471,
        longitude=-96.8517,
    ),
}

STATION_IDS = {city_key: meta.station_id for city_key, meta in STATION_REGISTRY.items()}
STATION_COORDS = {
    meta.station_id: (meta.latitude, meta.longitude)
    for meta in STATION_REGISTRY.values()
}
STATION_TIMEZONES = {
    meta.station_id: meta.timezone_name
    for meta in STATION_REGISTRY.values()
}
CITY_DISPLAY_NAMES = {
    meta.city_key: meta.city_name
    for meta in STATION_REGISTRY.values()
}
CITY_KEY_TO_CITY_ID = {
    meta.city_key: meta.city_id
    for meta in STATION_REGISTRY.values()
}
CITY_ID_TO_CITY_KEY = {
    meta.city_id: meta.city_key
    for meta in STATION_REGISTRY.values()
}
STATION_ID_TO_CITY_KEY = {
    meta.station_id: meta.city_key
    for meta in STATION_REGISTRY.values()
}
CITY_KEY_ALIASES = {
    'miami': 'miami',
    'mia': 'miami',
    'washington': 'dc',
    'washington dc': 'dc',
    'washington, dc': 'dc',
    'dc': 'dc',
    'dca': 'dc',
    'phl': 'phl',
    'philadelphia': 'phl',
    'philly': 'phl',
    'bos': 'bos',
    'boston': 'bos',
    'ny': 'nyc',
    'nyc': 'nyc',
    'new york': 'nyc',
    'new york city': 'nyc',
    'chi': 'chi',
    'chicago': 'chi',
    'lax': 'lax',
    'la': 'lax',
    'los angeles': 'lax',
    'hou': 'hou',
    'houston': 'hou',
    'sea': 'sea',
    'seattle': 'sea',
    'atl': 'atl',
    'atlanta': 'atl',
    'den': 'den',
    'denver': 'den',
    'dal': 'dal',
    'dallas': 'dal',
}


def _now_utc() -> datetime:
    return datetime.now(UTC)


def _fahrenheit_from_celsius(value_c: float | None) -> float | None:
    if value_c in (None, ''):
        return None
    return float(value_c) * 9.0 / 5.0 + 32.0


def _isoformat_z(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace('+00:00', 'Z')


def _normalize_station_id(station_id: str) -> str:
    normalized = str(station_id or '').strip().upper()
    if normalized in STATION_ID_TO_CITY_KEY:
        return normalized

    city_key = CITY_KEY_ALIASES.get(normalized.lower())
    if city_key:
        return STATION_IDS[city_key]

    raise KeyError(f'Unknown station identifier or city key: {station_id}')


def normalize_city_key(city_key: str) -> str:
    normalized = str(city_key or '').strip().lower()
    if normalized in STATION_REGISTRY:
        return normalized
    if normalized in CITY_KEY_ALIASES:
        return CITY_KEY_ALIASES[normalized]
    if normalized in CITY_ID_TO_CITY_KEY:
        return CITY_ID_TO_CITY_KEY[normalized]
    raise KeyError(f'Unknown city key: {city_key}')


def station_metadata_for_city(city_key: str) -> StationMetadata:
    return STATION_REGISTRY[normalize_city_key(city_key)]


def station_metadata_for_station(station_id: str) -> StationMetadata:
    resolved_station_id = _normalize_station_id(station_id)
    return STATION_REGISTRY[STATION_ID_TO_CITY_KEY[resolved_station_id]]


def _local_day_bounds(date_local: date, timezone_name: str) -> tuple[datetime, datetime]:
    zone = ZoneInfo(timezone_name)
    start_local = datetime.combine(date_local, time(0, 0), tzinfo=zone)
    end_local = start_local + timedelta(days=1) - timedelta(seconds=1)
    return start_local.astimezone(UTC), end_local.astimezone(UTC)


def fetch_station_observations(station_id: str, date_local: date) -> list[dict]:
    """
    Fetch all hourly observations for a station on a given local date.
    """
    metadata = station_metadata_for_station(station_id)
    start_utc, end_utc = _local_day_bounds(date_local, metadata.timezone_name)

    try:
        response = requests.get(
            f'{NWS_API_BASE_URL}/stations/{metadata.station_id}/observations',
            headers=NWS_HEADERS,
            params={
                'start': _isoformat_z(start_utc),
                'end': _isoformat_z(end_utc),
                'limit': 200,
            },
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        payload = response.json()
    except (requests.RequestException, ValueError) as exc:
        logger.warning(
            'Failed to fetch station observations for %s on %s: %s',
            metadata.station_id,
            date_local,
            exc,
        )
        return []

    rows: list[dict[str, Any]] = []
    for feature in payload.get('features') or []:
        properties = feature.get('properties') or {}
        timestamp = properties.get('timestamp')
        if not timestamp:
            continue
        rows.append(
            {
                'time_utc': _isoformat_z(datetime.fromisoformat(timestamp.replace('Z', '+00:00'))),
                'temp_f': _fahrenheit_from_celsius((properties.get('temperature') or {}).get('value')),
                'conditions': properties.get('textDescription') or '',
            }
        )

    return sorted(rows, key=lambda row: row['time_utc'])


def fetch_station_daily_high(station_id: str, date_local: date) -> float | None:
    """
    Return the observed daily high temp in Fahrenheit for the station/date.
    """
    observations = fetch_station_observations(station_id, date_local)
    temps = [float(row['temp_f']) for row in observations if row.get('temp_f') is not None]
    return max(temps) if temps else None


def _parse_forecast_periods(periods: list[dict[str, Any]], timezone_name: str) -> dict[str, Any]:
    zone = ZoneInfo(timezone_name)
    today_local = _now_utc().astimezone(zone).date()
    today_high_f: int | None = None
    tomorrow_high_f: int | None = None
    sky_condition = 'unknown'

    for period in periods:
        if sky_condition == 'unknown':
            sky_condition = str(period.get('shortForecast') or 'unknown')

        if not period.get('isDaytime'):
            continue

        raw_start = period.get('startTime')
        if not raw_start:
            continue
        start_local = datetime.fromisoformat(raw_start.replace('Z', '+00:00')).astimezone(zone)
        temp_value = period.get('temperature')
        if temp_value in (None, ''):
            continue

        day = start_local.date()
        if day == today_local and today_high_f is None:
            today_high_f = int(round(float(temp_value)))
            sky_condition = str(period.get('shortForecast') or sky_condition)
        elif day > today_local and tomorrow_high_f is None:
            tomorrow_high_f = int(round(float(temp_value)))
            if today_high_f is not None:
                break

    return {
        'today_high_f': today_high_f,
        'tomorrow_high_f': tomorrow_high_f,
        'sky_condition': sky_condition,
    }


def fetch_station_forecast(station_id: str) -> dict:
    """
    Get the point forecast for a station using verified settlement coordinates.
    """
    metadata = station_metadata_for_station(station_id)
    lat, lon = STATION_COORDS[metadata.station_id]

    try:
        points_response = requests.get(
            f'{NWS_API_BASE_URL}/points/{lat},{lon}',
            headers=NWS_HEADERS,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        points_response.raise_for_status()
        points_payload = points_response.json()
        forecast_url = ((points_payload.get('properties') or {}).get('forecast') or '').strip()
        if not forecast_url:
            raise ValueError(f'Missing forecast URL for station {metadata.station_id}')

        forecast_response = requests.get(
            forecast_url,
            headers=NWS_HEADERS,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        forecast_response.raise_for_status()
        forecast_payload = forecast_response.json()
    except (requests.RequestException, ValueError) as exc:
        logger.warning('Failed to fetch forecast for %s: %s', metadata.station_id, exc)
        return {
            'today_high_f': None,
            'tomorrow_high_f': None,
            'sky_condition': 'unknown',
        }

    periods = ((forecast_payload.get('properties') or {}).get('periods') or [])
    if not isinstance(periods, list):
        periods = []
    return _parse_forecast_periods(periods, metadata.timezone_name)


def fetch_morning_validation(station_id: str) -> dict:
    """
    Compare the station forecast against observations collected so far today.
    """
    metadata = station_metadata_for_station(station_id)
    zone = ZoneInfo(metadata.timezone_name)
    now_local = _now_utc().astimezone(zone)
    today_local = now_local.date()

    forecast = fetch_station_forecast(metadata.station_id)
    observations = fetch_station_observations(metadata.station_id, today_local)
    temps = [float(row['temp_f']) for row in observations if row.get('temp_f') is not None]
    observed_max = max(temps) if temps else None
    obs_count = len(temps)
    forecast_high = forecast.get('today_high_f')

    if observed_max is None:
        return {
            'forecast_high_f': forecast_high,
            'observed_max_so_far_f': None,
            'obs_count': 0,
            'forecast_confidence': 'unknown',
            'note': 'No ASOS observations are available yet for today.',
        }

    if forecast_high is None:
        return {
            'forecast_high_f': None,
            'observed_max_so_far_f': round(observed_max, 1),
            'obs_count': obs_count,
            'forecast_confidence': 'unknown',
            'note': 'Observed temperatures are available, but the NWS point forecast is unavailable.',
        }

    discrepancy = observed_max - float(forecast_high)
    if discrepancy > 6.0:
        confidence = 'low'
        note = (
            f'Observed max already exceeds the NWS high by {discrepancy:.1f}F; '
            'the forecast is likely using the wrong station or a stale grid.'
        )
    elif abs(discrepancy) <= 3.0 or (obs_count >= 6 and now_local.hour >= 10):
        confidence = 'high'
        if abs(discrepancy) <= 3.0:
            note = f'Observed temps are tracking within {abs(discrepancy):.1f}F of forecast.'
        else:
            note = f'{obs_count} observations collected and it is past 10 AM local.'
    elif abs(discrepancy) <= 6.0 or obs_count < 6:
        confidence = 'medium'
        note = (
            f'Observed max is {abs(discrepancy):.1f}F away from forecast with '
            f'{obs_count} observations so far.'
        )
    else:
        confidence = 'low'
        note = f'Observed trend is {abs(discrepancy):.1f}F away from the forecasted high.'

    return {
        'forecast_high_f': int(round(float(forecast_high))),
        'observed_max_so_far_f': round(observed_max, 1),
        'obs_count': obs_count,
        'forecast_confidence': confidence,
        'note': note,
    }
