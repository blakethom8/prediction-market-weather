from __future__ import annotations

from collections.abc import Iterable

from .asos import STATION_IDS, fetch_station_forecast, normalize_city_key


def fetch_city_forecast(city_key: str) -> dict:
    resolved_city_key = normalize_city_key(city_key)
    station_id = STATION_IDS[resolved_city_key]
    forecast = fetch_station_forecast(station_id)
    return {
        'city_key': resolved_city_key,
        'station_id': station_id,
        **forecast,
    }


def fetch_all_city_forecasts(city_keys: Iterable[str] | None = None) -> dict[str, dict]:
    target_city_keys = list(city_keys or STATION_IDS.keys())
    return {
        normalize_city_key(city_key): fetch_city_forecast(city_key)
        for city_key in target_city_keys
    }
