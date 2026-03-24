from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
import math
import re
from typing import Any

from ..forecast.asos import CITY_DISPLAY_NAMES, CITY_ID_TO_CITY_KEY
from ..parse.contract_parser import parse_temperature_contract

_MONTHS = {
    'JAN': 1,
    'FEB': 2,
    'MAR': 3,
    'APR': 4,
    'MAY': 5,
    'JUN': 6,
    'JUL': 7,
    'AUG': 8,
    'SEP': 9,
    'OCT': 10,
    'NOV': 11,
    'DEC': 12,
}

SERIES_PREFIX_TO_CITY_KEY: dict[str, str] = {
    'KXHIGHPHIL': 'phl',
    'KXHIGHMIA': 'miami',
    'KXHIGHCHI': 'chi',
    'KXHIGHLAX': 'lax',
    'KXHIGHNYC': 'nyc',
    'KXHIGHNY': 'nyc',
    'KXHIGHDEN': 'den',
    'KXHIGHTDC': 'dc',
    'KXHIGHDC': 'dc',
    'KXHIGHTBOS': 'bos',
    'KXHIGHBOS': 'bos',
    'KXHIGHTHOU': 'hou',
    'KXHIGHHOU': 'hou',
    'KXHIGHTSEA': 'sea',
    'KXHIGHSEA': 'sea',
    'KXHIGHTATL': 'atl',
    'KXHIGHATL': 'atl',
    'KXHIGHTDAL': 'dal',
    'KXHIGHDAL': 'dal',
}


@dataclass(frozen=True)
class WeatherMarket:
    ticker: str
    title: str
    city_key: str
    market_date_local: date | None
    operator: str | None
    threshold_low_f: float | None
    threshold_high_f: float | None
    yes_ask: float | None
    label: str
    bucket_code: str | None
    raw_market: dict[str, Any]


def _normalize_price(value: Any) -> float | None:
    if value in (None, ''):
        return None
    price = float(value)
    return price / 100.0 if price > 1.0 else price


def _city_key_from_ticker(ticker: str) -> str | None:
    for prefix in sorted(SERIES_PREFIX_TO_CITY_KEY, key=len, reverse=True):
        if ticker.startswith(prefix):
            return SERIES_PREFIX_TO_CITY_KEY[prefix]
    return None


def _market_date_from_ticker(ticker: str) -> date | None:
    match = re.search(r'-(\d{2})([A-Z]{3})(\d{2})-', ticker)
    if not match:
        return None
    month = _MONTHS.get(match.group(2).upper())
    if month is None:
        return None
    return date(2000 + int(match.group(1)), month, int(match.group(3)))


def _bucket_code_from_ticker(ticker: str) -> str | None:
    parts = ticker.split('-')
    if len(parts) < 3:
        return None
    return parts[-1]


def _infer_bucket_from_code(bucket_code: str | None) -> tuple[str | None, float | None, float | None]:
    if not bucket_code:
        return None, None, None

    code = bucket_code[0].upper()
    try:
        value = float(bucket_code[1:])
    except ValueError:
        return None, None, None

    if code == 'B':
        low = math.floor(value)
        return 'between', float(low), float(low + 1)
    if code == 'T':
        return '<=', value, None
    if code in {'A', 'H', 'G'}:
        return '>=', value, None
    return None, None, None


def format_bucket_label(
    operator: str | None,
    threshold_low_f: float | None,
    threshold_high_f: float | None,
) -> str:
    if operator == 'between' and threshold_low_f is not None and threshold_high_f is not None:
        return f'{int(threshold_low_f)}° to {int(threshold_high_f)}°'
    if operator == '<=' and threshold_low_f is not None:
        return f'<{int(threshold_low_f)}°F'
    if operator == '>=' and threshold_low_f is not None:
        return f'>{int(threshold_low_f)}°F'
    return 'Unknown bucket'


def market_bucket_center(market: WeatherMarket | None) -> float | None:
    if market is None or market.operator is None or market.threshold_low_f is None:
        return None
    if market.operator == 'between' and market.threshold_high_f is not None:
        return round((market.threshold_low_f + market.threshold_high_f) / 2.0, 1)
    return float(market.threshold_low_f)


def parse_weather_market(market: dict[str, Any]) -> WeatherMarket | None:
    ticker = str(market.get('ticker') or '').strip()
    if not ticker:
        return None

    title = str(market.get('title') or ticker)
    parsed = parse_temperature_contract(ticker, title)
    city_key = _city_key_from_ticker(ticker)
    if city_key is None and parsed.city_id:
        city_key = CITY_ID_TO_CITY_KEY.get(parsed.city_id)
    if city_key is None:
        return None

    operator = parsed.operator
    threshold_low_f = parsed.threshold_low_f
    threshold_high_f = parsed.threshold_high_f
    bucket_code = _bucket_code_from_ticker(ticker)

    if operator is None or threshold_low_f is None:
        operator, threshold_low_f, threshold_high_f = _infer_bucket_from_code(bucket_code)

    market_date_local = parsed.market_date_local or _market_date_from_ticker(ticker)

    return WeatherMarket(
        ticker=ticker,
        title=title,
        city_key=city_key,
        market_date_local=market_date_local,
        operator=operator,
        threshold_low_f=threshold_low_f,
        threshold_high_f=threshold_high_f,
        yes_ask=_normalize_price(market.get('yes_ask')),
        label=format_bucket_label(operator, threshold_low_f, threshold_high_f),
        bucket_code=bucket_code,
        raw_market=market,
    )


def estimate_model_probability(
    forecast_high_f: float | int | None,
    confidence: str,
    market: WeatherMarket,
) -> float | None:
    if forecast_high_f is None or market.operator is None or market.threshold_low_f is None:
        return None

    adjustment = {
        'high': 0.0,
        'medium': -0.10,
        'low': -0.20,
        'unknown': -0.25,
    }.get(confidence, -0.15)

    forecast_value = float(forecast_high_f)

    if market.operator == 'between' and market.threshold_high_f is not None:
        center = (market.threshold_low_f + market.threshold_high_f) / 2.0
        distance = abs(forecast_value - center)
        if distance <= 0.5:
            base = 0.65
        elif distance <= 1.0:
            base = 0.50
        elif distance <= 2.0:
            base = 0.35
        elif distance <= 3.0:
            base = 0.22
        else:
            base = 0.12
    elif market.operator == '<=':
        margin = market.threshold_low_f - forecast_value
        if margin >= 2.0:
            base = 0.75
        elif margin >= 1.0:
            base = 0.65
        elif margin >= 0.0:
            base = 0.50
        elif margin >= -1.0:
            base = 0.35
        else:
            base = 0.20
    elif market.operator == '>=':
        margin = forecast_value - market.threshold_low_f
        if margin >= 2.0:
            base = 0.75
        elif margin >= 1.0:
            base = 0.65
        elif margin >= 0.0:
            base = 0.50
        elif margin >= -1.0:
            base = 0.35
        else:
            base = 0.20
    else:
        return None

    return round(min(0.95, max(0.05, base + adjustment)), 4)


def market_fit_distance(forecast_high_f: float | int | None, market: WeatherMarket) -> float:
    if forecast_high_f is None or market.operator is None or market.threshold_low_f is None:
        return float('inf')

    forecast_value = float(forecast_high_f)
    if market.operator == 'between' and market.threshold_high_f is not None:
        center = (market.threshold_low_f + market.threshold_high_f) / 2.0
        return abs(forecast_value - center)
    if market.operator == '<=':
        return max(0.0, forecast_value - market.threshold_low_f)
    if market.operator == '>=':
        return max(0.0, market.threshold_low_f - forecast_value)
    return float('inf')


def choose_best_market(
    markets: list[WeatherMarket],
    forecast_high_f: float | int | None,
    confidence: str,
) -> tuple[WeatherMarket | None, float | None]:
    ranked: list[tuple[float, float, float, WeatherMarket]] = []
    for market in markets:
        probability = estimate_model_probability(forecast_high_f, confidence, market)
        if probability is None:
            continue
        distance = market_fit_distance(forecast_high_f, market)
        ask = market.yes_ask if market.yes_ask is not None else 1.0
        ranked.append((probability, -distance, -ask, market))

    if not ranked:
        return None, None

    probability, _, _, market = max(ranked, key=lambda row: row[:3])
    return market, probability


def find_adjacent_market(
    markets: list[WeatherMarket],
    best_market: WeatherMarket | None,
    forecast_high_f: float | int | None,
) -> WeatherMarket | None:
    if best_market is None or forecast_high_f is None:
        return None

    forecast_value = float(forecast_high_f)
    boundaries: list[float] = []
    if best_market.operator == 'between' and best_market.threshold_high_f is not None:
        boundaries.extend([best_market.threshold_low_f, best_market.threshold_high_f])
    elif best_market.threshold_low_f is not None:
        boundaries.append(best_market.threshold_low_f)

    for boundary in boundaries:
        if abs(forecast_value - boundary) > 0.25:
            continue
        for market in markets:
            if market.ticker == best_market.ticker:
                continue
            if market.operator == 'between' and market.threshold_high_f is not None:
                if boundary in {market.threshold_low_f, market.threshold_high_f}:
                    return market
            elif market.threshold_low_f == boundary:
                return market
    return None


def outcome_for_observed_high(observed_high_f: float | None, market: WeatherMarket) -> bool | None:
    if observed_high_f is None or market.operator is None or market.threshold_low_f is None:
        return None
    observed = float(observed_high_f)
    if market.operator == 'between' and market.threshold_high_f is not None:
        return market.threshold_low_f <= observed < market.threshold_high_f
    if market.operator == '<=':
        return observed < market.threshold_low_f
    if market.operator == '>=':
        return observed >= market.threshold_low_f
    return None


def display_name_for_city(city_key: str) -> str:
    return CITY_DISPLAY_NAMES.get(city_key, city_key.upper())
