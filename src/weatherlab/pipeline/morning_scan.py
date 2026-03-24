from __future__ import annotations

from datetime import UTC, date, datetime
import logging

from ..forecast.asos import CITY_DISPLAY_NAMES, STATION_REGISTRY, fetch_morning_validation
from ..ingest.kalshi_live import KalshiClient, KalshiClientError
from ._markets import (
    WeatherMarket,
    choose_best_market,
    find_adjacent_market,
    market_bucket_center,
    parse_weather_market,
)

logger = logging.getLogger(__name__)


def _default_scan_date() -> date:
    return date.today()


def _group_markets_by_city(markets: list[dict], target_date: date) -> dict[str, list[WeatherMarket]]:
    grouped = {city_key: [] for city_key in STATION_REGISTRY}
    for raw_market in markets:
        parsed = parse_weather_market(raw_market)
        if parsed is None or parsed.market_date_local != target_date:
            continue
        grouped.setdefault(parsed.city_key, []).append(parsed)
    return grouped


def _recommendation_for_city(
    *,
    station_verified: bool,
    confidence: str,
    best_market: WeatherMarket | None,
    model_probability: float | None,
    adjacent_market: WeatherMarket | None,
) -> tuple[str, str]:
    if not station_verified:
        return 'SKIP', 'Settlement station is not verified against current contract rules.'
    if best_market is None:
        return 'SKIP', 'No open Kalshi bucket matched this city/date.'
    if best_market.yes_ask is None or model_probability is None:
        return 'SKIP', 'Best-fit bucket is missing an ask price or model probability.'

    edge = model_probability - best_market.yes_ask
    if confidence in {'low', 'unknown'}:
        return 'SKIP', f'Forecast confidence is {confidence}.'
    if adjacent_market is not None:
        return 'WATCH', 'Forecast is on a bucket boundary; track both adjacent outcomes.'
    if edge >= 0.10 and confidence == 'high':
        return 'BUY', 'Positive edge with high-confidence station validation.'
    if edge >= 0.05 and confidence in {'high', 'medium'}:
        return 'WATCH', 'Edge is positive, but the setup is not clean enough for auto-buy.'
    return 'SKIP', 'Edge is too thin after confidence adjustment.'


def _favorite_market(markets: list[WeatherMarket]) -> WeatherMarket | None:
    priced_markets = [market for market in markets if market.yes_ask is not None]
    if not priced_markets:
        return None
    return max(
        priced_markets,
        key=lambda market: (market.yes_ask if market.yes_ask is not None else float('-inf'), market.ticker),
    )


def run_morning_scan(target_date: date | None = None, db_path: str | None = None) -> dict:
    """
    Run the morning weather market scan for the configured settlement stations.
    """
    del db_path  # The morning scan reads live APIs directly and does not write to DuckDB.

    resolved_date = target_date or _default_scan_date()
    scan_time_utc = datetime.now(UTC).replace(microsecond=0)

    try:
        kalshi_markets = KalshiClient(timeout_seconds=8.0).fetch_open_weather_markets()
    except KalshiClientError as exc:
        logger.warning('Kalshi market fetch failed during morning scan: %s', exc)
        kalshi_markets = []

    markets_by_city = _group_markets_by_city(kalshi_markets, resolved_date)

    cities: dict[str, dict] = {}
    for city_key, metadata in STATION_REGISTRY.items():
        validation = fetch_morning_validation(metadata.station_id)
        city_markets = markets_by_city.get(city_key, [])
        best_market, model_probability = choose_best_market(
            city_markets,
            validation.get('forecast_high_f'),
            validation.get('forecast_confidence', 'unknown'),
        )
        adjacent_market = find_adjacent_market(
            city_markets,
            best_market,
            validation.get('forecast_high_f'),
        )
        favorite_market = _favorite_market(city_markets)

        ask = best_market.yes_ask if best_market is not None else None
        edge = None if ask is None or model_probability is None else round(model_probability - ask, 4)
        observed_max = validation.get('observed_max_so_far_f')
        forecast_high = validation.get('forecast_high_f')
        obs_divergence_f = None
        if observed_max is not None and forecast_high is not None:
            obs_divergence_f = round(abs(float(observed_max) - float(forecast_high)), 1)
        recommendation, recommendation_reason = _recommendation_for_city(
            station_verified=metadata.settlement_verified,
            confidence=validation.get('forecast_confidence', 'unknown'),
            best_market=best_market,
            model_probability=model_probability,
            adjacent_market=adjacent_market,
        )

        if not metadata.settlement_verified and metadata.note:
            recommendation_reason = metadata.note
        elif validation.get('note') and recommendation == 'SKIP' and validation.get('forecast_confidence') in {'low', 'unknown'}:
            recommendation_reason = str(validation['note'])

        cities[city_key] = {
            'city_key': city_key,
            'city_name': CITY_DISPLAY_NAMES[city_key],
            'station_id': metadata.station_id,
            'station_verified': metadata.settlement_verified,
            'forecast_high_f': validation.get('forecast_high_f'),
            'forecast_confidence': validation.get('forecast_confidence'),
            'observed_max_so_far_f': validation.get('observed_max_so_far_f'),
            'obs_count': validation.get('obs_count'),
            'obs_forecast_divergence_f': obs_divergence_f,
            'validation_note': validation.get('note'),
            'best_bucket': best_market.ticker if best_market is not None else None,
            'best_bucket_code': best_market.bucket_code if best_market is not None else None,
            'best_bucket_label': best_market.label if best_market is not None else None,
            'best_bucket_center_f': market_bucket_center(best_market),
            'best_bucket_ask': ask,
            'model_probability': model_probability,
            'edge': edge,
            'market_favorite_bucket': favorite_market.ticker if favorite_market is not None else None,
            'market_favorite_bucket_code': favorite_market.bucket_code if favorite_market is not None else None,
            'market_favorite_label': favorite_market.label if favorite_market is not None else None,
            'market_favorite_center_f': market_bucket_center(favorite_market),
            'market_favorite_ask': favorite_market.yes_ask if favorite_market is not None else None,
            'adjacent_bucket': adjacent_market.ticker if adjacent_market is not None else None,
            'adjacent_bucket_label': adjacent_market.label if adjacent_market is not None else None,
            'recommendation': recommendation,
            'recommendation_reason': recommendation_reason,
        }

    top_picks = [
        city_key
        for city_key, row in sorted(
            cities.items(),
            key=lambda item: (
                item[1].get('edge') is not None,
                item[1].get('edge') if item[1].get('edge') is not None else float('-inf'),
            ),
            reverse=True,
        )
        if row.get('recommendation') == 'BUY'
    ]

    return {
        'scan_date': resolved_date.isoformat(),
        'scan_time_utc': scan_time_utc.isoformat().replace('+00:00', 'Z'),
        'cities': cities,
        'top_picks': top_picks,
    }


def _format_date_label(raw_date: str) -> str:
    parsed = date.fromisoformat(raw_date)
    return f'{parsed.strftime("%B")} {parsed.day}'


def _format_temp(value: float | int | None) -> str:
    if value is None:
        return 'n/a'
    if isinstance(value, float) and not value.is_integer():
        return f'{value:.1f}°F'
    return f'{int(round(float(value)))}°F'


def _format_price_cents(value: float | None) -> str:
    if value is None:
        return 'n/a'
    return f'{int(round(value * 100))}¢'


def _format_edge_cents(value: float | None) -> str:
    if value is None:
        return 'n/a'
    cents = int(round(value * 100))
    sign = '+' if cents >= 0 else ''
    return f'{sign}{cents}¢'


def format_scan_report(scan_results: dict, include_all: bool = False) -> str:
    """
    Format morning scan results as a Telegram-ready plain-text report.
    """
    cities = scan_results.get('cities', {})
    buys = [row for row in cities.values() if row.get('recommendation') == 'BUY']
    watches = [row for row in cities.values() if row.get('recommendation') == 'WATCH']
    skips = [row for row in cities.values() if row.get('recommendation') == 'SKIP']

    buys.sort(key=lambda row: row.get('edge') if row.get('edge') is not None else float('-inf'), reverse=True)
    watches.sort(key=lambda row: row.get('edge') if row.get('edge') is not None else float('-inf'), reverse=True)
    skips.sort(key=lambda row: row['city_name'])

    def render_row(icon: str, row: dict) -> list[str]:
        bucket = row.get('best_bucket_code') or 'NO-BUCKET'
        label = row.get('best_bucket_label') or row.get('recommendation_reason') or 'No bucket'
        lines = [
            f"{icon} {row['city_name']} {bucket} ({label}) - ask {_format_price_cents(row.get('best_bucket_ask'))}, edge {_format_edge_cents(row.get('edge'))}",
        ]
        detail = (
            f"   NWS={_format_temp(row.get('forecast_high_f'))} "
            f"({row.get('forecast_confidence', 'unknown')} conf), "
            f"observed max so far: {_format_temp(row.get('observed_max_so_far_f'))}"
        )
        lines.append(detail)
        if row.get('adjacent_bucket_label'):
            lines.append(f"   Adjacent bucket: {row.get('adjacent_bucket_label')}")
        elif row.get('recommendation_reason') and icon == '⏭':
            lines.append(f"   {row['recommendation_reason']}")
        return lines

    lines = [f'🎯 WEATHER BET SCAN - {_format_date_label(scan_results["scan_date"])}', '']

    lines.append('TOP PICKS:')
    if buys:
        for row in buys:
            lines.extend(render_row('✅', row))
            lines.append('')
    else:
        lines.append('No BUY setups passed the confidence and edge filters.')
        lines.append('')

    lines.append('WATCH LIST:')
    if watches:
        for row in watches:
            lines.extend(render_row('👀', row))
            lines.append('')
    else:
        lines.append('No WATCH setups right now.')
        lines.append('')

    if include_all or skips:
        lines.append('SKIP:')
        if skips:
            for row in skips:
                lines.extend(render_row('⏭', row))
                lines.append('')
        else:
            lines.append('No skips.')
            lines.append('')

    while lines and lines[-1] == '':
        lines.pop()
    lines.append('')
    lines.append('Ready to place? Reply with cities to bet or SKIP ALL.')
    return '\n'.join(lines)
