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


def _fetch_scan_context(
    target_date: date | None,
) -> tuple[date, datetime, dict[str, list[WeatherMarket]], dict[str, dict]]:
    resolved_date = target_date or _default_scan_date()
    scan_time_utc = datetime.now(UTC).replace(microsecond=0)

    try:
        kalshi_markets = KalshiClient(timeout_seconds=8.0).fetch_open_weather_markets()
    except KalshiClientError as exc:
        logger.warning('Kalshi market fetch failed during morning scan: %s', exc)
        kalshi_markets = []

    markets_by_city = _group_markets_by_city(kalshi_markets, resolved_date)
    validations_by_city = {
        city_key: fetch_morning_validation(metadata.station_id)
        for city_key, metadata in STATION_REGISTRY.items()
    }
    return resolved_date, scan_time_utc, markets_by_city, validations_by_city


def _format_degree_value(value: float | int | None) -> str:
    if value is None:
        return 'n/a'
    numeric = float(value)
    if numeric.is_integer():
        return str(int(numeric))
    return f'{numeric:.1f}'.rstrip('0').rstrip('.')


def _coldmath_confidence(gap_f: float) -> str:
    if gap_f >= 12.0:
        return 'very_high'
    if gap_f >= 8.0:
        return 'high'
    if gap_f >= 5.0:
        return 'medium'
    return 'low'


def _coldmath_market_play(
    *,
    city_key: str,
    station_id: str,
    forecast_high_f: float | int | None,
    market: WeatherMarket,
    min_yes_price: float,
    min_forecast_gap_f: float,
) -> dict | None:
    if forecast_high_f is None or market.yes_ask is None or market.operator is None or market.threshold_low_f is None:
        return None

    forecast_value = float(forecast_high_f)
    yes_ask = float(market.yes_ask)
    bet_side: str | None = None
    bet_price: float | None = None
    threshold_f: float | None = None
    forecast_gap_f = 0.0
    label: str | None = None
    thesis: str | None = None
    contract_type = 'threshold'

    if market.operator == '<=':
        threshold_f = float(market.threshold_low_f)
        forecast_gap_f = threshold_f - forecast_value
        if forecast_gap_f < min_forecast_gap_f:
            return None
        bet_side = 'YES'
        bet_price = yes_ask
        label = f'below {_format_degree_value(threshold_f)}°F'
        thesis = (
            f'NWS={_format_temp(forecast_value)}, threshold={_format_temp(threshold_f)}, '
            f'{_format_degree_value(forecast_gap_f)}°F margin - near-certain YES'
        )
    elif market.operator == '>=':
        threshold_f = float(market.threshold_low_f)
        forecast_gap_f = forecast_value - threshold_f
        if forecast_gap_f < min_forecast_gap_f:
            return None
        bet_side = 'YES'
        bet_price = yes_ask
        label = f'above {_format_degree_value(threshold_f)}°F'
        thesis = (
            f'NWS={_format_temp(forecast_value)}, threshold={_format_temp(threshold_f)}, '
            f'{_format_degree_value(forecast_gap_f)}°F margin - near-certain YES'
        )
    elif market.operator == 'between' and market.threshold_high_f is not None:
        lower = float(market.threshold_low_f)
        upper = float(market.threshold_high_f)
        if lower <= forecast_value < upper:
            return None
        contract_type = 'bucket'
        if forecast_value < lower:
            threshold_f = lower
            forecast_gap_f = lower - forecast_value
            distance_phrase = 'below bucket range'
        else:
            threshold_f = upper
            forecast_gap_f = forecast_value - upper
            distance_phrase = 'above bucket range'
        if forecast_gap_f < min_forecast_gap_f:
            return None
        bet_side = 'NO'
        bet_price = round(1.0 - yes_ask, 4)
        label = f'not {market.label}F'
        thesis = (
            f'NWS={_format_temp(forecast_value)}, bucket={market.label}, '
            f'{_format_degree_value(forecast_gap_f)}°F {distance_phrase} - near-certain NO'
        )
    else:
        return None

    if bet_price is None or bet_side is None or threshold_f is None:
        return None
    if bet_price < min_yes_price or bet_price > 0.99:
        return None

    forecast_gap_f = round(forecast_gap_f, 1)
    recommendation = 'BUY' if bet_price >= 0.88 and forecast_gap_f >= 10.0 else 'WATCH'
    return {
        'city': CITY_DISPLAY_NAMES[city_key],
        'city_key': city_key,
        'station_id': station_id,
        'ticker': market.ticker,
        'contract_type': contract_type,
        'label': label,
        'bet_side': bet_side,
        'bet_price': round(bet_price, 4),
        'yes_ask': round(yes_ask, 4),
        'no_equivalent': round(1.0 - yes_ask, 4),
        'forecast_f': round(forecast_value, 1),
        'threshold_f': round(threshold_f, 1),
        'forecast_gap_f': forecast_gap_f,
        'confidence': _coldmath_confidence(forecast_gap_f),
        'win_per_contract': round(1.0 - bet_price, 2),
        'recommendation': recommendation,
        'score': round(forecast_gap_f * bet_price, 4),
        'thesis': thesis,
    }


def _scan_coldmath_from_context(
    *,
    markets_by_city: dict[str, list[WeatherMarket]],
    validations_by_city: dict[str, dict],
    min_yes_price: float,
    min_forecast_gap_f: float,
) -> list[dict]:
    plays: list[dict] = []
    for city_key, metadata in STATION_REGISTRY.items():
        forecast_high = validations_by_city.get(city_key, {}).get('forecast_high_f')
        for market in markets_by_city.get(city_key, []):
            candidate = _coldmath_market_play(
                city_key=city_key,
                station_id=metadata.station_id,
                forecast_high_f=forecast_high,
                market=market,
                min_yes_price=min_yes_price,
                min_forecast_gap_f=min_forecast_gap_f,
            )
            if candidate is not None:
                plays.append(candidate)

    return sorted(
        plays,
        key=lambda row: (
            row.get('forecast_gap_f') if row.get('forecast_gap_f') is not None else float('-inf'),
            row.get('score') if row.get('score') is not None else float('-inf'),
            1 if row.get('contract_type') == 'threshold' else 0,
            row.get('ticker') or '',
        ),
        reverse=True,
    )


def scan_coldmath_plays(
    target_date: date | None = None,
    min_yes_price: float = 0.85,
    min_forecast_gap_f: float = 8.0,
    db_path=None,
) -> list[dict]:
    """
    Scan for high-confidence weather contracts where the forecast is comfortably
    on the winning side of the contract threshold or bucket range.
    """
    del db_path  # The scan reads live APIs directly and does not write to DuckDB.

    _, _, markets_by_city, validations_by_city = _fetch_scan_context(target_date)
    return _scan_coldmath_from_context(
        markets_by_city=markets_by_city,
        validations_by_city=validations_by_city,
        min_yes_price=min_yes_price,
        min_forecast_gap_f=min_forecast_gap_f,
    )


def run_morning_scan(target_date: date | None = None, db_path: str | None = None) -> dict:
    """
    Run the morning weather market scan for the configured settlement stations.
    """
    del db_path  # The morning scan reads live APIs directly and does not write to DuckDB.

    resolved_date, scan_time_utc, markets_by_city, validations_by_city = _fetch_scan_context(target_date)

    cities: dict[str, dict] = {}
    for city_key, metadata in STATION_REGISTRY.items():
        validation = validations_by_city[city_key]
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
        'coldmath_plays': _scan_coldmath_from_context(
            markets_by_city=markets_by_city,
            validations_by_city=validations_by_city,
            min_yes_price=0.85,
            min_forecast_gap_f=8.0,
        ),
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


def _format_coldmath_contract_line(play: dict) -> list[str]:
    side = str(play.get('bet_side') or 'YES').upper()
    price = _format_price_cents(play.get('bet_price'))
    profit_cents = int(round(float(play.get('win_per_contract') or 0.0) * 100))
    icon = '✅' if play.get('recommendation') == 'BUY' else '👀'
    lines = [
        f"{icon} {play['city']} {play['label']} - {side} @ {price} (+{profit_cents}¢/contract)",
    ]

    forecast_gap = _format_degree_value(play.get('forecast_gap_f'))
    threshold = _format_degree_value(play.get('threshold_f'))
    if play.get('contract_type') == 'bucket':
        lines.append(
            f"   NWS={_format_temp(play.get('forecast_f'))} at {play.get('station_id')} - "
            f"{forecast_gap}°F from bucket edge {threshold}°F"
        )
    else:
        direction = 'below' if str(play.get('label') or '').startswith('below') else 'above'
        lines.append(
            f"   NWS={_format_temp(play.get('forecast_f'))} at {play.get('station_id')} - "
            f"{forecast_gap}°F {direction} threshold"
        )

    deployed = round(float(play.get('bet_price') or 0.0) * 100.0, 2)
    won = round(float(play.get('win_per_contract') or 0.0) * 100.0, 2)
    roi = round((won / deployed) * 100.0, 1) if deployed > 0 else 0.0
    lines.append(
        f'   100 contracts = ${deployed:.2f} deployed -> win ${won:.2f} (+{roi:.0f}%)'
    )
    return lines


def format_scan_report(scan_results: dict, include_all: bool = False) -> str:
    """
    Format morning scan results as a Telegram-ready plain-text report.
    """
    cities = scan_results.get('cities', {})
    coldmath_plays = list(scan_results.get('coldmath_plays') or [])
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

    lines.append('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━')
    lines.append('🎯 COLDMATH LAYER - Near-Certain Plays')
    lines.append('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━')
    lines.append('These bets are designed to WIN at high confidence, small return per contract.')
    lines.append('Build win rate. Scale up over time.')
    lines.append('')
    if coldmath_plays:
        for play in coldmath_plays:
            lines.extend(_format_coldmath_contract_line(play))
            lines.append('')
    else:
        lines.append('No ColdMath plays found today - all forecast gaps are below 8°F.')
        lines.append('')

    lines.append('Auto-bet fires at 10 AM PDT.')
    lines.append('Budget: $10 edge + $20 ColdMath = $30 total')
    while lines and lines[-1] == '':
        lines.pop()
    lines.append('')
    lines.append('Ready to place? Reply with cities to bet or SKIP ALL.')
    return '\n'.join(lines)
