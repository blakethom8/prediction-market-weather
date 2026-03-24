from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
import json
import logging
import math
from pathlib import Path
import time as time_module
from typing import Any
from zoneinfo import ZoneInfo

from ..build.bootstrap import bootstrap
from ..db import connect
from ..forecast.asos import CITY_DISPLAY_NAMES, normalize_city_key
from ..ingest.kalshi_live import KalshiClient
from ..live.live_orders import seed_live_order
from ..pipeline._markets import market_bucket_center, parse_weather_market
from ..settings import ROOT

logger = logging.getLogger(__name__)

AUTO_BET_CONFIG = {
    'max_daily_spend_dollars': 30.00,
    'max_daily_edge_dollars': 5.00,
    'max_per_bet_dollars': 5.00,
    'min_edge': 0.20,
    'min_forecast_confidence': 'high',
    'require_asos_obs': True,
    'max_obs_forecast_divergence_f': 5.0,
    'skip_cities': ['hou'],
    'kill_switch_file': 'data/.auto_bet_disabled',
}

COLDMATH_CONFIG = {
    'min_yes_price': 0.88,
    'min_forecast_gap_f': 10.0,
    'target_per_bet_dollars': 3.00,
    'max_per_bet_dollars': 5.00,
    'max_daily_coldmath_dollars': 20.00,
    'skip_cities': ['hou'],
}

AUTO_BET_TIMEZONE = ZoneInfo('America/Los_Angeles')


def _kill_switch_path() -> Path:
    return ROOT / AUTO_BET_CONFIG['kill_switch_file']


def _normalize_price(value: Any) -> float | None:
    if value in (None, ''):
        return None
    numeric = float(value)
    return numeric / 100.0 if numeric > 1.0 else numeric


def _normalize_count(value: Any) -> int | None:
    if value in (None, ''):
        return None
    return int(round(float(value)))


def _coerce_float(value: Any) -> float | None:
    if value in (None, ''):
        return None
    return float(value)


def _normalize_status(value: Any) -> str:
    label = str(value or '').strip().lower()
    if label in {'', 'filled', 'executed', 'complete', 'completed'}:
        return 'executed'
    if label in {'open', 'resting', 'posted', 'partially_filled'}:
        return 'resting'
    if label in {'pending', 'queued', 'received'}:
        return 'pending'
    if label in {'canceled', 'cancelled'}:
        return 'cancelled'
    return label or 'executed'


def _parse_timestamp(value: Any) -> datetime | None:
    if value in (None, ''):
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value
        return value.astimezone(UTC).replace(tzinfo=None)
    text = str(value).strip()
    if not text:
        return None
    if text.endswith('Z'):
        text = text[:-1] + '+00:00'
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        return parsed
    return parsed.astimezone(UTC).replace(tzinfo=None)


def _coalesce(*values: Any) -> Any:
    for value in values:
        if value not in (None, ''):
            return value
    return None


def _scan_budget_date(scan_results: dict | None = None, candidate: dict | None = None) -> date:
    raw_value = None
    if candidate is not None:
        raw_value = candidate.get('budget_date_local') or candidate.get('scan_date')
    if raw_value is None and scan_results is not None:
        raw_value = scan_results.get('scan_date')
    if isinstance(raw_value, date):
        return raw_value
    if raw_value:
        return date.fromisoformat(str(raw_value))
    return datetime.now(AUTO_BET_TIMEZONE).date()


def _candidate_divergence(candidate: dict) -> float | None:
    divergence = candidate.get('obs_forecast_divergence_f')
    if divergence not in (None, ''):
        return float(divergence)
    observed = candidate.get('observed_max_so_far_f')
    forecast = candidate.get('forecast_high_f')
    if observed in (None, '') or forecast in (None, ''):
        return None
    return abs(float(observed) - float(forecast))


def _candidate_city_key(candidate: dict) -> str:
    raw_city = candidate.get('city_key') or candidate.get('city_id') or candidate.get('city_name')
    return normalize_city_key(str(raw_city or ''))


def _candidate_label(candidate: dict) -> str:
    return str(
        candidate.get('label')
        or candidate.get('best_bucket_label')
        or candidate.get('recommendation_reason')
        or 'Unknown bucket'
    )


def _candidate_ticker(candidate: dict) -> str:
    return str(candidate.get('best_bucket') or candidate.get('ticker') or '').strip()


def _candidate_strategy(candidate: dict) -> str:
    return str(candidate.get('bet_strategy') or 'edge').strip().lower() or 'edge'


def _candidate_trade_side(candidate: dict) -> str:
    return str(candidate.get('bet_side') or 'YES').strip().lower() or 'yes'


def _candidate_trade_price(candidate: dict) -> float | None:
    value = candidate.get('bet_price')
    if value in (None, ''):
        value = candidate.get('best_bucket_ask')
    return _normalize_price(value)


def _enrich_scan_candidate(city_key: str, row: dict, scan_results: dict, db_path: str | None = None) -> dict:
    candidate = dict(row)
    candidate.setdefault('city_key', city_key)
    candidate.setdefault('city_name', CITY_DISPLAY_NAMES.get(city_key, city_key.upper()))
    candidate['bet_strategy'] = 'edge'
    candidate['scan_date'] = scan_results.get('scan_date')
    candidate['scan_time_utc'] = scan_results.get('scan_time_utc')
    if db_path is not None:
        candidate['_db_path'] = db_path
    return candidate


def iter_scan_candidates(scan_results: dict, db_path: str | None = None) -> list[dict]:
    candidates = [
        _enrich_scan_candidate(city_key, row, scan_results, db_path=db_path)
        for city_key, row in scan_results.get('cities', {}).items()
        if row.get('best_bucket')
    ]
    return sorted(
        candidates,
        key=lambda row: (
            row.get('edge') is not None,
            row.get('edge') if row.get('edge') is not None else float('-inf'),
            row.get('city_name') or '',
        ),
        reverse=True,
    )


def _enrich_coldmath_candidate(play: dict, scan_results: dict, db_path: str | None = None) -> dict:
    candidate = dict(play)
    city_key = str(candidate.get('city_key') or '')
    candidate.setdefault('city_name', candidate.get('city') or CITY_DISPLAY_NAMES.get(city_key, city_key.upper()))
    candidate['bet_strategy'] = 'coldmath'
    candidate['scan_date'] = scan_results.get('scan_date')
    candidate['scan_time_utc'] = scan_results.get('scan_time_utc')
    if db_path is not None:
        candidate['_db_path'] = db_path
    return candidate


def iter_coldmath_candidates(scan_results: dict, db_path: str | None = None) -> list[dict]:
    plays = scan_results.get('coldmath_plays')
    return sorted(
        [
            _enrich_coldmath_candidate(play, scan_results, db_path=db_path)
            for play in list(plays or [])
        ],
        key=lambda row: (
            row.get('forecast_gap_f') if row.get('forecast_gap_f') is not None else float('-inf'),
            row.get('bet_price') if row.get('bet_price') is not None else float('-inf'),
            row.get('city_name') or '',
        ),
        reverse=True,
    )


def get_daily_spend(date_local: date, db_path=None, *, bet_strategy: str | None = None) -> float:
    """Sum taker_cost_dollars from ops.live_orders placed today."""

    bootstrap(db_path=db_path)
    start_utc = datetime.combine(date_local, time(0, 0), tzinfo=AUTO_BET_TIMEZONE).astimezone(UTC).replace(tzinfo=None)
    end_utc = (datetime.combine(date_local, time(0, 0), tzinfo=AUTO_BET_TIMEZONE) + timedelta(days=1)).astimezone(UTC).replace(tzinfo=None)

    con = connect(read_only=True, db_path=db_path)
    try:
        if bet_strategy is None:
            row = con.execute(
                '''
                select coalesce(sum(coalesce(taker_cost_dollars, 0)), 0)
                from ops.live_orders
                where created_at_utc >= ?
                  and created_at_utc < ?
                ''',
                [start_utc, end_utc],
            ).fetchone()
        else:
            row = con.execute(
                '''
                select coalesce(sum(coalesce(lo.taker_cost_dollars, 0)), 0)
                from ops.live_orders lo
                join ops.calibration_log cl
                  on cl.live_order_id = lo.live_order_id
                where lo.created_at_utc >= ?
                  and lo.created_at_utc < ?
                  and cl.bet_strategy = ?
                ''',
                [start_utc, end_utc, str(bet_strategy)],
            ).fetchone()
    finally:
        con.close()
    return round(float(row[0] or 0.0), 2)


def get_remaining_daily_budget(
    date_local: date,
    db_path=None,
    *,
    max_daily_spend_dollars: float | None = None,
    bet_strategy: str | None = None,
) -> float:
    """Returns max(0, budget_cap - get_daily_spend(date_local))."""

    budget_cap = float(
        AUTO_BET_CONFIG['max_daily_spend_dollars']
        if max_daily_spend_dollars is None
        else max_daily_spend_dollars
    )
    remaining = budget_cap - get_daily_spend(date_local, db_path=db_path, bet_strategy=bet_strategy)
    return round(max(0.0, remaining), 2)


def compute_bet_size(ask_price: float, budget_remaining: float) -> tuple[int, float]:
    """
    Given ask price and remaining budget, compute contract count and total cost.
    - Never spend more than min(budget_remaining, AUTO_BET_CONFIG['max_per_bet_dollars'])
    - Round down to whole contracts
    - Minimum 1 contract, or return (0, 0.0) if can't afford even 1
    Returns (contract_count, total_cost)
    """

    normalized_ask = _normalize_price(ask_price)
    if normalized_ask in (None, 0) or normalized_ask < 0:
        return 0, 0.0

    spend_cap = min(float(budget_remaining), float(AUTO_BET_CONFIG['max_per_bet_dollars']))
    if spend_cap <= 0:
        return 0, 0.0

    contracts = int(math.floor((spend_cap + 1e-9) / float(normalized_ask)))
    if contracts < 1:
        return 0, 0.0
    return contracts, round(contracts * float(normalized_ask), 2)


def compute_coldmath_bet_size(ask_price: float, budget_remaining: float) -> tuple[int, float]:
    """
    Target a small $2-$5 deployment per ColdMath bet, centered on $3.
    """

    normalized_ask = _normalize_price(ask_price)
    if normalized_ask in (None, 0) or normalized_ask < 0:
        return 0, 0.0

    spend_cap = min(
        float(budget_remaining),
        float(COLDMATH_CONFIG['max_per_bet_dollars']),
        float(COLDMATH_CONFIG['target_per_bet_dollars']),
    )
    if spend_cap <= 0:
        return 0, 0.0

    contracts = int(math.floor((spend_cap + 1e-9) / float(normalized_ask)))
    if contracts < 1:
        return 0, 0.0
    return contracts, round(contracts * float(normalized_ask), 2)


def should_auto_bet(candidate: dict, *, edge_budget_dollars: float | None = None) -> tuple[bool, str]:
    """
    Evaluate whether a candidate from morning_scan meets all guardrails.
    Returns (should_bet, reason_string).
    Checks:
    - kill switch file
    - daily budget remaining
    - edge >= min_edge
    - forecast_confidence == 'high'
    - obs_count >= 3
    - obs divergence within threshold
    - city not in skip_cities
    """

    if _kill_switch_path().exists():
        return False, 'auto betting disabled via kill switch'

    candidate_db_path = candidate.get('_db_path')
    budget_date = _scan_budget_date(candidate=candidate)
    total_budget_remaining = get_remaining_daily_budget(budget_date, db_path=candidate_db_path)
    if total_budget_remaining <= 0:
        return False, 'daily budget exhausted'
    strategy_budget_remaining = get_remaining_daily_budget(
        budget_date,
        db_path=candidate_db_path,
        max_daily_spend_dollars=(
            AUTO_BET_CONFIG['max_daily_edge_dollars']
            if edge_budget_dollars is None
            else edge_budget_dollars
        ),
        bet_strategy='edge',
    )
    if strategy_budget_remaining <= 0:
        return False, 'edge budget exhausted'

    try:
        city_key = _candidate_city_key(candidate)
    except KeyError:
        return False, 'unknown city key'

    if city_key in AUTO_BET_CONFIG['skip_cities'] or candidate.get('station_verified') is False:
        return False, 'station unverified'

    ask_price = _normalize_price(candidate.get('best_bucket_ask'))
    if ask_price in (None, 0) or ask_price < 0:
        return False, 'missing ask price'

    edge = candidate.get('edge')
    if edge in (None, ''):
        return False, 'missing edge estimate'
    if float(edge) < float(AUTO_BET_CONFIG['min_edge']):
        return False, f"edge {int(round(float(edge) * 100))}¢ is below {int(round(AUTO_BET_CONFIG['min_edge'] * 100))}¢ threshold"

    forecast_confidence = str(candidate.get('forecast_confidence') or 'unknown').lower()
    if forecast_confidence != AUTO_BET_CONFIG['min_forecast_confidence']:
        return False, f'forecast confidence is {forecast_confidence}'

    if AUTO_BET_CONFIG['require_asos_obs']:
        obs_count = int(candidate.get('obs_count') or 0)
        if obs_count < 3:
            return False, f'only {obs_count} ASOS observations today'

    divergence = _candidate_divergence(candidate)
    if divergence is not None and divergence > float(AUTO_BET_CONFIG['max_obs_forecast_divergence_f']):
        return False, f'ASOS divergence {divergence:.1f}F exceeds {AUTO_BET_CONFIG["max_obs_forecast_divergence_f"]:.1f}F'

    recommendation = str(candidate.get('recommendation') or '').upper()
    if recommendation and recommendation != 'BUY':
        reason = str(candidate.get('recommendation_reason') or '').strip()
        return False, reason or f'scan recommendation is {recommendation}'

    contracts, _ = compute_bet_size(float(ask_price), strategy_budget_remaining)
    if contracts < 1:
        return False, 'budget cannot afford one contract'

    return True, 'eligible'


def should_auto_bet_coldmath(candidate: dict, *, coldmath_budget_dollars: float | None = None) -> tuple[bool, str]:
    if _kill_switch_path().exists():
        return False, 'auto betting disabled via kill switch'

    candidate_db_path = candidate.get('_db_path')
    budget_date = _scan_budget_date(candidate=candidate)
    total_budget_remaining = get_remaining_daily_budget(budget_date, db_path=candidate_db_path)
    if total_budget_remaining <= 0:
        return False, 'daily budget exhausted'
    strategy_budget_remaining = get_remaining_daily_budget(
        budget_date,
        db_path=candidate_db_path,
        max_daily_spend_dollars=(
            COLDMATH_CONFIG['max_daily_coldmath_dollars']
            if coldmath_budget_dollars is None
            else coldmath_budget_dollars
        ),
        bet_strategy='coldmath',
    )
    if strategy_budget_remaining <= 0:
        return False, 'coldmath budget exhausted'

    try:
        city_key = _candidate_city_key(candidate)
    except KeyError:
        return False, 'unknown city key'

    if city_key in COLDMATH_CONFIG['skip_cities'] or candidate.get('station_verified') is False:
        return False, 'station unverified'

    ask_price = _candidate_trade_price(candidate)
    if ask_price in (None, 0) or ask_price < 0:
        return False, 'missing ask price'
    if float(ask_price) < float(COLDMATH_CONFIG['min_yes_price']):
        return (
            False,
            f"price {int(round(float(ask_price) * 100))}¢ is below {int(round(COLDMATH_CONFIG['min_yes_price'] * 100))}¢ threshold",
        )

    forecast_gap_f = _coerce_float(candidate.get('forecast_gap_f'))
    if forecast_gap_f is None:
        return False, 'missing forecast gap'
    if forecast_gap_f < float(COLDMATH_CONFIG['min_forecast_gap_f']):
        return (
            False,
            f'forecast gap {forecast_gap_f:.1f}F is below {COLDMATH_CONFIG["min_forecast_gap_f"]:.1f}F threshold',
        )

    contracts, _ = compute_coldmath_bet_size(float(ask_price), strategy_budget_remaining)
    if contracts < 1:
        return False, 'budget cannot afford one contract'

    return True, 'eligible'


def _extract_order_fields(payload: dict[str, Any], *, fallback_count: int, fallback_price_cents: int) -> dict[str, Any]:
    order = payload.get('order') if isinstance(payload.get('order'), dict) else payload
    status = _normalize_status(order.get('status'))
    initial_count = _normalize_count(
        _coalesce(
            order.get('initial_count'),
            order.get('order_count'),
            order.get('count'),
            order.get('quantity'),
            fallback_count,
        )
    )
    fill_count = _normalize_count(
        _coalesce(
            order.get('fill_count'),
            order.get('filled_count'),
            order.get('count_filled'),
        )
    )
    remaining_count = _normalize_count(
        _coalesce(
            order.get('remaining_count'),
            order.get('resting_count'),
            order.get('pending_count'),
        )
    )
    if fill_count is None:
        fill_count = initial_count if status == 'executed' else 0
    if remaining_count is None and initial_count is not None:
        remaining_count = max(initial_count - fill_count, 0)

    limit_price_cents = _normalize_count(
        _coalesce(
            order.get('limit_price'),
            order.get('price'),
            order.get('yes_price'),
            order.get('no_price'),
            order.get('price_cents'),
            fallback_price_cents,
        )
    ) or fallback_price_cents
    taker_cost_dollars = _coalesce(
        order.get('taker_cost_dollars'),
        order.get('taker_cost'),
        order.get('cost'),
    )
    if taker_cost_dollars in (None, ''):
        taker_cost_dollars = round((fill_count or 0) * limit_price_cents / 100.0, 2)

    return {
        'kalshi_order_id': str(order.get('order_id') or order.get('id') or '').strip(),
        'status': status,
        'initial_count': initial_count or fallback_count,
        'fill_count': fill_count,
        'remaining_count': remaining_count if remaining_count is not None else 0,
        'limit_price_cents': limit_price_cents,
        'taker_cost_dollars': round(float(taker_cost_dollars or 0.0), 2),
        'taker_fees_dollars': float(_coalesce(order.get('taker_fees_dollars'), order.get('taker_fees'), order.get('fees')) or 0.0),
        'updated_at_utc': _parse_timestamp(_coalesce(order.get('updated_at'), order.get('updated_time'))) or datetime.now(UTC).replace(tzinfo=None),
    }


def _upsert_pending_calibration(
    *,
    live_order_id: str,
    candidate: dict,
    market_ask_price: float,
    db_path=None,
) -> None:
    bootstrap(db_path=db_path)
    ticker = _candidate_ticker(candidate)
    if not ticker:
        return
    market = parse_weather_market({'ticker': ticker, 'title': ticker})
    if market is None or market.market_date_local is None:
        return

    notes = {
        'auto_bet': True,
        'bet_side': _candidate_trade_side(candidate),
        'contract_type': candidate.get('contract_type'),
        'city_name': candidate.get('city_name'),
        'label': _candidate_label(candidate),
        'forecast_gap_f': candidate.get('forecast_gap_f'),
        'threshold_f': candidate.get('threshold_f'),
        'validation_note': candidate.get('validation_note'),
        'obs_forecast_divergence_f': candidate.get('obs_forecast_divergence_f'),
        'market_favorite_ticker': candidate.get('market_favorite_bucket'),
        'market_favorite_price': candidate.get('market_favorite_ask'),
        'market_favorite_center_f': candidate.get('market_favorite_center_f'),
        'market_favorite_label': candidate.get('market_favorite_label'),
        'best_bucket_label': candidate.get('best_bucket_label'),
    }

    con = connect(db_path=db_path)
    try:
        con.execute('delete from ops.calibration_log where log_id = ?', [live_order_id])
        con.execute(
            '''
            insert into ops.calibration_log (
                log_id,
                market_date_local,
                city_key,
                station_id,
                ticker,
                bet_strategy,
                live_order_id,
                is_paper_bet,
                our_forecast_f,
                forecast_confidence,
                market_ask_price,
                bucket_center_f,
                actual_high_f,
                outcome,
                forecast_error_f,
                market_was_right,
                edge_realized,
                notes
            ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            [
                live_order_id,
                market.market_date_local,
                _candidate_city_key(candidate),
                candidate.get('station_id'),
                ticker,
                _candidate_strategy(candidate),
                live_order_id,
                False,
                candidate.get('forecast_high_f', candidate.get('forecast_f')),
                candidate.get('forecast_confidence', candidate.get('confidence')),
                market_ask_price,
                market_bucket_center(market),
                None,
                None,
                None,
                None,
                None,
                json.dumps(notes, sort_keys=True),
            ],
        )
    finally:
        con.close()


def _candidate_bucket_code(candidate: dict) -> str | None:
    ticker = _candidate_ticker(candidate)
    if not ticker:
        return None
    return str(candidate.get('best_bucket_code') or ticker.split('-')[-1]).strip() or None


def _place_candidate_bet(
    candidate: dict,
    *,
    db_path=None,
    validator,
    budget_dollars: float,
    bet_size_fn,
) -> dict:
    candidate_with_db = dict(candidate)
    if db_path is not None:
        candidate_with_db['_db_path'] = db_path

    strategy = _candidate_strategy(candidate_with_db)
    budget_kwarg = (
        {'edge_budget_dollars': budget_dollars}
        if strategy == 'edge'
        else {'coldmath_budget_dollars': budget_dollars}
    )
    should_bet, reason = validator(candidate_with_db, **budget_kwarg)
    if not should_bet:
        raise ValueError(f'Auto-bet guardrail blocked order: {reason}')

    budget_date = _scan_budget_date(candidate=candidate_with_db)
    budget_remaining = get_remaining_daily_budget(
        budget_date,
        db_path=db_path,
        max_daily_spend_dollars=budget_dollars,
        bet_strategy=strategy,
    )
    ask_price = _candidate_trade_price(candidate_with_db)
    if ask_price is None:
        raise ValueError('Candidate is missing an ask price.')
    contracts, total_cost = bet_size_fn(ask_price, budget_remaining)
    if contracts < 1:
        raise ValueError('Remaining budget cannot afford one contract.')

    city_key = _candidate_city_key(candidate_with_db)
    ticker = _candidate_ticker(candidate_with_db)
    side = _candidate_trade_side(candidate_with_db)
    import random as _random
    client_order_id = f'auto-{strategy}-{city_key}-{int(time_module.time())}-{_random.randint(1000,9999)}'
    price_cents = int(round(float(ask_price) * 100))
    client = KalshiClient(timeout_seconds=10.0)
    payload = client.place_order(
        ticker=ticker,
        client_order_id=client_order_id,
        count=contracts,
        side=side,
        action='buy',
        order_type='limit',
        price_cents=price_cents,
    )

    order_fields = _extract_order_fields(payload, fallback_count=contracts, fallback_price_cents=price_cents)
    kalshi_order_id = order_fields['kalshi_order_id']
    if not kalshi_order_id:
        raise RuntimeError('Kalshi order response did not include an order_id.')

    live_order_id = seed_live_order(
        kalshi_order_id=kalshi_order_id,
        client_order_id=client_order_id,
        strategy_id=None,
        ticker=ticker,
        action='buy',
        side=side,
        order_type='limit',
        limit_price_cents=order_fields['limit_price_cents'],
        initial_count=order_fields['initial_count'],
        fill_count=order_fields['fill_count'],
        remaining_count=order_fields['remaining_count'],
        status=order_fields['status'],
        taker_cost_dollars=order_fields['taker_cost_dollars'],
        taker_fees_dollars=order_fields['taker_fees_dollars'],
        updated_at_utc=order_fields['updated_at_utc'],
        db_path=db_path,
    )
    _upsert_pending_calibration(
        live_order_id=live_order_id,
        candidate=candidate_with_db,
        market_ask_price=order_fields['limit_price_cents'] / 100.0,
        db_path=db_path,
    )

    return {
        'bet_strategy': strategy,
        'city_key': city_key,
        'city_name': candidate_with_db.get('city_name', CITY_DISPLAY_NAMES.get(city_key, city_key.upper())),
        'ticker': ticker,
        'bucket_code': _candidate_bucket_code(candidate_with_db),
        'bucket_label': _candidate_label(candidate_with_db),
        'edge': candidate_with_db.get('edge'),
        'forecast_gap_f': candidate_with_db.get('forecast_gap_f'),
        'bet_side': side,
        'contracts': contracts,
        'price': round(order_fields['limit_price_cents'] / 100.0, 2),
        'cost': order_fields['taker_cost_dollars'] if order_fields['taker_cost_dollars'] is not None else total_cost,
        'order_id': kalshi_order_id,
        'live_order_id': live_order_id,
        'payout_if_win': float(contracts),
        'status': order_fields['status'],
        'fill_count': order_fields['fill_count'],
    }


def place_auto_bet(candidate: dict, db_path=None, *, edge_budget_dollars: float | None = None) -> dict:
    return _place_candidate_bet(
        candidate,
        db_path=db_path,
        validator=should_auto_bet,
        budget_dollars=(
            AUTO_BET_CONFIG['max_daily_edge_dollars']
            if edge_budget_dollars is None
            else edge_budget_dollars
        ),
        bet_size_fn=compute_bet_size,
    )


def place_coldmath_bet(candidate: dict, db_path=None, *, coldmath_budget_dollars: float | None = None) -> dict:
    return _place_candidate_bet(
        candidate,
        db_path=db_path,
        validator=should_auto_bet_coldmath,
        budget_dollars=(
            COLDMATH_CONFIG['max_daily_coldmath_dollars']
            if coldmath_budget_dollars is None
            else coldmath_budget_dollars
        ),
        bet_size_fn=compute_coldmath_bet_size,
    )


def evaluate_auto_bet_candidates(
    scan_results: dict,
    db_path=None,
    *,
    edge_budget_dollars: float | None = None,
) -> list[dict]:
    evaluations: list[dict] = []
    for candidate in iter_scan_candidates(scan_results, db_path=db_path):
        should_bet, reason = should_auto_bet(
            candidate,
            edge_budget_dollars=(
                AUTO_BET_CONFIG['max_daily_edge_dollars']
                if edge_budget_dollars is None
                else edge_budget_dollars
            ),
        )
        evaluations.append(
            {
                'candidate': candidate,
                'should_bet': should_bet,
                'reason': reason,
            }
        )
    return evaluations


def evaluate_coldmath_auto_bet_candidates(
    scan_results: dict,
    db_path=None,
    *,
    coldmath_budget_dollars: float | None = None,
) -> list[dict]:
    evaluations: list[dict] = []
    for candidate in iter_coldmath_candidates(scan_results, db_path=db_path):
        should_bet, reason = should_auto_bet_coldmath(
            candidate,
            coldmath_budget_dollars=(
                COLDMATH_CONFIG['max_daily_coldmath_dollars']
                if coldmath_budget_dollars is None
                else coldmath_budget_dollars
            ),
        )
        evaluations.append(
            {
                'candidate': candidate,
                'should_bet': should_bet,
                'reason': reason,
            }
        )
    return evaluations


def evaluate_all_auto_bet_candidates(
    scan_results: dict,
    db_path=None,
    *,
    coldmath_budget_dollars: float = 5.00,
) -> list[dict]:
    edge_budget_dollars = max(0.0, float(AUTO_BET_CONFIG['max_daily_spend_dollars']) - float(coldmath_budget_dollars))
    return [
        *evaluate_auto_bet_candidates(
            scan_results,
            db_path=db_path,
            edge_budget_dollars=edge_budget_dollars,
        ),
        *evaluate_coldmath_auto_bet_candidates(
            scan_results,
            db_path=db_path,
            coldmath_budget_dollars=coldmath_budget_dollars,
        ),
    ]


def run_auto_betting_session(scan_results: dict, db_path=None, *, coldmath_budget_dollars: float = 5.00) -> list[dict]:
    """
    Given scan results from run_morning_scan(), place all qualifying bets.
    Split the daily budget between edge and ColdMath candidates.
    Returns list of placed bet dicts.
    """

    placed_bets: list[dict] = []
    budget_date = _scan_budget_date(scan_results=scan_results)
    edge_budget_dollars = max(0.0, float(AUTO_BET_CONFIG['max_daily_spend_dollars']) - float(coldmath_budget_dollars))

    for evaluation in evaluate_auto_bet_candidates(
        scan_results,
        db_path=db_path,
        edge_budget_dollars=edge_budget_dollars,
    ):
        if get_remaining_daily_budget(budget_date, db_path=db_path) <= 0:
            break
        if not evaluation['should_bet']:
            continue
        try:
            placed_bets.append(
                place_auto_bet(
                    evaluation['candidate'],
                    db_path=db_path,
                    edge_budget_dollars=edge_budget_dollars,
                )
            )
        except (ValueError, Exception) as exc:
            if 'order_already_exists' in str(exc) or '409' in str(exc):
                logger.info('Skipping edge bet — duplicate order id (harmless): %s', exc)
            else:
                logger.info('Skipping edge auto-bet after re-check: %s', exc)

    for evaluation in evaluate_coldmath_auto_bet_candidates(
        scan_results,
        db_path=db_path,
        coldmath_budget_dollars=coldmath_budget_dollars,
    ):
        if get_remaining_daily_budget(budget_date, db_path=db_path) <= 0:
            break
        if not evaluation['should_bet']:
            continue
        try:
            placed_bets.append(
                place_coldmath_bet(
                    evaluation['candidate'],
                    db_path=db_path,
                    coldmath_budget_dollars=coldmath_budget_dollars,
                )
            )
        except (ValueError, Exception) as exc:
            if 'order_already_exists' in str(exc) or '409' in str(exc):
                logger.info('Skipping ColdMath bet — duplicate order id (harmless): %s', exc)
            else:
                logger.info('Skipping ColdMath auto-bet after re-check: %s', exc)
    return placed_bets


def _parse_scan_time(scan_results: dict) -> datetime:
    raw_value = str(scan_results.get('scan_time_utc') or '')
    if raw_value.endswith('Z'):
        raw_value = raw_value[:-1] + '+00:00'
    return datetime.fromisoformat(raw_value).astimezone(AUTO_BET_TIMEZONE)


def _format_scan_time(scan_results: dict) -> str:
    local_dt = _parse_scan_time(scan_results)
    hour = local_dt.strftime('%I').lstrip('0') or '0'
    return f'{local_dt.strftime("%B")} {local_dt.day} {hour}:{local_dt.strftime("%M")} {local_dt.strftime("%p")}'


def _format_cents(value: float | None) -> str:
    if value is None:
        return 'n/a'
    return f'{int(round(float(value) * 100))}¢'


def _format_edge(value: float | None) -> str:
    if value is None:
        return 'n/a'
    cents = int(round(float(value) * 100))
    return f'{cents:+d}¢'


def _format_bucket_label(label: str) -> str:
    if '° to ' in label:
        left, right = label.split('° to ', 1)
        return f'{left}-{right}'
    return label


def _short_order_id(order_id: str) -> str:
    return str(order_id or '').split('-')[0][:8]


def _kill_switch_hint() -> str:
    home = Path.home()
    try:
        relative = _kill_switch_path().relative_to(home)
        return f'touch ~/{relative.as_posix()}'
    except ValueError:
        return f'touch {_kill_switch_path()}'


def _next_scan_label(scan_results: dict) -> str:
    scan_dt = _parse_scan_time(scan_results)
    schedule = [
        (8, 'Morning scan'),
        (10, 'East Coast window'),
        (12, 'West Coast window'),
    ]
    for hour, label in schedule:
        if scan_dt.hour < hour:
            meridiem = 'AM' if hour < 12 else 'PM'
            display_hour = hour if hour <= 12 else hour - 12
            return f'{display_hour}:00 {meridiem} PDT ({label})'
    return '8:00 AM PDT (Morning scan)'


def _compact_candidate_reason(candidate: dict, reason: str) -> str:
    if _candidate_strategy(candidate) == 'coldmath':
        if reason == 'station unverified':
            return 'skipped (station unverified)'
        ask_price = _candidate_trade_price(candidate)
        if ask_price is not None and float(ask_price) < float(COLDMATH_CONFIG['min_yes_price']):
            return (
                f"price {int(round(float(ask_price) * 100))}¢ "
                f"(below {int(round(COLDMATH_CONFIG['min_yes_price'] * 100))}¢ threshold)"
            )
        forecast_gap_f = _coerce_float(candidate.get('forecast_gap_f'))
        if forecast_gap_f is not None and forecast_gap_f < float(COLDMATH_CONFIG['min_forecast_gap_f']):
            return (
                f'gap {forecast_gap_f:.1f}F '
                f'(below {COLDMATH_CONFIG["min_forecast_gap_f"]:.1f}F threshold)'
            )
        return reason

    city_key = _candidate_city_key(candidate)
    if reason == 'station unverified':
        return 'skipped (station unverified)'

    edge = _coalesce(candidate.get('edge'), 0.0)
    if float(edge) < float(AUTO_BET_CONFIG['min_edge']):
        return (
            f'edge {int(round(float(edge) * 100))}¢ '
            f'(below {int(round(AUTO_BET_CONFIG["min_edge"] * 100))}¢ threshold)'
        )

    confidence = str(candidate.get('forecast_confidence') or 'unknown').lower()
    divergence = _candidate_divergence(candidate)
    if confidence != AUTO_BET_CONFIG['min_forecast_confidence']:
        if divergence is not None and divergence > float(AUTO_BET_CONFIG['max_obs_forecast_divergence_f']):
            return f'{confidence} confidence (ASOS diverging)'
        return f'{confidence} confidence'

    if reason.startswith('only '):
        return reason
    if reason.startswith('ASOS divergence '):
        return 'ASOS diverging'
    if reason == 'daily budget exhausted':
        return reason
    if city_key in AUTO_BET_CONFIG['skip_cities']:
        return 'skipped (station unverified)'
    return reason


def _strategy_prefix(strategy: str) -> str:
    return '[COLDMATH]' if strategy == 'coldmath' else '[EDGE]'


def _candidate_summary_label(candidate: dict) -> str:
    bucket_code = _candidate_bucket_code(candidate)
    if _candidate_strategy(candidate) == 'coldmath':
        label = _candidate_label(candidate)
        return f"{candidate['city_name']} {label}".strip()
    label = f' {bucket_code}' if bucket_code else ''
    return f"{candidate['city_name']}{label}"


def format_auto_bet_notification(scan_results: dict, placed_bets: list[dict], db_path=None) -> str:
    budget_date = _scan_budget_date(scan_results=scan_results)
    total_spend = get_daily_spend(budget_date, db_path=db_path)
    remaining_budget = get_remaining_daily_budget(budget_date, db_path=db_path)

    lines = [f'🤖 AUTO-BET FIRED — {_format_scan_time(scan_results)}', '']
    for bet in placed_bets:
        strategy = _candidate_strategy(bet)
        lines.append(
            f"✅ {_strategy_prefix(strategy)} {bet['city_name']} {bet.get('bucket_code') or ''} ({_format_bucket_label(str(bet.get('bucket_label') or 'Unknown bucket'))})".rstrip()
        )
        lines.append(
            f"   {int(bet['contracts'])} {str(bet.get('bet_side') or 'yes').upper()} contracts @ {_format_cents(float(bet['price']))} = ${float(bet['cost']):.2f} deployed"
        )
        if strategy == 'coldmath':
            forecast_gap_f = _coerce_float(bet.get('forecast_gap_f'))
            margin_text = f'{forecast_gap_f:.1f}F' if forecast_gap_f is not None else 'n/a'
            lines.append(
                f"   Payout if win: ${float(bet['payout_if_win']):.2f} | Margin: {margin_text}"
            )
        else:
            lines.append(
                f"   Payout if win: ${float(bet['payout_if_win']):.2f} | Edge: {_format_edge(_coerce_float(bet.get('edge')))}"
            )
        lines.append(f"   Order: {_short_order_id(str(bet['order_id']))}")
        lines.append('')

    lines.append(
        f"Total deployed today: ${total_spend:.2f} / ${AUTO_BET_CONFIG['max_daily_spend_dollars']:.2f} budget"
    )
    lines.append(f'Remaining budget: ${remaining_budget:.2f}')
    lines.append('')
    lines.append(f'Kill auto-betting: {_kill_switch_hint()}')
    return '\n'.join(lines).rstrip()


def format_no_auto_bet_notification(scan_results: dict, evaluations: list[dict] | None = None) -> str:
    rows = evaluations if evaluations is not None else evaluate_all_auto_bet_candidates(scan_results)
    lines = [f'📊 MORNING SCAN — {_format_scan_time(scan_results)}', '', 'No auto-bets placed. Candidates:']

    if not rows:
        lines.append('No tradeable weather buckets matched the scan date.')
    else:
        for evaluation in rows[:3]:
            candidate = evaluation['candidate']
            icon = '⏭' if evaluation['reason'] == 'station unverified' else '👀'
            lines.append(
                f"{icon} {_strategy_prefix(_candidate_strategy(candidate))} {_candidate_summary_label(candidate)} — {_compact_candidate_reason(candidate, evaluation['reason'])}"
            )

    lines.append('')
    lines.append(f'Next scan: {_next_scan_label(scan_results)}')
    return '\n'.join(lines).rstrip()
