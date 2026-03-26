from __future__ import annotations

from datetime import date, datetime, UTC
import hashlib
import json
import logging
import re
from typing import Any

from ..build.bootstrap import bootstrap
from ..db import connect
from ..forecast.asos import STATION_IDS
from ..ingest.kalshi_live import KalshiClient
from ..live.live_orders import fetch_live_orders, settle_live_order as persist_live_order_settlement
from ..pipeline._markets import (
    display_name_for_city,
    market_bucket_center,
    outcome_for_observed_high,
    parse_weather_market,
)

logger = logging.getLogger(__name__)

_MARCH23_TARGET_DATE = date(2026, 3, 23)
_FINALIZED_STATUSES = {'finalized', 'settled'}
_TICKER_PATTERN = re.compile(r'^(?P<series>.+)-(?P<date>\d{2}[A-Z]{3}\d{2})-(?P<bucket>[A-Z].+)$')


def _normalize_label(value: Any) -> str | None:
    if value in (None, ''):
        return None
    return str(value).strip().lower()


def _coerce_float(value: Any) -> float | None:
    if value in (None, ''):
        return None
    return float(value)


def _parse_notes(raw_value: Any) -> dict[str, Any]:
    if raw_value in (None, ''):
        return {}
    if isinstance(raw_value, dict):
        return raw_value
    try:
        parsed = json.loads(str(raw_value))
    except (TypeError, ValueError):
        return {'raw_note': str(raw_value)}
    return parsed if isinstance(parsed, dict) else {'raw_note': parsed}


def _split_weather_ticker(ticker: str) -> tuple[str | None, str | None, str | None]:
    match = _TICKER_PATTERN.match(str(ticker or '').strip())
    if not match:
        return None, None, None
    return match.group('series'), match.group('date'), match.group('bucket')


def _extract_market_payload(payload: dict[str, Any], *, ticker: str | None = None) -> dict[str, Any]:
    market = payload.get('market')
    if isinstance(market, dict):
        return market

    markets = payload.get('markets')
    if isinstance(markets, list):
        if ticker is not None:
            for candidate in markets:
                if str(candidate.get('ticker') or '') == ticker:
                    return candidate
        if len(markets) == 1 and isinstance(markets[0], dict):
            return markets[0]

    return payload


def _estimate_actual_high_from_market_ticker(ticker: str) -> float | None:
    market = parse_weather_market({'ticker': ticker, 'title': ticker})
    if market is None or market.operator is None or market.threshold_low_f is None:
        return None
    if market.operator == 'between' and market.threshold_high_f is not None:
        return round((market.threshold_low_f + market.threshold_high_f) / 2.0, 1)
    if market.operator == '<=':
        return round(market.threshold_low_f - 0.5, 1)
    if market.operator == '>=':
        return round(market.threshold_low_f + 0.5, 1)
    return None


def _fetch_calibration_row(live_order_id: str, db_path=None) -> dict[str, Any] | None:
    bootstrap(db_path=db_path)
    con = connect(read_only=True, db_path=db_path)
    try:
        cursor = con.execute(
            '''
            select
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
            from ops.calibration_log
            where live_order_id = ?
            ''',
            [live_order_id],
        )
        row = cursor.fetchone()
        if row is None:
            return None
        columns = [column[0] for column in cursor.description]
        return dict(zip(columns, row))
    finally:
        con.close()


def _favorite_market_diagnostics(
    *,
    actual_high_f: float | None,
    notes: dict[str, Any],
) -> tuple[bool | None, float | None]:
    market_favorite_ticker = notes.get('market_favorite_ticker')
    if not market_favorite_ticker:
        return None, None

    favorite_market = parse_weather_market(
        {'ticker': market_favorite_ticker, 'title': market_favorite_ticker}
    )
    if favorite_market is None:
        return None, None

    market_was_right = None
    if actual_high_f is not None:
        favorite_outcome = outcome_for_observed_high(actual_high_f, favorite_market)
        if favorite_outcome is not None:
            market_was_right = bool(favorite_outcome)

    market_favorite_center_f = _coerce_float(notes.get('market_favorite_center_f'))
    if market_favorite_center_f is None:
        market_favorite_center_f = market_bucket_center(favorite_market)
    if actual_high_f is None or market_favorite_center_f is None:
        return market_was_right, None
    return market_was_right, round(abs(actual_high_f - market_favorite_center_f), 2)


def _update_calibration_log(
    *,
    live_order_id: str,
    actual_high_f: float | None,
    outcome: str,
    realized_pnl_dollars: float,
    settlement_note: str,
    db_path=None,
) -> dict[str, Any] | None:
    existing = _fetch_calibration_row(live_order_id, db_path=db_path)
    if existing is None:
        return None

    notes = _parse_notes(existing.get('notes'))
    notes['settlement_note'] = settlement_note
    notes['settlement_source'] = 'kalshi_api'

    our_forecast_f = _coerce_float(existing.get('our_forecast_f'))
    forecast_error_f = None
    if actual_high_f is not None and our_forecast_f is not None:
        forecast_error_f = round(actual_high_f - our_forecast_f, 2)

    market_was_right, market_favorite_error_f = _favorite_market_diagnostics(
        actual_high_f=actual_high_f,
        notes=notes,
    )

    con = connect(db_path=db_path)
    try:
        con.execute(
            '''
            update ops.calibration_log
            set actual_high_f = ?,
                outcome = ?,
                forecast_error_f = ?,
                market_was_right = ?,
                edge_realized = ?,
                notes = ?
            where live_order_id = ?
            ''',
            [
                actual_high_f,
                outcome,
                forecast_error_f,
                market_was_right,
                realized_pnl_dollars,
                json.dumps(notes, sort_keys=True),
                live_order_id,
            ],
        )
    finally:
        con.close()

    return {
        'our_forecast_f': our_forecast_f,
        'forecast_confidence': str(existing.get('forecast_confidence') or 'unknown'),
        'forecast_error_f': forecast_error_f,
        'our_model_abs_error_f': round(abs(forecast_error_f), 2)
        if forecast_error_f is not None
        else None,
        'market_was_right': market_was_right,
        'market_favorite_error_f': market_favorite_error_f,
        'market_ask_price': _coerce_float(existing.get('market_ask_price')),
        'bucket_center_f': _coerce_float(existing.get('bucket_center_f')),
        'station_id': str(existing.get('station_id') or ''),
    }


def fetch_market_result(
    ticker: str,
    *,
    client: KalshiClient | None = None,
) -> dict[str, Any]:
    """Returns {'status': str, 'result': str|None, 'title': str}."""

    kalshi_client = client or KalshiClient()
    payload = kalshi_client._request_json('GET', f'/markets/{ticker}')
    market = _extract_market_payload(payload, ticker=ticker)
    return {
        'status': str(market.get('status') or ''),
        'result': _normalize_label(market.get('result')),
        'title': str(market.get('title') or market.get('subtitle') or ticker),
    }


def fetch_actual_high_from_kalshi(
    series_ticker: str,
    date_label: str,
    *,
    client: KalshiClient | None = None,
) -> float | None:
    """
    Queries all markets in a series for a date and finds the YES-settling one.
    Returns best estimate of actual official high in °F.
    date_label: e.g. '26MAR23'
    Returns None if markets not yet finalized.
    """

    kalshi_client = client or KalshiClient()
    payload = kalshi_client._request_json(
        'GET',
        '/markets',
        params={'series_ticker': series_ticker, 'limit': 50},
    )
    markets = payload.get('markets') or []
    relevant_markets = [
        market
        for market in markets
        if str(market.get('ticker') or '').startswith(f'{series_ticker}-{date_label}-')
    ]
    if not relevant_markets:
        return None

    yes_market = next(
        (
            market for market in relevant_markets
            if _normalize_label(market.get('result')) == 'yes'
        ),
        None,
    )
    if yes_market is not None:
        return _estimate_actual_high_from_market_ticker(str(yes_market.get('ticker') or ''))

    all_finalized = all(
        _normalize_label(market.get('status')) in _FINALIZED_STATUSES
        and _normalize_label(market.get('result')) in {'yes', 'no'}
        for market in relevant_markets
    )
    if not all_finalized:
        return None
    return None


def settle_live_order(
    order: dict[str, Any],
    db_path,
    *,
    client: KalshiClient | None = None,
) -> dict[str, Any]:
    """
    Given a live_order row, fetch Kalshi result and update DB.
    Returns settlement summary dict.
    """

    ticker = str(order.get('ticker') or '')
    market = parse_weather_market({'ticker': ticker, 'title': ticker})
    city_key = market.city_key if market is not None else None
    station_id = STATION_IDS.get(city_key, '') if city_key is not None else ''
    market_result = fetch_market_result(ticker, client=client)
    status = _normalize_label(market_result.get('status')) or ''
    result = _normalize_label(market_result.get('result'))

    summary: dict[str, Any] = {
        'live_order_id': str(order.get('live_order_id') or ''),
        'kalshi_order_id': str(order.get('kalshi_order_id') or ''),
        'ticker': ticker,
        'city_key': city_key,
        'city_name': display_name_for_city(city_key) if city_key else ticker,
        'station_id': station_id,
        'bucket_code': market.bucket_code if market is not None else None,
        'bucket_label': market.label if market is not None else ticker,
        'kalshi_status': status,
        'kalshi_result': result,
        'title': market_result.get('title') or ticker,
        'official_high_f': None,
        'actual_high_f': None,
        'outcome': None,
        'realized_pnl_dollars': None,
        'settled': False,
        'note': '',
        'forecast_confidence': 'unknown',
        'forecast_error_f': None,
        'our_model_abs_error_f': None,
        'market_was_right': None,
        'market_favorite_error_f': None,
        'market_ask_price': None,
    }

    calibration = _fetch_calibration_row(str(order.get('live_order_id') or ''), db_path=db_path)
    if calibration is not None and calibration.get('station_id'):
        summary['station_id'] = str(calibration['station_id'])

    if status not in _FINALIZED_STATUSES or result not in {'yes', 'no'}:
        summary['note'] = 'Kalshi market not finalized yet.'
        return summary

    series_ticker, date_label, _ = _split_weather_ticker(ticker)
    actual_high_f = None
    if series_ticker and date_label:
        actual_high_f = fetch_actual_high_from_kalshi(
            series_ticker,
            date_label,
            client=client,
        )
    if actual_high_f is None and result == 'yes':
        actual_high_f = _estimate_actual_high_from_market_ticker(ticker)

    fill_count = int(order.get('fill_count') or 0)
    taker_cost_dollars = round(float(order.get('taker_cost_dollars') or 0.0), 2)
    realized_pnl_dollars = (
        round(fill_count * 1.0 - taker_cost_dollars, 2)
        if result == 'yes'
        else round(-taker_cost_dollars, 2)
    )
    settlement_note_parts = [f'Kalshi finalized {result.upper()}']
    if actual_high_f is not None:
        settlement_note_parts.append(f'official high ~{actual_high_f:.1f}F')
    settlement_note = '; '.join(settlement_note_parts)

    persist_live_order_settlement(
        str(order.get('kalshi_order_id') or ''),
        result,
        settlement_note=settlement_note,
        db_path=db_path,
    )
    updated_order = fetch_live_orders(
        db_path=db_path,
        live_order_id=str(order.get('live_order_id') or ''),
    )[0]
    calibration_summary = _update_calibration_log(
        live_order_id=str(order.get('live_order_id') or ''),
        actual_high_f=actual_high_f,
        outcome=result,
        realized_pnl_dollars=realized_pnl_dollars,
        settlement_note=settlement_note,
        db_path=db_path,
    )

    summary.update(
        {
            'official_high_f': actual_high_f,
            'actual_high_f': actual_high_f,
            'outcome': result,
            'realized_pnl_dollars': float(updated_order.get('realized_pnl_dollars') or 0.0),
            'settled': True,
            'note': settlement_note,
        }
    )
    if calibration_summary is not None:
        summary.update(calibration_summary)
    return summary


def fix_march23_settlements(
    db_path,
    *,
    client: KalshiClient | None = None,
) -> dict[str, Any]:
    bootstrap(db_path=db_path)
    march23_orders: list[dict[str, Any]] = []
    for order in fetch_live_orders(db_path=db_path):
        if int(order.get('fill_count') or 0) <= 0:
            continue
        market = parse_weather_market({'ticker': order.get('ticker'), 'title': order.get('ticker')})
        if market is None or market.market_date_local != _MARCH23_TARGET_DATE:
            continue
        march23_orders.append(order)

    summaries: list[dict[str, Any]] = []
    kalshi_client = client or KalshiClient()
    for order in sorted(march23_orders, key=lambda row: (str(row.get('ticker') or ''), str(row.get('live_order_id') or ''))):
        summaries.append(settle_live_order(order, db_path=db_path, client=kalshi_client))

    settled_orders = [summary for summary in summaries if summary.get('settled')]
    total_realized_pnl = round(
        sum(float(summary.get('realized_pnl_dollars') or 0.0) for summary in settled_orders),
        2,
    )
    return {
        'target_date': _MARCH23_TARGET_DATE.isoformat(),
        'orders': summaries,
        'order_count': len(summaries),
        'settled_count': len(settled_orders),
        'total_realized_pnl': total_realized_pnl,
    }


def _fetch_open_paper_bets(db_path=None, *, tickers: list[str] | None = None) -> list[dict[str, Any]]:
    """Fetch open paper bets, optionally filtered to specific tickers."""
    bootstrap(db_path=db_path)
    con = connect(read_only=True, db_path=db_path)
    try:
        if tickers:
            placeholders = ', '.join(['?'] * len(tickers))
            rows = con.execute(
                f"SELECT * FROM ops.paper_bets WHERE status = 'open' AND market_ticker IN ({placeholders})",
                tickers,
            ).fetchall()
        else:
            rows = con.execute("SELECT * FROM ops.paper_bets WHERE status = 'open'").fetchall()
        columns = [col[0] for col in con.description]
        return [dict(zip(columns, row)) for row in rows]
    finally:
        con.close()


def settle_paper_bet(
    paper_bet: dict[str, Any],
    db_path=None,
    *,
    client: KalshiClient | None = None,
) -> dict[str, Any]:
    """Settle a single paper bet using Kalshi API to determine the market outcome."""
    ticker = str(paper_bet.get('market_ticker') or '')
    side = str(paper_bet.get('side') or 'YES').lower()
    paper_bet_id = str(paper_bet.get('paper_bet_id') or '')
    quantity = float(paper_bet.get('quantity') or 0)
    limit_price = float(paper_bet.get('limit_price') or 0)

    summary: dict[str, Any] = {
        'paper_bet_id': paper_bet_id,
        'ticker': ticker,
        'side': side,
        'settled': False,
        'outcome': None,
        'realized_pnl': None,
        'note': '',
    }

    market_result = fetch_market_result(ticker, client=client)
    status = _normalize_label(market_result.get('status')) or ''
    result = _normalize_label(market_result.get('result'))

    if status not in _FINALIZED_STATUSES or result not in {'yes', 'no'}:
        summary['note'] = 'Kalshi market not finalized yet.'
        return summary

    # Determine actual high from Kalshi sibling markets
    series_ticker, date_label, _ = _split_weather_ticker(ticker)
    actual_high_f = None
    if series_ticker and date_label:
        actual_high_f = fetch_actual_high_from_kalshi(series_ticker, date_label, client=client)
    if actual_high_f is None and result == 'yes':
        actual_high_f = _estimate_actual_high_from_market_ticker(ticker)

    # Compute PnL: if we bought YES and market = yes, we win $1/contract minus cost
    bet_won = (result == side)
    cost = round(quantity * limit_price, 4)
    realized_pnl = round(quantity * 1.0 - cost, 4) if bet_won else round(-cost, 4)

    now_utc = datetime.now(UTC).replace(tzinfo=None)
    settlement_note = f'Kalshi finalized {result.upper()}'
    if actual_high_f is not None:
        settlement_note += f'; official high ~{actual_high_f:.1f}F'

    # Update paper bet
    con = connect(db_path=db_path)
    try:
        con.execute(
            '''
            UPDATE ops.paper_bets
            SET status = 'closed',
                outcome_label = ?,
                realized_pnl = ?,
                closed_at_utc = ?
            WHERE paper_bet_id = ?
            ''',
            [result, realized_pnl, now_utc, paper_bet_id],
        )
    finally:
        con.close()

    # Insert review
    review_id = 'review_' + hashlib.md5(
        f'{paper_bet_id}-{now_utc.isoformat()}'.encode()
    ).hexdigest()[:12]
    market = parse_weather_market({'ticker': ticker, 'title': ticker})
    bucket_code = market.bucket_code if market else ticker.split('-')[-1]
    lesson = (
        f'Market settled {result.upper()}. '
        f'Actual high ~{actual_high_f:.1f}F vs bucket {bucket_code}. '
        f'Bet {"won" if bet_won else "lost"} — PnL ${realized_pnl:+.4f}.'
        if actual_high_f is not None
        else f'Market settled {result.upper()}. Bet {"won" if bet_won else "lost"} — PnL ${realized_pnl:+.4f}.'
    )

    con = connect(db_path=db_path)
    try:
        con.execute(
            '''
            INSERT INTO ops.paper_bet_reviews (
                review_id, paper_bet_id, proposal_id, strategy_id,
                reviewed_at_utc, kalshi_outcome_label, realized_pnl,
                lesson_summary, review_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            [
                review_id,
                paper_bet_id,
                None,
                paper_bet.get('strategy_id'),
                now_utc,
                result,
                realized_pnl,
                lesson,
                json.dumps({
                    'actual_high_f': actual_high_f,
                    'bucket_code': bucket_code,
                    'bet_won': bet_won,
                    'settlement_note': settlement_note,
                }),
            ],
        )
    finally:
        con.close()

    # Update calibration_log if there is a matching paper bet entry
    _update_calibration_log(
        live_order_id=paper_bet_id,
        actual_high_f=actual_high_f,
        outcome=result,
        realized_pnl_dollars=realized_pnl,
        settlement_note=settlement_note,
        db_path=db_path,
    )

    logger.info('Settled paper bet %s on %s: %s PnL=$%.4f', paper_bet_id, ticker, result.upper(), realized_pnl)

    summary.update({
        'settled': True,
        'outcome': result,
        'realized_pnl': realized_pnl,
        'actual_high_f': actual_high_f,
        'note': settlement_note,
        'bet_won': bet_won,
    })
    return summary


def settle_open_paper_bets(
    db_path=None,
    *,
    tickers: list[str] | None = None,
    client: KalshiClient | None = None,
) -> list[dict[str, Any]]:
    """Settle all open paper bets, optionally filtered to specific tickers.

    Called after settling live orders to also close out paper bets on the same markets.
    """
    paper_bets = _fetch_open_paper_bets(db_path=db_path, tickers=tickers)
    if not paper_bets:
        return []

    kalshi_client = client or KalshiClient()
    summaries = []
    for pb in sorted(paper_bets, key=lambda r: str(r.get('market_ticker') or '')):
        summaries.append(settle_paper_bet(pb, db_path=db_path, client=kalshi_client))
    return summaries
