from __future__ import annotations

from datetime import date
import json
from pathlib import Path
from typing import Any

from ..build.bootstrap import bootstrap
from ..db import connect
from ..forecast.asos import STATION_IDS, fetch_station_daily_high
from ..live.live_orders import fetch_live_orders, settle_live_order
from ._markets import display_name_for_city, market_bucket_center, outcome_for_observed_high, parse_weather_market


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


def _coerce_float(value: Any) -> float | None:
    if value in (None, ''):
        return None
    return float(value)


def _signed_currency(value: float | None) -> str:
    if value is None:
        return 'n/a'
    if value >= 0:
        return f'+${value:.2f}'
    return f'-${abs(value):.2f}'


def _extract_first_insight(insights_text: str) -> str | None:
    for raw_line in insights_text.splitlines():
        line = raw_line.strip()
        if line.startswith('- '):
            return line[2:]
    return None


def _historical_city_summary(db_path=None) -> dict[str, dict[str, float]]:
    bootstrap(db_path=db_path)
    con = connect(read_only=True, db_path=db_path)
    try:
        rows = con.execute(
            '''
            select
                city_key,
                count(*) as total_bets,
                avg(abs(forecast_error_f)) as avg_abs_error_f
            from ops.calibration_log
            where actual_high_f is not null
            group by city_key
            '''
        ).fetchall()
    finally:
        con.close()
    return {
        row[0]: {
            'total_bets': int(row[1] or 0),
            'avg_abs_error_f': float(row[2] or 0.0),
        }
        for row in rows
    }


def record_bet_outcome(
    live_order_id: str,
    actual_high_f: float,
    station_id: str,
    our_forecast_f: float,
    forecast_confidence: str,
    db_path=None
) -> dict:
    """
    After a bet settles, record the outcome for learning.
    1. Call settle_live_order() with yes/no outcome
    2. Insert into ops.calibration_log (see schema below)
    3. Return summary dict
    """

    bootstrap(db_path=db_path)
    rows = fetch_live_orders(db_path=db_path, live_order_id=live_order_id)
    if not rows:
        raise LookupError(f'Live order not found: {live_order_id}')
    row = rows[0]
    market = parse_weather_market({'ticker': row['ticker'], 'title': row['ticker']})
    if market is None or market.market_date_local is None:
        raise ValueError(f'Unable to parse weather market for {row["ticker"]}')

    outcome_bool = outcome_for_observed_high(float(actual_high_f), market)
    if outcome_bool is None:
        raise ValueError(f'Unable to determine market outcome for {row["ticker"]}')
    outcome_result = 'yes' if outcome_bool else 'no'
    settlement_note = f'{station_id} observed {float(actual_high_f):.1f}F'
    settle_live_order(str(row['kalshi_order_id']), outcome_result, settlement_note=settlement_note, db_path=db_path)

    updated_row = fetch_live_orders(db_path=db_path, live_order_id=live_order_id)[0]
    existing = _fetch_calibration_row(live_order_id, db_path=db_path)
    notes = _parse_notes(existing['notes'] if existing else None)
    notes['settlement_note'] = settlement_note

    market_favorite_ticker = notes.get('market_favorite_ticker')
    market_favorite_center_f = _coerce_float(notes.get('market_favorite_center_f'))
    if market_favorite_center_f is None and market_favorite_ticker:
        favorite_market = parse_weather_market({'ticker': market_favorite_ticker, 'title': market_favorite_ticker})
        market_favorite_center_f = market_bucket_center(favorite_market)
    market_was_right = None
    market_favorite_error_f = None
    if market_favorite_ticker:
        favorite_market = parse_weather_market({'ticker': market_favorite_ticker, 'title': market_favorite_ticker})
        if favorite_market is not None:
            favorite_outcome = outcome_for_observed_high(float(actual_high_f), favorite_market)
            if favorite_outcome is not None:
                market_was_right = bool(favorite_outcome)
        if market_favorite_center_f is not None:
            market_favorite_error_f = round(abs(float(actual_high_f) - market_favorite_center_f), 2)

    forecast_error_f = round(float(actual_high_f) - float(our_forecast_f), 2)
    our_model_abs_error_f = round(abs(forecast_error_f), 2)
    market_ask_price = (
        _coerce_float(existing['market_ask_price'])
        if existing is not None
        else (_coerce_float(updated_row.get('limit_price_cents')) or 0.0) / 100.0
    )
    bucket_center_f = (
        _coerce_float(existing['bucket_center_f'])
        if existing is not None
        else market_bucket_center(market)
    )
    realized_pnl = _coerce_float(updated_row.get('realized_pnl_dollars')) or 0.0
    log_id = str(existing['log_id']) if existing and existing.get('log_id') else live_order_id

    con = connect(db_path=db_path)
    try:
        con.execute('delete from ops.calibration_log where log_id = ?', [log_id])
        con.execute(
            '''
            insert into ops.calibration_log (
                log_id,
                market_date_local,
                city_key,
                station_id,
                ticker,
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
            ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            [
                log_id,
                market.market_date_local,
                market.city_key,
                station_id,
                updated_row['ticker'],
                live_order_id,
                False,
                float(our_forecast_f),
                forecast_confidence,
                market_ask_price,
                bucket_center_f,
                float(actual_high_f),
                outcome_result,
                forecast_error_f,
                market_was_right,
                realized_pnl,
                json.dumps(notes, sort_keys=True),
            ],
        )
    finally:
        con.close()

    return {
        'live_order_id': live_order_id,
        'kalshi_order_id': updated_row['kalshi_order_id'],
        'ticker': updated_row['ticker'],
        'city_key': market.city_key,
        'city_name': display_name_for_city(market.city_key),
        'station_id': station_id,
        'bucket_code': market.bucket_code,
        'bucket_label': market.label,
        'actual_high_f': float(actual_high_f),
        'outcome': outcome_result,
        'realized_pnl_dollars': realized_pnl,
        'our_forecast_f': float(our_forecast_f),
        'forecast_confidence': forecast_confidence,
        'forecast_error_f': forecast_error_f,
        'our_model_abs_error_f': our_model_abs_error_f,
        'market_was_right': market_was_right,
        'market_favorite_ticker': market_favorite_ticker,
        'market_favorite_error_f': market_favorite_error_f,
        'market_ask_price': market_ask_price,
    }


def run_settlement_and_learning(target_date: date, db_path=None) -> dict:
    """
    Full settlement pipeline for a completed market day:
    1. For each city: fetch actual ASOS daily high (previous day)
    2. For each open live_order on that date:
       - Determine yes/no outcome from actual high vs bucket
       - Call record_bet_outcome()
    3. Compute session P&L
    4. Generate insights text (see below)
    5. Return full settlement report
    """

    bootstrap(db_path=db_path)
    open_orders = []
    for row in fetch_live_orders(db_path=db_path, status_filter=['pending', 'resting', 'executed']):
        if int(row.get('fill_count') or 0) <= 0:
            continue
        market = parse_weather_market({'ticker': row['ticker'], 'title': row['ticker']})
        if market is None or market.market_date_local != target_date:
            continue
        open_orders.append((market.city_key, row, market))

    grouped_orders: dict[str, list[tuple[dict, Any]]] = {}
    for city_key, row, market in open_orders:
        grouped_orders.setdefault(city_key, []).append((row, market))

    city_reports: list[dict[str, Any]] = []
    settled_orders: list[dict[str, Any]] = []
    for city_key in sorted(grouped_orders):
        station_id = STATION_IDS[city_key]
        actual_high_f = fetch_station_daily_high(station_id, target_date)
        city_report = {
            'city_key': city_key,
            'city_name': display_name_for_city(city_key),
            'station_id': station_id,
            'actual_high_f': actual_high_f,
            'orders': [],
        }
        if actual_high_f is None:
            city_report['note'] = 'ASOS daily high unavailable'
            city_reports.append(city_report)
            continue

        for row, market in sorted(grouped_orders[city_key], key=lambda item: item[0]['ticker']):
            calibration = _fetch_calibration_row(str(row['live_order_id']), db_path=db_path)
            fallback_forecast = market_bucket_center(market)
            our_forecast_f = _coerce_float(calibration['our_forecast_f']) if calibration is not None else None
            if our_forecast_f is None:
                our_forecast_f = fallback_forecast if fallback_forecast is not None else float(actual_high_f)
            forecast_confidence = (
                str(calibration['forecast_confidence'])
                if calibration is not None and calibration.get('forecast_confidence')
                else 'unknown'
            )
            summary = record_bet_outcome(
                str(row['live_order_id']),
                float(actual_high_f),
                station_id,
                float(our_forecast_f),
                forecast_confidence,
                db_path=db_path,
            )
            city_report['orders'].append(summary)
            settled_orders.append(summary)
        city_reports.append(city_report)

    session_pnl = round(sum(float(row.get('realized_pnl_dollars') or 0.0) for row in settled_orders), 2)
    con = connect(read_only=True, db_path=db_path)
    try:
        row = con.execute(
            '''
            select coalesce(sum(coalesce(realized_pnl_dollars, 0)), 0)
            from ops.live_orders
            where status = 'settled'
            '''
        ).fetchone()
    finally:
        con.close()
    cumulative_pnl = round(float(row[0] or 0.0), 2)

    settlement_report = {
        'target_date': target_date.isoformat(),
        'cities': city_reports,
        'settled_orders': settled_orders,
        'session_pnl': session_pnl,
        'cumulative_pnl': cumulative_pnl,
        'historical_city_summary': _historical_city_summary(db_path=db_path),
    }
    settlement_report['insights_text'] = generate_insights_text(settlement_report)
    settlement_report['key_insight'] = _extract_first_insight(settlement_report['insights_text'])
    return settlement_report


def generate_insights_text(settlement_report: dict) -> str:
    """
    Analyze the settlement results and generate actionable insight text.

    Look for patterns:
    - If NWS forecast was off by >5°F: note it with city + direction
    - If market was more accurate than our model: note it
    - If our model beat the market: note it
    - If a specific city consistently misses: flag it

    Returns markdown text suitable for appending to BETTING_INSIGHTS.md
    """

    target_date = date.fromisoformat(settlement_report['target_date'])
    lines = [f'## {target_date.strftime("%B")} {target_date.day}, {target_date.year} Settlement Review', '']
    insights: list[str] = []

    for city in settlement_report.get('cities', []):
        orders = city.get('orders') or []
        if not orders:
            continue
        city_name = city['city_name']
        model_order = next((order for order in orders if order.get('forecast_error_f') is not None), None)
        if model_order is not None and abs(float(model_order['forecast_error_f'])) > 5.0:
            insights.append(
                f"- {city_name}: model missed by {float(model_order['forecast_error_f']):+.1f}°F versus {city['station_id']} actual {float(city['actual_high_f']):.1f}°F."
            )

        market_better = next(
            (
                order for order in orders
                if order.get('market_favorite_error_f') is not None
                and order.get('our_model_abs_error_f') is not None
                and float(order['market_favorite_error_f']) + 0.5 < float(order['our_model_abs_error_f'])
            ),
            None,
        )
        if market_better is not None:
            insights.append(
                f"- {city_name}: the market favorite beat our model ({float(market_better['market_favorite_error_f']):.1f}°F vs {float(market_better['our_model_abs_error_f']):.1f}°F error)."
            )

        model_better = next(
            (
                order for order in orders
                if order.get('market_favorite_error_f') is not None
                and order.get('our_model_abs_error_f') is not None
                and float(order['our_model_abs_error_f']) + 0.5 < float(order['market_favorite_error_f'])
            ),
            None,
        )
        if model_better is not None:
            insights.append(
                f"- {city_name}: our model beat the market favorite ({float(model_better['our_model_abs_error_f']):.1f}°F vs {float(model_better['market_favorite_error_f']):.1f}°F error)."
            )

    for city_key, summary in sorted((settlement_report.get('historical_city_summary') or {}).items()):
        if int(summary.get('total_bets') or 0) < 2:
            continue
        avg_abs_error_f = float(summary.get('avg_abs_error_f') or 0.0)
        if avg_abs_error_f < 5.0:
            continue
        insights.append(
            f"- {display_name_for_city(city_key)}: calibration warning, {int(summary['total_bets'])} settled bets now average {avg_abs_error_f:.1f}°F absolute error."
        )

    if not insights:
        insights.append('- No new calibration red flags today. Keep collecting settled outcomes under the current guardrails.')

    lines.extend(insights)
    return '\n'.join(lines)


def append_insights_to_file(insights_text: str, insights_path: str = 'docs/BETTING_INSIGHTS.md') -> None:
    """Append new insights block to BETTING_INSIGHTS.md with date header."""

    if not insights_text.strip():
        return
    path = Path(insights_path).expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = path.read_text() if path.exists() else ''
    separator = '\n\n' if existing and not existing.endswith('\n\n') else ''
    path.write_text(f'{existing}{separator}{insights_text.strip()}\n')


def write_daily_memory(settlement_report: dict, memory_dir: str = None) -> None:
    """
    Append settlement summary to ~/.openclaw/workspace/memory/YYYY-MM-DD.md
    (the date of the settled market, not today).
    If file doesn't exist, create it.
    Uses the standard memory file format.
    """

    target_date = date.fromisoformat(settlement_report['target_date'])
    base_dir = (
        Path(memory_dir).expanduser()
        if memory_dir is not None
        else Path.home() / '.openclaw' / 'workspace' / 'memory'
    )
    base_dir.mkdir(parents=True, exist_ok=True)
    path = base_dir / f'{target_date.isoformat()}.md'

    lines = [
        '## Prediction Market — Settlement Results',
        '',
        f'- Session P&L: {_signed_currency(float(settlement_report.get("session_pnl") or 0.0))}',
        f'- Cumulative P&L: {_signed_currency(float(settlement_report.get("cumulative_pnl") or 0.0))}',
        '',
    ]
    for city in settlement_report.get('cities', []):
        actual_high = city.get('actual_high_f')
        actual_label = 'unavailable' if actual_high is None else f'{float(actual_high):.1f}°F'
        lines.append(f"### {city['city_name']} ({city['station_id']})")
        lines.append(f'- Actual high: {actual_label}')
        for order in city.get('orders', []):
            outcome_label = 'YES' if order['outcome'] == 'yes' else 'NO'
            lines.append(
                f"- {order['ticker']} -> {outcome_label} | P&L {_signed_currency(float(order.get('realized_pnl_dollars') or 0.0))}"
            )
        lines.append('')

    if settlement_report.get('key_insight'):
        lines.append(f"### Key Insight\n- {settlement_report['key_insight']}")
        lines.append('')

    section = '\n'.join(lines).rstrip()
    if path.exists():
        existing = path.read_text().rstrip()
        path.write_text(f'{existing}\n\n{section}\n')
        return

    path.write_text(f'# {target_date.isoformat()}\n\n{section}\n')


def format_settlement_notification(settlement_report: dict, *, insights_updated: bool = False) -> str:
    lines = [
        f'📊 SETTLEMENT — {date.fromisoformat(settlement_report["target_date"]).strftime("%B")} {date.fromisoformat(settlement_report["target_date"]).day} Results',
        '',
    ]
    if not settlement_report.get('settled_orders'):
        lines.append('No filled live orders were ready to settle.')
    else:
        for city in settlement_report.get('cities', []):
            actual_high = city.get('actual_high_f')
            actual_label = 'unavailable' if actual_high is None else f'{float(actual_high):.1f}°F'
            lines.append(f"{city['station_id']} ({city['city_name']}): {actual_label}")
            if actual_high is None:
                lines.append('  Observation data unavailable; skipping settlement')
            for order in city.get('orders', []):
                outcome_label = 'YES' if order['outcome'] == 'yes' else 'NO'
                pnl_value = float(order.get('realized_pnl_dollars') or 0.0)
                pnl_label = f'Win: +${pnl_value:.2f}' if pnl_value >= 0 else f'Loss: -${abs(pnl_value):.2f}'
                status_icon = '✅' if order['outcome'] == 'yes' else '❌'
                lines.append(
                    f"  {order['bucket_code']} ({order['bucket_label']}) — {status_icon} {outcome_label}  {pnl_label}"
                )
            lines.append('')

    lines.append(f"Session P&L: {_signed_currency(float(settlement_report.get('session_pnl') or 0.0))}")
    lines.append(f"Cumulative P&L: {_signed_currency(float(settlement_report.get('cumulative_pnl') or 0.0))}")
    if settlement_report.get('key_insight'):
        lines.append('')
        lines.append(f"🧠 Key insight: {settlement_report['key_insight']}")
    if insights_updated:
        lines.append('📝 BETTING_INSIGHTS.md updated')
    return '\n'.join(lines).rstrip()
