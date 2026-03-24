"""Persistence and sync helpers for real-money Kalshi orders."""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
from pathlib import Path
import re
from typing import Any, Iterable

from ..build.bootstrap import bootstrap
from ..db import connect
from ..ingest.kalshi_live import KalshiClient
from ..settings import WAREHOUSE_PATH
from ..utils.ids import new_id
from ._shared import serialize_value as _serialize_value

ORDERS_TO_SEED = [
    dict(
        kalshi_order_id='a3675221-d090-4196-b812-b393ebfdb5f1',
        client_order_id='chief-test-1774251694',
        strategy_id='strategy_ad19954fb914',
        ticker='KXHIGHMIA-26MAR23-B79.5',
        action='buy',
        side='yes',
        order_type='limit',
        limit_price_cents=4,
        initial_count=125,
        fill_count=13,
        remaining_count=0,
        status='cancelled',
        taker_cost_dollars=0.52,
    ),
    dict(
        kalshi_order_id='b4569b8e-4ec2-43ed-92b5-3e0185b35eae',
        client_order_id='chief-fill-1774251756',
        strategy_id='strategy_ad19954fb914',
        ticker='KXHIGHMIA-26MAR23-B79.5',
        action='buy',
        side='yes',
        order_type='limit',
        limit_price_cents=6,
        initial_count=74,
        fill_count=74,
        remaining_count=0,
        status='executed',
        taker_cost_dollars=4.44,
    ),
    dict(
        kalshi_order_id='d229ceb2-08b0-4719-b48c-78ade09f0b7c',
        client_order_id='chief-mia795-1774253254',
        strategy_id='strategy_ad19954fb914',
        ticker='KXHIGHMIA-26MAR23-B79.5',
        action='buy',
        side='yes',
        order_type='limit',
        limit_price_cents=5,
        initial_count=80,
        fill_count=80,
        remaining_count=0,
        status='executed',
        taker_cost_dollars=4.00,
    ),
    dict(
        kalshi_order_id='f9836e23-a287-4655-9a4e-12d961d48936',
        client_order_id='chief-T79-1774253223',
        strategy_id='strategy_ad19954fb914',
        ticker='KXHIGHMIA-26MAR23-T79',
        action='buy',
        side='yes',
        order_type='limit',
        limit_price_cents=2,
        initial_count=200,
        fill_count=200,
        remaining_count=0,
        status='executed',
        taker_cost_dollars=4.00,
    ),
    dict(
        kalshi_order_id='3507f35c-c181-42cb-8053-6d303214c432',
        client_order_id='chief-T58-1774253223',
        strategy_id='strategy_159766531730',
        ticker='KXHIGHPHIL-26MAR23-T58',
        action='buy',
        side='yes',
        order_type='limit',
        limit_price_cents=1,
        initial_count=200,
        fill_count=200,
        remaining_count=0,
        status='executed',
        taker_cost_dollars=2.00,
    ),
    dict(
        kalshi_order_id='818c272a-fe15-484e-ac1b-325a3471be1d',
        client_order_id='chief-T67-1774253224',
        strategy_id='strategy_159766531730',
        ticker='KXHIGHTDC-26MAR23-T67',
        action='buy',
        side='yes',
        order_type='limit',
        limit_price_cents=1,
        initial_count=200,
        fill_count=200,
        remaining_count=0,
        status='executed',
        taker_cost_dollars=2.00,
    ),
    dict(
        kalshi_order_id='da5a9204-59ba-4c4f-8ed5-eeb4d01d4918',
        client_order_id='chief-T36-1774253225',
        strategy_id='strategy_159766531730',
        ticker='KXHIGHTBOS-26MAR23-T36',
        action='buy',
        side='yes',
        order_type='limit',
        limit_price_cents=1,
        initial_count=200,
        fill_count=200,
        remaining_count=0,
        status='executed',
        taker_cost_dollars=2.00,
    ),
    dict(
        kalshi_order_id='f6b07664-2c39-44e4-bb90-6374f34d503b',
        client_order_id='chief-T83-1774253226',
        strategy_id='strategy_159766531730',
        ticker='KXHIGHTHOU-26MAR23-T83',
        action='buy',
        side='yes',
        order_type='limit',
        limit_price_cents=6,
        initial_count=100,
        fill_count=88,
        remaining_count=12,
        status='resting',
        taker_cost_dollars=5.28,
    ),
]

_READY_DATABASES: set[str] = set()


def _fetch_dicts(con, query: str, params: Iterable[Any] | None = None) -> list[dict[str, Any]]:
    cursor = con.execute(query, list(params or []))
    columns = [column[0] for column in cursor.description]
    rows: list[dict[str, Any]] = []
    for raw_row in cursor.fetchall():
        row: dict[str, Any] = {}
        for column, value in zip(columns, raw_row):
            row[column] = _serialize_value(value)
        rows.append(row)
    return rows


def _ensure_schema(db_path: str | Path | None = None) -> None:
    resolved = str(Path(db_path).expanduser()) if db_path else str(WAREHOUSE_PATH)
    if resolved in _READY_DATABASES:
        return

    try:
        con = connect(read_only=True, db_path=db_path)
        try:
            row = con.execute(
                '''
                select count(*)
                from information_schema.tables
                where (table_schema, table_name) in (
                    ('ops', 'live_orders'),
                    ('ops', 'v_live_positions')
                )
                '''
            ).fetchone()
        finally:
            con.close()
    except Exception:
        row = None

    if row and row[0] == 2:
        _READY_DATABASES.add(resolved)
        return

    bootstrap(db_path=db_path)
    _READY_DATABASES.add(resolved)


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


def _normalize_cents(value: Any) -> int | None:
    if value in (None, ''):
        return None
    numeric = float(value)
    if abs(numeric) <= 1:
        numeric *= 100
    return int(round(numeric))


def _normalize_count(value: Any) -> int | None:
    if value in (None, ''):
        return None
    return int(round(float(value)))


def _normalize_money(value: Any) -> float | None:
    if value in (None, ''):
        return None
    return float(value)


def _normalize_label(value: Any) -> str | None:
    if value in (None, ''):
        return None
    return str(value).strip().lower()


def _coalesce(*values: Any) -> Any:
    for value in values:
        if value not in (None, ''):
            return value
    return None


def _normalize_status(value: Any, *, fill_count: int | None = None, remaining_count: int | None = None) -> str | None:
    label = _normalize_label(value)
    if label in (None, ''):
        return None
    if label in {'filled', 'executed', 'completed', 'complete'}:
        return 'executed'
    if label in {'open', 'resting', 'posted', 'partially_filled'}:
        if remaining_count == 0 and fill_count not in (None, 0):
            return 'executed'
        return 'resting'
    if label in {'pending', 'queued', 'received'}:
        return 'pending'
    if label in {'canceled', 'cancelled'}:
        return 'cancelled'
    if label == 'settled':
        return 'settled'
    return label


def _extract_timestamp_from_client_order_id(client_order_id: str | None) -> datetime | None:
    if not client_order_id:
        return None
    match = re.search(r'(\d{10,})$', client_order_id)
    if not match:
        return None
    return datetime.fromtimestamp(int(match.group(1)), UTC).replace(tzinfo=None)


def _position_pnl_per_contract(*, action: str, side: str, outcome_result: str, price_cents: int | None) -> float:
    price = float(price_cents or 0) / 100.0
    is_match = outcome_result == side
    if action == 'buy':
        return (1.0 - price) if is_match else -price
    return price if not is_match else -(1.0 - price)


def _get_live_order_by_kalshi_id(
    kalshi_order_id: str,
    *,
    db_path: str | Path | None = None,
) -> dict[str, Any] | None:
    rows = fetch_live_orders(db_path=db_path, kalshi_order_id=kalshi_order_id)
    return rows[0] if rows else None


def _extract_kalshi_order_fields(payload: dict[str, Any]) -> dict[str, Any]:
    order = payload.get('order') if isinstance(payload.get('order'), dict) else payload
    initial_count = _normalize_count(
        _coalesce(
            order.get('initial_count'),
            order.get('order_count'),
            order.get('count'),
            order.get('quantity'),
        )
    )
    fill_count = _normalize_count(
        _coalesce(
            order.get('fill_count'),
            order.get('filled_count'),
            order.get('count_filled'),
            order.get('filled_quantity'),
        )
    )
    remaining_count = _normalize_count(
        _coalesce(
            order.get('remaining_count'),
            order.get('resting_count'),
            order.get('pending_count'),
            order.get('count_left'),
        )
    )
    if remaining_count is None and initial_count is not None and fill_count is not None:
        remaining_count = max(initial_count - fill_count, 0)

    return {
        'kalshi_order_id': str(order.get('order_id') or order.get('id') or ''),
        'fill_count': fill_count,
        'remaining_count': remaining_count,
        'status': _normalize_status(order.get('status'), fill_count=fill_count, remaining_count=remaining_count),
        'limit_price_cents': _normalize_cents(
            _coalesce(
                order.get('limit_price'),
                order.get('price'),
                order.get('yes_price'),
                order.get('price_cents'),
            )
        ),
        'taker_cost_dollars': _normalize_money(
            _coalesce(
                order.get('taker_cost_dollars'),
                order.get('taker_cost'),
                order.get('cost'),
            )
        ),
        'taker_fees_dollars': _normalize_money(
            _coalesce(
                order.get('taker_fees_dollars'),
                order.get('taker_fees'),
                order.get('fees'),
            )
        ),
        'updated_at_utc': _parse_timestamp(
            _coalesce(
                order.get('updated_at'),
                order.get('updated_time'),
                order.get('last_update_time'),
            )
        ) or datetime.now(UTC).replace(tzinfo=None),
    }


def seed_live_order(
    *,
    kalshi_order_id: str,
    ticker: str,
    action: str,
    side: str,
    order_type: str,
    strategy_id: str | None = None,
    client_order_id: str | None = None,
    limit_price_cents: int | None = None,
    initial_count: int | None = None,
    fill_count: int = 0,
    remaining_count: int | None = None,
    status: str = 'pending',
    taker_cost_dollars: float | None = None,
    taker_fees_dollars: float | None = None,
    outcome_result: str | None = None,
    realized_pnl_dollars: float | None = None,
    settlement_note: str = '',
    created_at_utc: datetime | None = None,
    updated_at_utc: datetime | None = None,
    settled_at_utc: datetime | None = None,
    db_path: str | Path | None = None,
) -> str:
    """Insert a known live order into ops.live_orders. Returns live_order_id."""

    _ensure_schema(db_path=db_path)
    normalized_action = _normalize_label(action) or 'buy'
    normalized_side = _normalize_label(side) or 'yes'
    normalized_order_type = _normalize_label(order_type) or 'limit'
    normalized_status = _normalize_status(status, fill_count=fill_count, remaining_count=remaining_count) or 'pending'
    normalized_outcome = _normalize_label(outcome_result)
    created_ts = (
        _parse_timestamp(created_at_utc)
        or _extract_timestamp_from_client_order_id(client_order_id)
        or datetime.now(UTC).replace(tzinfo=None)
    )
    updated_ts = _parse_timestamp(updated_at_utc) or created_ts
    resolved_remaining = remaining_count
    if resolved_remaining is None and initial_count is not None:
        resolved_remaining = max(int(initial_count) - int(fill_count), 0)

    con = connect(db_path=db_path)
    try:
        existing = con.execute(
            '''
            select live_order_id
            from ops.live_orders
            where kalshi_order_id = ?
            ''',
            [kalshi_order_id],
        ).fetchone()
        if existing:
            return existing[0]

        live_order_id = new_id('liveorder')
        con.execute(
            '''
            insert into ops.live_orders (
                live_order_id,
                kalshi_order_id,
                client_order_id,
                strategy_id,
                ticker,
                action,
                side,
                order_type,
                limit_price_cents,
                initial_count,
                fill_count,
                remaining_count,
                status,
                taker_cost_dollars,
                taker_fees_dollars,
                outcome_result,
                realized_pnl_dollars,
                settlement_note,
                created_at_utc,
                updated_at_utc,
                settled_at_utc
            ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            [
                live_order_id,
                kalshi_order_id,
                client_order_id,
                strategy_id,
                ticker,
                normalized_action,
                normalized_side,
                normalized_order_type,
                limit_price_cents,
                initial_count,
                fill_count,
                resolved_remaining,
                normalized_status,
                taker_cost_dollars,
                taker_fees_dollars,
                normalized_outcome,
                realized_pnl_dollars,
                settlement_note,
                created_ts,
                updated_ts,
                _parse_timestamp(settled_at_utc),
            ],
        )
        return live_order_id
    finally:
        con.close()


def seed_tonights_live_orders(*, db_path: str | Path | None = None) -> list[str]:
    """Seed the known March 23, 2026 live orders without duplicating rows."""

    return [seed_live_order(db_path=db_path, **order) for order in ORDERS_TO_SEED]


def sync_live_order_from_kalshi(kalshi_order_id, db_path=None) -> dict:
    """Fetch order status from Kalshi API and update ops.live_orders. Returns updated row."""

    _ensure_schema(db_path=db_path)
    existing = _get_live_order_by_kalshi_id(kalshi_order_id, db_path=db_path)
    if existing is None:
        raise LookupError(f'Live order not found: {kalshi_order_id}')

    client = KalshiClient()
    payload = client._request_json('GET', f'/portfolio/orders/{kalshi_order_id}')
    sync_fields = _extract_kalshi_order_fields(payload)

    fill_count = sync_fields['fill_count']
    remaining_count = sync_fields['remaining_count']
    status = sync_fields['status'] or existing['status']
    if fill_count is None:
        fill_count = int(existing.get('fill_count') or 0)
    if remaining_count is None:
        if existing.get('initial_count') is not None:
            remaining_count = max(int(existing['initial_count']) - fill_count, 0)
        else:
            remaining_count = int(existing.get('remaining_count') or 0)

    con = connect(db_path=db_path)
    try:
        con.execute(
            '''
            update ops.live_orders
            set fill_count = ?,
                remaining_count = ?,
                status = ?,
                limit_price_cents = coalesce(?, limit_price_cents),
                taker_cost_dollars = coalesce(?, taker_cost_dollars),
                taker_fees_dollars = coalesce(?, taker_fees_dollars),
                updated_at_utc = ?
            where kalshi_order_id = ?
            ''',
            [
                fill_count,
                remaining_count,
                status,
                sync_fields['limit_price_cents'],
                sync_fields['taker_cost_dollars'],
                sync_fields['taker_fees_dollars'],
                sync_fields['updated_at_utc'],
                kalshi_order_id,
            ],
        )
    finally:
        con.close()

    updated = _get_live_order_by_kalshi_id(kalshi_order_id, db_path=db_path)
    if updated is None:
        raise LookupError(f'Live order disappeared during sync: {kalshi_order_id}')
    return updated


def sync_all_open_live_orders(db_path=None) -> list[dict]:
    """Sync all resting/pending orders from Kalshi API. Returns list of updated rows."""

    orders = fetch_live_orders(db_path=db_path, status_filter=['pending', 'resting'])
    return [sync_live_order_from_kalshi(order['kalshi_order_id'], db_path=db_path) for order in orders]


def settle_live_order(kalshi_order_id, outcome_result: str, settlement_note: str = '', db_path=None) -> None:
    """Mark a live order as settled with its outcome (yes/no) and compute realized P&L."""

    _ensure_schema(db_path=db_path)
    row = _get_live_order_by_kalshi_id(kalshi_order_id, db_path=db_path)
    if row is None:
        raise LookupError(f'Live order not found: {kalshi_order_id}')

    normalized_outcome = _normalize_label(outcome_result)
    if normalized_outcome not in {'yes', 'no'}:
        raise ValueError("outcome_result must be 'yes' or 'no'")

    realized_pnl = round(
        float(row.get('fill_count') or 0)
        * _position_pnl_per_contract(
            action=_normalize_label(row.get('action')) or 'buy',
            side=_normalize_label(row.get('side')) or 'yes',
            outcome_result=normalized_outcome,
            price_cents=row.get('limit_price_cents'),
        ),
        2,
    )
    settled_at_utc = datetime.now(UTC).replace(tzinfo=None)

    con = connect(db_path=db_path)
    try:
        con.execute(
            '''
            update ops.live_orders
            set status = 'settled',
                outcome_result = ?,
                realized_pnl_dollars = ?,
                settlement_note = ?,
                updated_at_utc = ?,
                settled_at_utc = ?
            where kalshi_order_id = ?
            ''',
            [
                normalized_outcome,
                realized_pnl,
                settlement_note,
                settled_at_utc,
                settled_at_utc,
                kalshi_order_id,
            ],
        )
    finally:
        con.close()


def fetch_live_positions(db_path=None, strategy_id: str | None = None) -> list[dict]:
    """Return aggregated current holdings from v_live_positions."""

    _ensure_schema(db_path=db_path)
    params: list[Any] = []
    if strategy_id is None:
        query = '''
            select
                ticker,
                side,
                total_contracts,
                total_cost_dollars,
                max_payout_dollars,
                avg_price_cents,
                order_count,
                latest_status,
                strategy_id,
                realized_pnl_dollars,
                outcome_result
            from ops.v_live_positions
            order by ticker asc, side asc
        '''
    else:
        query = '''
            select
                ticker,
                side,
                sum(fill_count) as total_contracts,
                sum(fill_count * limit_price_cents) / 100.0 as total_cost_dollars,
                sum(fill_count) * 1.0 as max_payout_dollars,
                sum(fill_count * limit_price_cents) / 100.0 / nullif(sum(fill_count), 0) * 100 as avg_price_cents,
                count(*) as order_count,
                max(status) as latest_status,
                max(strategy_id) as strategy_id,
                sum(
                    case
                        when outcome_result = 'yes' then fill_count * (1.0 - limit_price_cents / 100.0)
                        when outcome_result = 'no' then -fill_count * limit_price_cents / 100.0
                        else null
                    end
                ) as realized_pnl_dollars,
                max(outcome_result) as outcome_result
            from ops.live_orders
            where (status != 'cancelled' or fill_count > 0)
              and strategy_id = ?
            group by ticker, side
            order by ticker asc, side asc
        '''
        params.append(strategy_id)

    con = connect(read_only=True, db_path=db_path)
    try:
        return _fetch_dicts(con, query, params)
    finally:
        con.close()


def fetch_live_orders(
    db_path=None,
    status_filter=None,
    strategy_id: str | None = None,
    kalshi_order_id: str | None = None,
    live_order_id: str | None = None,
) -> list[dict]:
    """Return raw live order rows with optional status filter."""

    _ensure_schema(db_path=db_path)
    filters: list[str] = []
    params: list[Any] = []

    if status_filter is not None:
        statuses = [status_filter] if isinstance(status_filter, str) else list(status_filter)
        normalized_statuses = [_normalize_label(status) for status in statuses if _normalize_label(status)]
        if normalized_statuses:
            placeholders = ', '.join(['?'] * len(normalized_statuses))
            filters.append(f'lo.status in ({placeholders})')
            params.extend(normalized_statuses)
    if strategy_id is not None:
        filters.append('lo.strategy_id = ?')
        params.append(strategy_id)
    if kalshi_order_id is not None:
        filters.append('lo.kalshi_order_id = ?')
        params.append(kalshi_order_id)
    if live_order_id is not None:
        filters.append('lo.live_order_id = ?')
        params.append(live_order_id)

    where_sql = f"where {' and '.join(filters)}" if filters else ''
    query = f'''
        select
            lo.live_order_id,
            lo.kalshi_order_id,
            lo.client_order_id,
            lo.strategy_id,
            s.strategy_date_local,
            lo.ticker,
            lo.action,
            lo.side,
            lo.order_type,
            lo.limit_price_cents,
            lo.initial_count,
            lo.fill_count,
            lo.remaining_count,
            lo.status,
            lo.taker_cost_dollars,
            lo.taker_fees_dollars,
            lo.outcome_result,
            lo.realized_pnl_dollars,
            lo.settlement_note,
            lo.created_at_utc,
            lo.updated_at_utc,
            lo.settled_at_utc
        from ops.live_orders lo
        left join ops.strategy_sessions s on s.strategy_id = lo.strategy_id
        {where_sql}
        order by coalesce(lo.settled_at_utc, lo.updated_at_utc, lo.created_at_utc) desc nulls last, lo.live_order_id desc
    '''

    con = connect(read_only=True, db_path=db_path)
    try:
        return _fetch_dicts(con, query, params)
    finally:
        con.close()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Operate on live Kalshi order tracking.')
    parser.add_argument('command', choices=['seed', 'sync'], help='Action to perform')
    parser.add_argument('--db-path', default=None, help='Optional DuckDB path override')
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    if args.command == 'seed':
        result = seed_tonights_live_orders(db_path=args.db_path)
    else:
        result = sync_all_open_live_orders(db_path=args.db_path)
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
