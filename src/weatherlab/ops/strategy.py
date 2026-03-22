from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

from ..db import connect
from ..utils.ids import new_id


def update_strategy_approval(
    *,
    strategy_id: str,
    approval_status: str,
    approval_notes: dict | None = None,
    db_path: str | Path | None = None,
) -> None:
    con = connect(db_path=db_path)
    try:
        con.execute(
            '''
            update ops.strategy_sessions
            set approval_status = ?,
                approved_at_utc = ?,
                approval_notes_json = ?
            where strategy_id = ?
            ''',
            [approval_status, datetime.now(UTC), json.dumps(approval_notes or {}), strategy_id],
        )
    finally:
        con.close()


def create_strategy_session(
    *,
    strategy_date_local: date,
    thesis: str,
    focus_cities: list[str] | tuple[str, ...],
    selection_framework: dict | None = None,
    notes: dict | None = None,
    status: str = 'draft',
    approval_status: str = 'pending_review',
    db_path: str | Path | None = None,
) -> str:
    strategy_id = new_id('strategy')
    con = connect(db_path=db_path)
    try:
        con.execute(
            '''
            insert into ops.strategy_sessions (
                strategy_id, created_at_utc, strategy_date_local, status,
                approval_status, approved_at_utc, approval_notes_json,
                focus_cities_json, thesis, selection_framework_json, notes_json
            ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            [
                strategy_id,
                datetime.now(UTC),
                strategy_date_local,
                status,
                approval_status,
                None,
                json.dumps({}),
                json.dumps(list(focus_cities)),
                thesis,
                json.dumps(selection_framework or {}),
                json.dumps(notes or {}),
            ],
        )
    finally:
        con.close()
    return strategy_id


def populate_strategy_market_board(
    *,
    strategy_id: str,
    strategy_date_local: date,
    focus_cities: list[str] | tuple[str, ...] | None = None,
    db_path: str | Path | None = None,
) -> int:
    con = connect(db_path=db_path)
    try:
        con.execute('delete from ops.strategy_market_board where strategy_id = ?', [strategy_id])

        params: list[object] = [strategy_id, datetime.now(UTC), strategy_date_local]
        city_filter_sql = ''
        if focus_cities:
            placeholders = ', '.join(['?'] * len(focus_cities))
            city_filter_sql = f' and city_id in ({placeholders})'
            params.extend([city.lower() for city in focus_cities])

        con.execute(
            f'''
            insert into ops.strategy_market_board (
                board_entry_id, strategy_id, market_ticker, captured_at_utc, city_id,
                market_date_local, forecast_snapshot_id, settlement_source,
                price_yes_mid, price_yes_ask, price_yes_bid, fair_prob,
                edge_vs_mid, edge_vs_ask, candidate_rank, candidate_bucket, board_notes_json
            )
            select
                'board_' || replace(uuid()::varchar, '-', '') as board_entry_id,
                ?,
                market_ticker,
                ?,
                city_id,
                market_date_local,
                forecast_snapshot_id,
                settlement_source,
                price_yes_mid,
                price_yes_ask,
                price_yes_bid,
                fair_prob,
                edge_vs_mid,
                edge_vs_ask,
                candidate_rank,
                candidate_bucket,
                json_object('source', 'features.v_daily_market_board')
            from features.v_daily_market_board
            where market_date_local = ?
            {city_filter_sql}
            ''',
            params,
        )
        return con.execute(
            'select count(*) from ops.strategy_market_board where strategy_id = ?', [strategy_id]
        ).fetchone()[0]
    finally:
        con.close()
