"""Low-level persistence helpers for the live betting platform.

`weatherlab.live.workflow` owns the day-of orchestration layer. This module
keeps the underlying DuckDB reads/writes in one live-domain home so the repo's
live path is easier to distinguish from research-facing code.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ..db import connect
from ..utils.ids import new_id


def _normalize_focus_cities(focus_cities: list[str] | tuple[str, ...]) -> list[str]:
    return [city.lower() for city in focus_cities]


def update_strategy_approval(
    *,
    strategy_id: str,
    approval_status: str,
    approval_notes: dict[str, Any] | None = None,
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
    selection_framework: dict[str, Any] | None = None,
    notes: dict[str, Any] | None = None,
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
                json.dumps(_normalize_focus_cities(focus_cities)),
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
            normalized_cities = _normalize_focus_cities(focus_cities)
            placeholders = ', '.join(['?'] * len(normalized_cities))
            city_filter_sql = f' and city_id in ({placeholders})'
            params.extend(normalized_cities)

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
            'select count(*) from ops.strategy_market_board where strategy_id = ?',
            [strategy_id],
        ).fetchone()[0]
    finally:
        con.close()


def create_paper_bet(
    *,
    strategy_id: str,
    market_ticker: str,
    side: str,
    limit_price: float,
    quantity: float,
    rationale_summary: str,
    decision_id: str | None = None,
    forecast_snapshot_id: str | None = None,
    db_path: str | Path | None = None,
) -> str:
    paper_bet_id = new_id('paperbet')
    notional_dollars = limit_price * quantity
    con = connect(db_path=db_path)
    try:
        con.execute(
            '''
            insert into ops.paper_bets (
                paper_bet_id, strategy_id, decision_id, market_ticker, created_at_utc,
                status, side, limit_price, quantity, notional_dollars,
                rationale_summary, forecast_snapshot_id, realized_pnl, outcome_label,
                closed_at_utc, review_json
            ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            [
                paper_bet_id,
                strategy_id,
                decision_id,
                market_ticker,
                datetime.now(UTC),
                'open',
                side,
                limit_price,
                quantity,
                notional_dollars,
                rationale_summary,
                forecast_snapshot_id,
                None,
                None,
                None,
                json.dumps({}),
            ],
        )
    finally:
        con.close()
    return paper_bet_id


def settle_paper_bet(
    *,
    paper_bet_id: str,
    outcome_label: str,
    review: dict[str, Any] | None = None,
    db_path: str | Path | None = None,
) -> None:
    con = connect(db_path=db_path)
    try:
        row = con.execute(
            'select side, limit_price, quantity from ops.paper_bets where paper_bet_id = ?',
            [paper_bet_id],
        ).fetchone()
        if row is None:
            raise ValueError(f'Unknown paper bet: {paper_bet_id}')

        side, limit_price, quantity = row
        if side == 'BUY_YES':
            payout = quantity if outcome_label == 'YES' else 0.0
        elif side == 'BUY_NO':
            payout = quantity if outcome_label == 'NO' else 0.0
        else:
            raise ValueError(f'Unsupported paper bet side: {side}')

        realized_pnl = payout - (limit_price * quantity)
        con.execute(
            '''
            update ops.paper_bets
            set status = 'closed',
                realized_pnl = ?,
                outcome_label = ?,
                closed_at_utc = ?,
                review_json = ?
            where paper_bet_id = ?
            ''',
            [realized_pnl, outcome_label, datetime.now(UTC), json.dumps(review or {}), paper_bet_id],
        )
    finally:
        con.close()
