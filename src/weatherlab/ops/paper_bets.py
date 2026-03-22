from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from ..db import connect
from ..utils.ids import new_id


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
    review: dict | None = None,
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
