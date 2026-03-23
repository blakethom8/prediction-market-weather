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


def _normalize_city_ids(city_ids: list[str] | tuple[str, ...] | None) -> list[str]:
    if not city_ids:
        return []
    return [city.strip().lower() for city in city_ids if city and city.strip()]


def _json_dumps(payload: Any) -> str:
    return json.dumps({} if payload is None else payload)


def _json_loads(payload: Any, *, default: Any) -> Any:
    if payload in (None, ''):
        return default
    if isinstance(payload, str):
        return json.loads(payload)
    return payload


def _serialize_value(value: Any) -> Any:
    if hasattr(value, 'isoformat'):
        return value.isoformat()
    return value


def _insert_strategy_review_event(
    con,
    *,
    strategy_id: str,
    reviewed_at_utc: datetime,
    decision: str,
    resulting_approval_status: str,
    notes: dict[str, Any] | None,
    actor: str,
) -> None:
    con.execute(
        '''
        insert into ops.strategy_review_events (
            strategy_review_id, strategy_id, reviewed_at_utc, actor,
            decision, resulting_approval_status, notes_json
        ) values (?, ?, ?, ?, ?, ?, ?)
        ''',
        [
            new_id('strategyreview'),
            strategy_id,
            reviewed_at_utc,
            actor,
            decision,
            resulting_approval_status,
            _json_dumps(notes),
        ],
    )


def _insert_proposal_event(
    con,
    *,
    proposal_id: str,
    strategy_id: str,
    event_at_utc: datetime,
    decision: str,
    resulting_status: str,
    notes: dict[str, Any] | None,
    actor: str,
) -> None:
    con.execute(
        '''
        insert into ops.bet_proposal_events (
            proposal_event_id, proposal_id, strategy_id, event_at_utc,
            actor, decision, resulting_status, notes_json
        ) values (?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        [
            new_id('proposalevent'),
            proposal_id,
            strategy_id,
            event_at_utc,
            actor,
            decision,
            resulting_status,
            _json_dumps(notes),
        ],
    )


def _proposal_status_sources(target_status: str) -> tuple[str, ...]:
    if target_status == 'approved':
        return ('pending_review', 'adjustments_requested')
    if target_status == 'rejected':
        return ('pending_review', 'adjustments_requested', 'approved')
    if target_status == 'adjustments_requested':
        return ('pending_review', 'approved')
    return ('pending_review',)


def _transition_strategy_proposals(
    con,
    *,
    strategy_id: str,
    decision: str,
    proposal_status: str,
    event_at_utc: datetime,
    notes: dict[str, Any] | None,
    actor: str,
) -> None:
    source_statuses = _proposal_status_sources(proposal_status)
    placeholders = ', '.join(['?'] * len(source_statuses))
    proposal_rows = con.execute(
        f'''
        select proposal_id
        from ops.bet_proposals
        where strategy_id = ?
          and proposal_status in ({placeholders})
        order by candidate_rank asc, proposal_id asc
        ''',
        [strategy_id, *source_statuses],
    ).fetchall()

    for row in proposal_rows:
        proposal_id = row[0]
        con.execute(
            'update ops.bet_proposals set proposal_status = ? where proposal_id = ?',
            [proposal_status, proposal_id],
        )
        _insert_proposal_event(
            con,
            proposal_id=proposal_id,
            strategy_id=strategy_id,
            event_at_utc=event_at_utc,
            decision=decision,
            resulting_status=proposal_status,
            notes=notes,
            actor=actor,
        )


def update_strategy_approval(
    *,
    strategy_id: str,
    approval_status: str,
    approval_notes: dict[str, Any] | None = None,
    decision_label: str | None = None,
    actor: str = 'blake',
    db_path: str | Path | None = None,
) -> None:
    reviewed_at_utc = datetime.now(UTC)
    approved_at_utc = reviewed_at_utc if approval_status == 'approved' else None
    con = connect(db_path=db_path)
    try:
        con.execute(
            '''
            update ops.strategy_sessions
            set approval_status = ?,
                approved_at_utc = ?,
                last_reviewed_at_utc = ?,
                approval_notes_json = ?
            where strategy_id = ?
            ''',
            [
                approval_status,
                approved_at_utc,
                reviewed_at_utc,
                _json_dumps(approval_notes),
                strategy_id,
            ],
        )
        _insert_strategy_review_event(
            con,
            strategy_id=strategy_id,
            reviewed_at_utc=reviewed_at_utc,
            decision=decision_label or approval_status,
            resulting_approval_status=approval_status,
            notes=approval_notes,
            actor=actor,
        )
        _transition_strategy_proposals(
            con,
            strategy_id=strategy_id,
            decision=decision_label or approval_status,
            proposal_status=approval_status,
            event_at_utc=reviewed_at_utc,
            notes=approval_notes,
            actor=actor,
        )
    finally:
        con.close()


def create_strategy_session(
    *,
    strategy_date_local: date,
    thesis: str,
    research_focus_cities: list[str] | tuple[str, ...] | None = None,
    focus_cities: list[str] | tuple[str, ...] | None = None,
    selection_framework: dict[str, Any] | None = None,
    notes: dict[str, Any] | None = None,
    status: str = 'draft',
    approval_status: str = 'pending_review',
    board_scope: str = 'all_markets',
    board_filters: dict[str, Any] | None = None,
    strategy_variant: str = 'baseline',
    scenario_label: str = 'live',
    session_context: dict[str, Any] | None = None,
    db_path: str | Path | None = None,
) -> str:
    strategy_id = new_id('strategy')
    normalized_focus = _normalize_city_ids(research_focus_cities if research_focus_cities is not None else focus_cities)
    con = connect(db_path=db_path)
    try:
        con.execute(
            '''
            insert into ops.strategy_sessions (
                strategy_id, created_at_utc, strategy_date_local, status,
                approval_status, approved_at_utc, last_reviewed_at_utc, approval_notes_json,
                focus_cities_json, research_focus_cities_json,
                board_scope, board_filters_json, board_generated_at_utc,
                board_market_count, board_city_count,
                thesis, selection_framework_json,
                strategy_variant, scenario_label, session_context_json, notes_json
            ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            [
                strategy_id,
                datetime.now(UTC),
                strategy_date_local,
                status,
                approval_status,
                None,
                None,
                _json_dumps({}),
                _json_dumps(normalized_focus),
                _json_dumps(normalized_focus),
                board_scope,
                _json_dumps(board_filters),
                None,
                0,
                0,
                thesis,
                _json_dumps(selection_framework),
                strategy_variant,
                scenario_label,
                _json_dumps(session_context),
                _json_dumps(notes),
            ],
        )
    finally:
        con.close()
    return strategy_id


def populate_strategy_market_board(
    *,
    strategy_id: str,
    strategy_date_local: date,
    board_cities: list[str] | tuple[str, ...] | None = None,
    focus_cities: list[str] | tuple[str, ...] | None = None,
    db_path: str | Path | None = None,
) -> int:
    filter_city_ids = _normalize_city_ids(board_cities if board_cities is not None else focus_cities)
    board_scope = 'city_subset' if filter_city_ids else 'all_markets'
    board_filters = {'city_ids': filter_city_ids} if filter_city_ids else {}
    captured_at_utc = datetime.now(UTC)

    con = connect(db_path=db_path)
    try:
        con.execute('delete from ops.strategy_market_board where strategy_id = ?', [strategy_id])

        params: list[object] = [strategy_id, captured_at_utc, strategy_date_local]
        city_filter_sql = ''
        if filter_city_ids:
            placeholders = ', '.join(['?'] * len(filter_city_ids))
            city_filter_sql = f' and city_id in ({placeholders})'
            params.extend(filter_city_ids)

        con.execute(
            f'''
            insert into ops.strategy_market_board (
                board_entry_id, strategy_id, market_ticker, market_title, captured_at_utc,
                city_id, market_date_local, forecast_snapshot_id, settlement_source,
                minutes_to_close, price_yes_mid, price_yes_ask, price_yes_bid,
                fair_prob, edge_vs_mid, edge_vs_ask,
                candidate_rank, candidate_bucket, board_notes_json
            )
            select
                'board_' || replace(uuid()::varchar, '-', '') as board_entry_id,
                ?,
                market_ticker,
                market_title,
                ?,
                city_id,
                market_date_local,
                forecast_snapshot_id,
                settlement_source,
                minutes_to_close,
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
        board_market_count, board_city_count = con.execute(
            '''
            select count(*), count(distinct city_id)
            from ops.strategy_market_board
            where strategy_id = ?
            ''',
            [strategy_id],
        ).fetchone()
        con.execute(
            '''
            update ops.strategy_sessions
            set board_scope = ?,
                board_filters_json = ?,
                board_generated_at_utc = ?,
                board_market_count = ?,
                board_city_count = ?
            where strategy_id = ?
            ''',
            [
                board_scope,
                _json_dumps(board_filters),
                captured_at_utc,
                board_market_count,
                board_city_count,
                strategy_id,
            ],
        )
        return board_market_count
    finally:
        con.close()


def replace_strategy_proposals(
    *,
    strategy_id: str,
    proposals: list[dict[str, Any]],
    actor: str = 'system',
    db_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    con = connect(db_path=db_path)
    try:
        con.execute(
            '''
            delete from ops.bet_proposal_events
            where proposal_id in (
                select proposal_id
                from ops.bet_proposals
                where strategy_id = ?
            )
            ''',
            [strategy_id],
        )
        con.execute('delete from ops.bet_proposals where strategy_id = ?', [strategy_id])

        for proposal in proposals:
            proposal_id = proposal.get('proposal_id') or new_id('proposal')
            proposed_at_utc = proposal.get('proposed_at_utc') or datetime.now(UTC)
            proposal['proposal_id'] = proposal_id
            proposal['proposed_at_utc'] = _serialize_value(proposed_at_utc)

            con.execute(
                '''
                insert into ops.bet_proposals (
                    proposal_id, strategy_id, board_entry_id, market_ticker, city_id,
                    market_date_local, proposed_at_utc, proposal_status, side,
                    market_price, target_price, target_quantity,
                    fair_prob, perceived_edge, candidate_rank, candidate_bucket,
                    forecast_snapshot_id, strategy_variant, scenario_label,
                    thesis, rationale_summary, rationale_json, context_json,
                    linked_paper_bet_id
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                [
                    proposal_id,
                    strategy_id,
                    proposal.get('board_entry_id'),
                    proposal['market_ticker'],
                    proposal.get('city_id'),
                    proposal.get('market_date_local'),
                    proposed_at_utc,
                    proposal.get('proposal_status', 'pending_review'),
                    proposal.get('side'),
                    proposal.get('market_price'),
                    proposal.get('target_price'),
                    proposal.get('target_quantity'),
                    proposal.get('fair_prob'),
                    proposal.get('perceived_edge'),
                    proposal.get('candidate_rank'),
                    proposal.get('candidate_bucket'),
                    proposal.get('forecast_snapshot_id'),
                    proposal.get('strategy_variant'),
                    proposal.get('scenario_label'),
                    proposal.get('thesis'),
                    proposal.get('rationale_summary'),
                    _json_dumps(proposal.get('rationale_json')),
                    _json_dumps(proposal.get('context_json')),
                    proposal.get('linked_paper_bet_id'),
                ],
            )
            _insert_proposal_event(
                con,
                proposal_id=proposal_id,
                strategy_id=strategy_id,
                event_at_utc=proposed_at_utc,
                decision='proposed',
                resulting_status=proposal.get('proposal_status', 'pending_review'),
                notes={
                    'market_ticker': proposal['market_ticker'],
                    'target_price': proposal.get('target_price'),
                    'target_quantity': proposal.get('target_quantity'),
                },
                actor=actor,
            )
    finally:
        con.close()
    return proposals


def fetch_strategy_proposals(*, strategy_id: str, db_path: str | Path | None = None) -> list[dict[str, Any]]:
    con = connect(db_path=db_path)
    try:
        rows = con.execute(
            '''
            select
                proposal_id,
                board_entry_id,
                market_ticker,
                city_id,
                market_date_local,
                proposed_at_utc,
                proposal_status,
                side,
                market_price,
                target_price,
                target_quantity,
                fair_prob,
                perceived_edge,
                candidate_rank,
                candidate_bucket,
                forecast_snapshot_id,
                strategy_variant,
                scenario_label,
                thesis,
                rationale_summary,
                rationale_json,
                context_json,
                linked_paper_bet_id
            from ops.bet_proposals
            where strategy_id = ?
            order by candidate_rank asc nulls last, proposal_id asc
            ''',
            [strategy_id],
        ).fetchall()
    finally:
        con.close()

    proposals: list[dict[str, Any]] = []
    for row in rows:
        proposals.append(
            {
                'proposal_id': row[0],
                'board_entry_id': row[1],
                'market_ticker': row[2],
                'city_id': row[3],
                'market_date_local': _serialize_value(row[4]),
                'proposed_at_utc': _serialize_value(row[5]),
                'proposal_status': row[6],
                'side': row[7],
                'market_price': row[8],
                'target_price': row[9],
                'target_quantity': row[10],
                'fair_prob': row[11],
                'perceived_edge': row[12],
                'candidate_rank': row[13],
                'candidate_bucket': row[14],
                'forecast_snapshot_id': row[15],
                'strategy_variant': row[16],
                'scenario_label': row[17],
                'thesis': row[18],
                'rationale_summary': row[19],
                'rationale_json': _json_loads(row[20], default={}),
                'context_json': _json_loads(row[21], default={}),
                'linked_paper_bet_id': row[22],
            }
        )
    return proposals


def create_paper_bet(
    *,
    strategy_id: str,
    market_ticker: str,
    side: str,
    limit_price: float,
    quantity: float,
    rationale_summary: str | None = None,
    proposal_id: str | None = None,
    decision_id: str | None = None,
    forecast_snapshot_id: str | None = None,
    expected_edge: float | None = None,
    strategy_variant: str | None = None,
    scenario_label: str | None = None,
    thesis_at_entry: str | None = None,
    rationale: dict[str, Any] | None = None,
    db_path: str | Path | None = None,
) -> str:
    paper_bet_id = new_id('paperbet')
    notional_dollars = limit_price * quantity
    created_at_utc = datetime.now(UTC)

    con = connect(db_path=db_path)
    try:
        resolved_forecast_snapshot_id = forecast_snapshot_id
        resolved_expected_edge = expected_edge
        resolved_strategy_variant = strategy_variant
        resolved_scenario_label = scenario_label
        resolved_thesis = thesis_at_entry
        resolved_rationale_summary = rationale_summary
        resolved_rationale = rationale or {}

        if proposal_id:
            proposal_row = con.execute(
                '''
                select
                    strategy_id,
                    forecast_snapshot_id,
                    perceived_edge,
                    strategy_variant,
                    scenario_label,
                    thesis,
                    rationale_summary,
                    rationale_json
                from ops.bet_proposals
                where proposal_id = ?
                ''',
                [proposal_id],
            ).fetchone()
            if proposal_row is None:
                raise ValueError(f'Unknown proposal: {proposal_id}')
            if proposal_row[0] != strategy_id:
                raise ValueError(f'Proposal {proposal_id} does not belong to strategy {strategy_id}')

            if resolved_forecast_snapshot_id is None:
                resolved_forecast_snapshot_id = proposal_row[1]
            if resolved_expected_edge is None:
                resolved_expected_edge = proposal_row[2]
            if resolved_strategy_variant is None:
                resolved_strategy_variant = proposal_row[3]
            if resolved_scenario_label is None:
                resolved_scenario_label = proposal_row[4]
            if resolved_thesis is None:
                resolved_thesis = proposal_row[5]
            if resolved_rationale_summary is None:
                resolved_rationale_summary = proposal_row[6]
            if not resolved_rationale:
                resolved_rationale = _json_loads(proposal_row[7], default={})

        con.execute(
            '''
            insert into ops.paper_bets (
                paper_bet_id, strategy_id, proposal_id, decision_id, market_ticker, created_at_utc,
                status, side, limit_price, quantity, notional_dollars,
                expected_edge, strategy_variant, scenario_label, thesis_at_entry,
                rationale_summary, rationale_json, forecast_snapshot_id,
                realized_pnl, outcome_label, closed_at_utc, review_json
            ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            [
                paper_bet_id,
                strategy_id,
                proposal_id,
                decision_id,
                market_ticker,
                created_at_utc,
                'open',
                side,
                limit_price,
                quantity,
                notional_dollars,
                resolved_expected_edge,
                resolved_strategy_variant or 'baseline',
                resolved_scenario_label or 'live',
                resolved_thesis,
                resolved_rationale_summary,
                _json_dumps(resolved_rationale),
                resolved_forecast_snapshot_id,
                None,
                None,
                None,
                _json_dumps({}),
            ],
        )

        if proposal_id:
            con.execute(
                '''
                update ops.bet_proposals
                set linked_paper_bet_id = ?,
                    proposal_status = 'converted_to_paper'
                where proposal_id = ?
                ''',
                [paper_bet_id, proposal_id],
            )
            _insert_proposal_event(
                con,
                proposal_id=proposal_id,
                strategy_id=strategy_id,
                event_at_utc=created_at_utc,
                decision='converted_to_paper',
                resulting_status='converted_to_paper',
                notes={
                    'paper_bet_id': paper_bet_id,
                    'limit_price': limit_price,
                    'quantity': quantity,
                },
                actor='system',
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
    review_payload = review or {}
    settled_at_utc = datetime.now(UTC)
    con = connect(db_path=db_path)
    try:
        row = con.execute(
            '''
            select
                side,
                limit_price,
                quantity,
                proposal_id,
                strategy_id
            from ops.paper_bets
            where paper_bet_id = ?
            ''',
            [paper_bet_id],
        ).fetchone()
        if row is None:
            raise ValueError(f'Unknown paper bet: {paper_bet_id}')

        side, limit_price, quantity, proposal_id, strategy_id = row
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
            [
                realized_pnl,
                outcome_label,
                settled_at_utc,
                _json_dumps(review_payload),
                paper_bet_id,
            ],
        )

        lesson_summary = (
            review_payload.get('lesson_summary')
            or review_payload.get('lesson')
            or review_payload.get('summary')
        )
        con.execute(
            '''
            insert into ops.paper_bet_reviews (
                review_id, paper_bet_id, proposal_id, strategy_id,
                reviewed_at_utc, kalshi_outcome_label, realized_pnl,
                lesson_summary, review_json
            ) values (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            [
                new_id('paperreview'),
                paper_bet_id,
                proposal_id,
                strategy_id,
                settled_at_utc,
                outcome_label,
                realized_pnl,
                lesson_summary,
                _json_dumps(review_payload),
            ],
        )

        if proposal_id:
            con.execute(
                'update ops.bet_proposals set proposal_status = ? where proposal_id = ?',
                ['settled', proposal_id],
            )
            _insert_proposal_event(
                con,
                proposal_id=proposal_id,
                strategy_id=strategy_id,
                event_at_utc=settled_at_utc,
                decision='settled',
                resulting_status='settled',
                notes={
                    'paper_bet_id': paper_bet_id,
                    'outcome_label': outcome_label,
                    'realized_pnl': realized_pnl,
                    'lesson_summary': lesson_summary,
                },
                actor='system',
            )
    finally:
        con.close()
