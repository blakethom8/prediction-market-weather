"""Read-oriented query helpers for live betting surfaces."""

from __future__ import annotations

from collections import Counter
import json
from datetime import date
from pathlib import Path
from typing import Any

from ..db import connect
from .persistence import fetch_strategy_proposals
from .workflow import fetch_strategy_board, summarize_strategy_board


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


def _sum_numeric(rows: list[dict[str, Any]], key: str) -> float:
    return sum(float(row.get(key) or 0.0) for row in rows)


def _attach_board_workflow(
    *,
    board_rows: list[dict[str, Any]],
    proposal_rows: list[dict[str, Any]],
    paper_bets: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    proposals_by_entry_id = {
        row['board_entry_id']: row for row in proposal_rows if row.get('board_entry_id')
    }
    proposals_by_market: dict[str, list[dict[str, Any]]] = {}
    for proposal in proposal_rows:
        proposals_by_market.setdefault(proposal['market_ticker'], []).append(proposal)

    paper_bets_by_proposal_id = {
        row['proposal_id']: row for row in paper_bets if row.get('proposal_id')
    }
    paper_bets_by_market: dict[str, list[dict[str, Any]]] = {}
    for bet in paper_bets:
        paper_bets_by_market.setdefault(bet['market_ticker'], []).append(bet)

    enriched_rows: list[dict[str, Any]] = []
    for row in board_rows:
        proposal = proposals_by_entry_id.get(row['board_entry_id'])
        if proposal is None:
            market_matches = proposals_by_market.get(row['market_ticker'], [])
            proposal = market_matches[0] if market_matches else None

        paper_bet = None
        if proposal is not None:
            paper_bet = paper_bets_by_proposal_id.get(proposal['proposal_id'])
        if paper_bet is None:
            market_matches = paper_bets_by_market.get(row['market_ticker'], [])
            paper_bet = market_matches[0] if market_matches else None

        enriched_rows.append(
            {
                **row,
                'proposal_id': proposal['proposal_id'] if proposal else None,
                'proposal_status': proposal['proposal_status'] if proposal else None,
                'proposal_side': proposal['side'] if proposal else None,
                'proposal_target_price': proposal['target_price'] if proposal else None,
                'linked_paper_bet_id': (
                    paper_bet['paper_bet_id']
                    if paper_bet
                    else (proposal['linked_paper_bet_id'] if proposal else None)
                ),
                'paper_bet_status': paper_bet['status'] if paper_bet else None,
                'paper_bet_side': paper_bet['side'] if paper_bet else None,
                'workflow_status': (
                    paper_bet['status']
                    if paper_bet
                    else (proposal['proposal_status'] if proposal else 'unproposed')
                ),
            }
        )

    return enriched_rows


def _session_row_to_dict(row: tuple[Any, ...]) -> dict[str, Any]:
    return {
        'strategy_id': row[0],
        'created_at_utc': _serialize_value(row[1]),
        'strategy_date_local': _serialize_value(row[2]),
        'status': row[3],
        'approval_status': row[4],
        'approved_at_utc': _serialize_value(row[5]),
        'last_reviewed_at_utc': _serialize_value(row[6]),
        'approval_notes': _json_loads(row[7], default={}),
        'focus_cities': _json_loads(row[8], default=[]),
        'research_focus_cities': _json_loads(row[9], default=[]),
        'board_scope': row[10] or 'all_markets',
        'board_filters': _json_loads(row[11], default={}),
        'board_generated_at_utc': _serialize_value(row[12]),
        'board_market_count': row[13] or 0,
        'board_city_count': row[14] or 0,
        'thesis': row[15],
        'selection_framework': _json_loads(row[16], default={}),
        'strategy_variant': row[17] or 'baseline',
        'scenario_label': row[18] or 'live',
        'session_context': _json_loads(row[19], default={}),
        'notes': _json_loads(row[20], default={}),
    }


def list_strategy_sessions(*, limit: int = 10, db_path: str | Path | None = None) -> list[dict[str, Any]]:
    con = connect(db_path=db_path)
    try:
        rows = con.execute(
            '''
            select
                strategy_id,
                created_at_utc,
                strategy_date_local,
                status,
                approval_status,
                approved_at_utc,
                last_reviewed_at_utc,
                approval_notes_json,
                focus_cities_json,
                research_focus_cities_json,
                board_scope,
                board_filters_json,
                board_generated_at_utc,
                board_market_count,
                board_city_count,
                thesis,
                selection_framework_json,
                strategy_variant,
                scenario_label,
                session_context_json,
                notes_json
            from ops.strategy_sessions
            order by strategy_date_local desc nulls last, created_at_utc desc nulls last, strategy_id desc
            limit ?
            ''',
            [limit],
        ).fetchall()
    finally:
        con.close()
    return [_session_row_to_dict(row) for row in rows]


def list_strategy_sessions_for_date(
    *,
    strategy_date_local: date,
    limit: int = 10,
    db_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    con = connect(db_path=db_path)
    try:
        rows = con.execute(
            '''
            select
                strategy_id,
                created_at_utc,
                strategy_date_local,
                status,
                approval_status,
                approved_at_utc,
                last_reviewed_at_utc,
                approval_notes_json,
                focus_cities_json,
                research_focus_cities_json,
                board_scope,
                board_filters_json,
                board_generated_at_utc,
                board_market_count,
                board_city_count,
                thesis,
                selection_framework_json,
                strategy_variant,
                scenario_label,
                session_context_json,
                notes_json
            from ops.strategy_sessions
            where strategy_date_local = ?
            order by created_at_utc desc nulls last, strategy_id desc
            limit ?
            ''',
            [strategy_date_local, limit],
        ).fetchall()
    finally:
        con.close()
    return [_session_row_to_dict(row) for row in rows]


def get_strategy_session(*, strategy_id: str, db_path: str | Path | None = None) -> dict[str, Any] | None:
    con = connect(db_path=db_path)
    try:
        row = con.execute(
            '''
            select
                strategy_id,
                created_at_utc,
                strategy_date_local,
                status,
                approval_status,
                approved_at_utc,
                last_reviewed_at_utc,
                approval_notes_json,
                focus_cities_json,
                research_focus_cities_json,
                board_scope,
                board_filters_json,
                board_generated_at_utc,
                board_market_count,
                board_city_count,
                thesis,
                selection_framework_json,
                strategy_variant,
                scenario_label,
                session_context_json,
                notes_json
            from ops.strategy_sessions
            where strategy_id = ?
            ''',
            [strategy_id],
        ).fetchone()
    finally:
        con.close()
    if row is None:
        return None
    return _session_row_to_dict(row)


def get_latest_strategy_id(
    *,
    strategy_date_local: date | None = None,
    db_path: str | Path | None = None,
) -> str | None:
    con = connect(db_path=db_path)
    try:
        if strategy_date_local is None:
            row = con.execute(
                '''
                select strategy_id
                from ops.strategy_sessions
                order by strategy_date_local desc nulls last, created_at_utc desc nulls last, strategy_id desc
                limit 1
                '''
            ).fetchone()
        else:
            row = con.execute(
                '''
                select strategy_id
                from ops.strategy_sessions
                where strategy_date_local = ?
                order by created_at_utc desc nulls last, strategy_id desc
                limit 1
                ''',
                [strategy_date_local],
            ).fetchone()
    finally:
        con.close()
    return row[0] if row else None


def list_strategy_review_events(*, strategy_id: str, db_path: str | Path | None = None) -> list[dict[str, Any]]:
    con = connect(db_path=db_path)
    try:
        rows = con.execute(
            '''
            select
                strategy_review_id,
                reviewed_at_utc,
                actor,
                decision,
                resulting_approval_status,
                notes_json
            from ops.strategy_review_events
            where strategy_id = ?
            order by reviewed_at_utc desc nulls last, strategy_review_id desc
            ''',
            [strategy_id],
        ).fetchall()
    finally:
        con.close()

    return [
        {
            'strategy_review_id': row[0],
            'reviewed_at_utc': _serialize_value(row[1]),
            'actor': row[2],
            'decision': row[3],
            'resulting_approval_status': row[4],
            'notes': _json_loads(row[5], default={}),
        }
        for row in rows
    ]


def list_strategy_proposal_outcomes(
    *,
    strategy_id: str | None = None,
    limit: int = 100,
    db_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    filters: list[str] = []
    params: list[Any] = []
    if strategy_id is not None:
        filters.append('where spo.strategy_id = ?')
        params.append(strategy_id)
    params.append(limit)
    where_sql = f" {' '.join(filters)}" if filters else ''

    con = connect(db_path=db_path)
    try:
        rows = con.execute(
            f'''
            select
                spo.strategy_id,
                s.strategy_date_local,
                spo.proposal_id,
                spo.proposal_status,
                spo.market_ticker,
                spo.city_id,
                spo.market_date_local,
                spo.proposed_side,
                spo.observed_market_price,
                spo.target_price,
                spo.target_quantity,
                spo.fair_prob,
                spo.perceived_edge,
                spo.strategy_variant,
                spo.scenario_label,
                spo.thesis,
                spo.rationale_summary,
                spo.paper_bet_id,
                spo.paper_bet_status,
                spo.executed_side,
                spo.executed_limit_price,
                spo.executed_quantity,
                spo.expected_edge,
                spo.realized_pnl,
                spo.kalshi_outcome_label,
                spo.lesson_summary
            from ops.v_strategy_proposal_outcomes spo
            left join ops.strategy_sessions s on s.strategy_id = spo.strategy_id
            {where_sql}
            order by s.strategy_date_local desc nulls last, spo.market_ticker asc
            limit ?
            ''',
            params,
        ).fetchall()
    finally:
        con.close()

    return [
        {
            'strategy_id': row[0],
            'strategy_date_local': _serialize_value(row[1]),
            'proposal_id': row[2],
            'proposal_status': row[3],
            'market_ticker': row[4],
            'city_id': row[5],
            'market_date_local': _serialize_value(row[6]),
            'proposed_side': row[7],
            'observed_market_price': row[8],
            'target_price': row[9],
            'target_quantity': row[10],
            'fair_prob': row[11],
            'perceived_edge': row[12],
            'strategy_variant': row[13],
            'scenario_label': row[14],
            'thesis': row[15],
            'rationale_summary': row[16],
            'paper_bet_id': row[17],
            'paper_bet_status': row[18],
            'executed_side': row[19],
            'executed_limit_price': row[20],
            'executed_quantity': row[21],
            'expected_edge': row[22],
            'realized_pnl': row[23],
            'kalshi_outcome_label': row[24],
            'lesson_summary': row[25],
        }
        for row in rows
    ]


def list_paper_bets(
    *,
    strategy_id: str | None = None,
    limit: int = 100,
    db_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    params: list[Any] = []
    filters: list[str] = []
    if strategy_id is not None:
        filters.append('where pb.strategy_id = ?')
        params.append(strategy_id)
    params.append(limit)
    where_sql = f" {' '.join(filters)}" if filters else ''

    con = connect(db_path=db_path)
    try:
        rows = con.execute(
            f'''
            with latest_reviews as (
                select *
                from (
                    select
                        r.*,
                        row_number() over (
                            partition by r.paper_bet_id
                            order by r.reviewed_at_utc desc nulls last, r.review_id desc
                        ) as rn
                    from ops.paper_bet_reviews r
                ) ranked
                where rn = 1
            )
            select
                pb.paper_bet_id,
                pb.strategy_id,
                s.strategy_date_local,
                pb.proposal_id,
                pb.market_ticker,
                pb.created_at_utc,
                pb.status,
                pb.side,
                pb.limit_price,
                pb.quantity,
                pb.notional_dollars,
                pb.expected_edge,
                pb.strategy_variant,
                pb.scenario_label,
                pb.thesis_at_entry,
                pb.rationale_summary,
                pb.realized_pnl,
                coalesce(lr.kalshi_outcome_label, pb.outcome_label) as outcome_label,
                pb.closed_at_utc,
                lr.lesson_summary
            from ops.paper_bets pb
            left join ops.strategy_sessions s on s.strategy_id = pb.strategy_id
            left join latest_reviews lr on lr.paper_bet_id = pb.paper_bet_id
            {where_sql}
            order by coalesce(pb.closed_at_utc, pb.created_at_utc) desc nulls last, pb.paper_bet_id desc
            limit ?
            ''',
            params,
        ).fetchall()
    finally:
        con.close()

    return [
        {
            'paper_bet_id': row[0],
            'strategy_id': row[1],
            'strategy_date_local': _serialize_value(row[2]),
            'proposal_id': row[3],
            'market_ticker': row[4],
            'created_at_utc': _serialize_value(row[5]),
            'status': row[6],
            'side': row[7],
            'limit_price': row[8],
            'quantity': row[9],
            'notional_dollars': row[10],
            'expected_edge': row[11],
            'strategy_variant': row[12],
            'scenario_label': row[13],
            'thesis_at_entry': row[14],
            'rationale_summary': row[15],
            'realized_pnl': row[16],
            'outcome_label': row[17],
            'closed_at_utc': _serialize_value(row[18]),
            'lesson_summary': row[19],
        }
        for row in rows
    ]


def get_strategy_detail(*, strategy_id: str, db_path: str | Path | None = None) -> dict[str, Any] | None:
    session = get_strategy_session(strategy_id=strategy_id, db_path=db_path)
    if session is None:
        return None

    board_rows = fetch_strategy_board(strategy_id=strategy_id, db_path=db_path)
    proposal_rows = fetch_strategy_proposals(strategy_id=strategy_id, db_path=db_path)
    paper_bets = list_paper_bets(strategy_id=strategy_id, db_path=db_path)
    review_events = list_strategy_review_events(strategy_id=strategy_id, db_path=db_path)
    proposal_outcomes = list_strategy_proposal_outcomes(strategy_id=strategy_id, db_path=db_path)
    proposal_outcomes_by_id = {row['proposal_id']: row for row in proposal_outcomes}
    proposal_rows = [
        {
            **row,
            'paper_bet_id': (
                proposal_outcomes_by_id[row['proposal_id']]['paper_bet_id']
                if row['proposal_id'] in proposal_outcomes_by_id
                else row.get('linked_paper_bet_id')
            ),
            'paper_bet_status': (
                proposal_outcomes_by_id[row['proposal_id']]['paper_bet_status']
                if row['proposal_id'] in proposal_outcomes_by_id
                else None
            ),
            'realized_pnl': (
                proposal_outcomes_by_id[row['proposal_id']]['realized_pnl']
                if row['proposal_id'] in proposal_outcomes_by_id
                else None
            ),
            'outcome_label': (
                proposal_outcomes_by_id[row['proposal_id']]['kalshi_outcome_label']
                if row['proposal_id'] in proposal_outcomes_by_id
                else None
            ),
        }
        for row in proposal_rows
    ]
    board_rows = _attach_board_workflow(
        board_rows=board_rows,
        proposal_rows=proposal_rows,
        paper_bets=paper_bets,
    )

    summary = summarize_strategy_board(
        board_rows=board_rows,
        research_focus_cities=session['research_focus_cities'],
        thesis=session['thesis'],
        board_scope=session['board_scope'],
    )
    proposal_status_counts = Counter(row['proposal_status'] or 'unknown' for row in proposal_rows)
    paper_status_counts = Counter(row['status'] or 'unknown' for row in paper_bets)
    open_paper_bets = [row for row in paper_bets if row['status'] == 'open']
    closed_paper_bets = [row for row in paper_bets if row['status'] == 'closed']
    board_rows_with_edge = [row for row in board_rows if row.get('edge_vs_ask') is not None]
    board_rows_with_close = [row for row in board_rows if row.get('minutes_to_close') is not None]
    summary['approval_status'] = session['approval_status']
    summary['proposal_count'] = len(proposal_rows)
    summary['paper_bet_count'] = len(paper_bets)
    summary['top_candidate'] = summary['top_candidates'][0] if summary['top_candidates'] else None
    summary['best_edge_row'] = (
        max(board_rows_with_edge, key=lambda row: row['edge_vs_ask']) if board_rows_with_edge else None
    )
    summary['soonest_close_minutes'] = (
        min(row['minutes_to_close'] for row in board_rows_with_close) if board_rows_with_close else None
    )
    summary['pending_proposals'] = proposal_status_counts.get('pending_review', 0)
    summary['approved_proposals'] = proposal_status_counts.get('approved', 0)
    summary['adjusted_proposals'] = proposal_status_counts.get('adjustments_requested', 0)
    summary['rejected_proposals'] = proposal_status_counts.get('rejected', 0)
    summary['converted_proposals'] = proposal_status_counts.get('converted_to_paper', 0)
    summary['open_paper_bets'] = paper_status_counts.get('open', 0)
    summary['closed_paper_bets'] = paper_status_counts.get('closed', 0)
    summary['open_paper_notional'] = _sum_numeric(open_paper_bets, 'notional_dollars')
    summary['closed_realized_pnl'] = _sum_numeric(closed_paper_bets, 'realized_pnl')
    summary['latest_review_event'] = review_events[0] if review_events else None
    summary['latest_lesson'] = next(
        (row['lesson_summary'] for row in closed_paper_bets if row.get('lesson_summary')),
        None,
    )

    return {
        'session': session,
        'summary': summary,
        'board_rows': board_rows,
        'proposal_rows': proposal_rows,
        'proposal_outcomes': proposal_outcomes,
        'review_events': review_events,
        'paper_bets': paper_bets,
    }


def get_dashboard_snapshot(*, db_path: str | Path | None = None) -> dict[str, Any]:
    recent_sessions = list_strategy_sessions(limit=8, db_path=db_path)
    recent_paper_bets = list_paper_bets(limit=8, db_path=db_path)

    con = connect(db_path=db_path)
    try:
        counts = con.execute(
            '''
            select
                (select count(*) from ops.strategy_sessions) as total_strategy_sessions,
                (select count(*) from ops.strategy_sessions where approval_status = 'pending_review') as pending_strategy_reviews,
                (select count(*) from ops.bet_proposals where proposal_status = 'pending_review') as pending_proposals,
                (select count(*) from ops.paper_bets where status = 'open') as open_paper_bets,
                (select count(*) from ops.paper_bets where status = 'closed') as closed_paper_bets
            '''
        ).fetchone()
    finally:
        con.close()

    latest_session = recent_sessions[0] if recent_sessions else None
    return {
        'latest_session': latest_session,
        'recent_sessions': recent_sessions,
        'recent_paper_bets': recent_paper_bets,
        'metrics': {
            'total_strategy_sessions': counts[0],
            'pending_strategy_reviews': counts[1],
            'pending_proposals': counts[2],
            'open_paper_bets': counts[3],
            'closed_paper_bets': counts[4],
        },
    }
