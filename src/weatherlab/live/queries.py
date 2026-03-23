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


def _bucket_sort_key(value: str | None) -> int:
    mapping = {'priority': 0, 'watch': 1, 'pass': 2}
    return mapping.get(value or '', 3)


def _workflow_sort_key(value: str | None) -> int:
    mapping = {
        'pending_review': 0,
        'adjustments_requested': 1,
        'approved': 2,
        'unproposed': 3,
        'open': 4,
        'closed': 5,
        'rejected': 6,
    }
    return mapping.get(value or '', 7)


def _display_slug(value: str | None, *, default: str) -> str:
    if value in (None, ''):
        return default
    return str(value).replace('_', ' ')


def _board_principle_label(session_context: dict[str, Any]) -> str:
    principle = session_context.get('board_principle')
    if principle == 'scan_all_available_markets_before_selecting_bets':
        return 'Scan all available markets before isolating any single bet.'
    return _display_slug(principle, default='Scan the full live board before approving any bet.').capitalize()


def _annotate_operator_state(row: dict[str, Any]) -> dict[str, Any]:
    bucket = row.get('candidate_bucket')
    proposal_status = row.get('proposal_status')
    paper_status = row.get('paper_bet_status')

    if paper_status == 'open':
        state = {
            'operator_action': 'In paper book',
            'operator_note': 'Already converted. Monitor exposure instead of reopening the contract.',
            'operator_tone': 'good',
            'is_available_candidate': False,
            'is_trade_ready': False,
        }
    elif paper_status == 'closed':
        state = {
            'operator_action': 'Settled',
            'operator_note': 'Closed position. Carry the lesson forward instead of trading the same snapshot again.',
            'operator_tone': 'muted',
            'is_available_candidate': False,
            'is_trade_ready': False,
        }
    elif proposal_status == 'approved':
        state = {
            'operator_action': 'Approved to convert',
            'operator_note': 'Cleared in review. Convert only if the live price still agrees with the thesis.',
            'operator_tone': 'good',
            'is_available_candidate': bucket in {'priority', 'watch'},
            'is_trade_ready': bucket == 'priority',
        }
    elif proposal_status == 'adjustments_requested':
        state = {
            'operator_action': 'Needs adjustment',
            'operator_note': 'Re-price or re-size before any paper conversion.',
            'operator_tone': 'warn',
            'is_available_candidate': bucket in {'priority', 'watch'},
            'is_trade_ready': bucket == 'priority',
        }
    elif proposal_status == 'pending_review':
        state = {
            'operator_action': 'Review now',
            'operator_note': 'Still waiting for an explicit operator decision.',
            'operator_tone': 'warn',
            'is_available_candidate': bucket in {'priority', 'watch'},
            'is_trade_ready': bucket == 'priority',
        }
    elif proposal_status == 'rejected':
        state = {
            'operator_action': 'Rejected',
            'operator_note': 'Explicit pass unless the market changes materially.',
            'operator_tone': 'muted',
            'is_available_candidate': False,
            'is_trade_ready': False,
        }
    elif bucket == 'priority':
        state = {
            'operator_action': 'Ready to propose',
            'operator_note': 'Priority edge on the broad board with no workflow state recorded yet.',
            'operator_tone': 'good',
            'is_available_candidate': True,
            'is_trade_ready': True,
        }
    elif bucket == 'watch':
        state = {
            'operator_action': 'Watch only',
            'operator_note': 'Interesting, but not yet clean enough to push.',
            'operator_tone': 'warn',
            'is_available_candidate': True,
            'is_trade_ready': False,
        }
    elif bucket == 'pass':
        state = {
            'operator_action': 'Pass',
            'operator_note': 'No action unless the next refresh changes the edge.',
            'operator_tone': 'muted',
            'is_available_candidate': False,
            'is_trade_ready': False,
        }
    else:
        state = {
            'operator_action': 'Needs data',
            'operator_note': 'Pricing or model inputs are incomplete.',
            'operator_tone': 'neutral',
            'is_available_candidate': False,
            'is_trade_ready': False,
        }

    return {**row, **state}


def _operator_row_sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
    edge_vs_ask = row.get('edge_vs_ask')
    candidate_rank = row.get('candidate_rank')
    minutes_to_close = row.get('minutes_to_close')
    return (
        _bucket_sort_key(row.get('candidate_bucket')),
        0 if row.get('is_trade_ready') else 1 if row.get('is_available_candidate') else 2,
        _workflow_sort_key(row.get('workflow_status')),
        -(float(edge_vs_ask) if edge_vs_ask is not None else -999.0),
        int(minutes_to_close) if minutes_to_close is not None else 10**9,
        int(candidate_rank) if candidate_rank is not None else 10**9,
        row.get('market_ticker') or '',
    )


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


def _build_strategy_points(*, session: dict[str, Any], summary: dict[str, Any]) -> list[dict[str, str]]:
    selection_framework = session.get('selection_framework') or {}
    research_focus = session.get('research_focus_cities') or []
    return [
        {
            'label': 'Board principle',
            'value': _board_principle_label(session.get('session_context') or {}),
            'note': 'The live scan should stay broad even when a few cities anchor research.',
        },
        {
            'label': 'Operating goal',
            'value': selection_framework.get('goal') or 'Small repeatable daily edge.',
            'note': 'Keep the day explainable and size-controlled.',
        },
        {
            'label': 'Research anchors',
            'value': ', '.join(research_focus) if research_focus else 'No specific anchors recorded.',
            'note': 'Anchors guide review confidence. They do not narrow the board by default.',
        },
        {
            'label': 'Board scope',
            'value': f"{summary['board_scope_label']} across {summary['board_city_count']} cities.",
            'note': f"{summary['board_size']} captured market rows on this run.",
        },
    ]


def _build_operator_queue(*, session: dict[str, Any], summary: dict[str, Any]) -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    top_candidate = summary.get('top_candidate')
    top_market = top_candidate['market_ticker'] if top_candidate else 'the top-ranked name'

    if session['approval_status'] == 'pending_review':
        items.append(
            {
                'label': 'Make the strategy call',
                'value': f"{summary['pending_proposals']} proposal(s) still need review.",
                'note': f'Start with {top_market} and decide whether today has a real approval set.',
                'tone': 'warn',
            }
        )
    elif session['approval_status'] == 'adjustments_requested':
        items.append(
            {
                'label': 'Rework the proposed prices',
                'value': f"{summary['adjustment_queue_count']} name(s) are waiting on adjustments.",
                'note': 'Re-check price discipline, size, and close-time risk before approving.',
                'tone': 'warn',
            }
        )
    elif session['approval_status'] == 'approved':
        items.append(
            {
                'label': 'Use approved names deliberately',
                'value': f"{summary['approved_waiting_conversion_count']} approved name(s) are not converted yet.",
                'note': 'Convert only if the current board still supports the same edge.',
                'tone': 'good' if summary['approved_waiting_conversion_count'] == 0 else 'warn',
            }
        )
    elif session['approval_status'] == 'rejected':
        items.append(
            {
                'label': 'Treat today as a pass unless the board moves',
                'value': 'The session was rejected.',
                'note': 'Keep the explanation and wait for a materially better slate instead of forcing action.',
                'tone': 'muted',
            }
        )

    if summary['available_candidate_count']:
        items.append(
            {
                'label': 'Review the live candidate set',
                'value': f"{summary['available_candidate_count']} available candidate(s) remain on the board.",
                'note': f"{summary['priority_candidate_count']} are priority names and {summary['watch_count']} are on the watchlist.",
                'tone': 'good' if summary['priority_candidate_count'] else 'neutral',
            }
        )

    if summary['open_paper_bets']:
        items.append(
            {
                'label': 'Watch the open paper book',
                'value': f"{summary['open_paper_bets']} open paper bet(s) still track this strategy.",
                'note': f"Current open notional is ${summary['open_paper_notional']:,.2f}.",
                'tone': 'warn',
            }
        )

    if summary['latest_lesson']:
        items.append(
            {
                'label': 'Carry forward the latest lesson',
                'value': summary['latest_lesson'],
                'note': "Use the most recent closed-bet note as a constraint on today's sizing and selectivity.",
                'tone': 'neutral',
            }
        )

    if not items:
        items.append(
            {
                'label': 'Low-action board',
                'value': 'There is no active queue right now.',
                'note': 'Keep the broad scan recorded, leave the passes explicit, and wait for a better refresh.',
                'tone': 'neutral',
            }
        )
    return items[:4]


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
    board_rows = [_annotate_operator_state(row) for row in board_rows]

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
    priority_candidates = sorted(
        [row for row in board_rows if row['candidate_bucket'] == 'priority' and row['is_available_candidate']],
        key=_operator_row_sort_key,
    )
    available_candidates = sorted(
        [row for row in board_rows if row['is_available_candidate']],
        key=_operator_row_sort_key,
    )
    watchlist_rows = sorted(
        [row for row in board_rows if row['candidate_bucket'] == 'watch'],
        key=_operator_row_sort_key,
    )
    pass_rows = sorted(
        [row for row in board_rows if row['candidate_bucket'] == 'pass'],
        key=_operator_row_sort_key,
    )
    summary['priority_candidates'] = priority_candidates
    summary['priority_candidate_count'] = len(priority_candidates)
    summary['top_recommendations'] = priority_candidates[:3] if priority_candidates else available_candidates[:3]
    summary['available_candidates'] = available_candidates
    summary['available_candidate_count'] = len(available_candidates)
    summary['watchlist_rows'] = watchlist_rows
    summary['pass_rows'] = pass_rows
    summary['adjustment_queue_count'] = len(
        [row for row in board_rows if row.get('proposal_status') == 'adjustments_requested']
    )
    summary['approved_waiting_conversion_count'] = len(
        [
            row
            for row in board_rows
            if row.get('proposal_status') == 'approved' and row.get('paper_bet_status') is None
        ]
    )
    summary['strategy_points'] = _build_strategy_points(session=session, summary=summary)
    summary['operator_queue'] = _build_operator_queue(session=session, summary=summary)

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


def get_today_snapshot(
    *,
    reference_date_local: date,
    strategy_id: str | None = None,
    db_path: str | Path | None = None,
) -> dict[str, Any]:
    today_sessions = list_strategy_sessions_for_date(
        strategy_date_local=reference_date_local,
        limit=10,
        db_path=db_path,
    )
    selected_session = next(
        (row for row in today_sessions if strategy_id is not None and row['strategy_id'] == strategy_id),
        None,
    )
    if selected_session is None and today_sessions:
        selected_session = today_sessions[0]

    latest_sessions = list_strategy_sessions(limit=1, db_path=db_path)
    latest_session = latest_sessions[0] if latest_sessions else None

    today_detail = (
        get_strategy_detail(strategy_id=selected_session['strategy_id'], db_path=db_path)
        if selected_session
        else None
    )
    fallback_detail = None
    if today_detail is None and latest_session is not None:
        fallback_detail = get_strategy_detail(strategy_id=latest_session['strategy_id'], db_path=db_path)

    active_detail = today_detail or fallback_detail
    active_session = selected_session or latest_session

    return {
        'reference_date_local': reference_date_local.isoformat(),
        'today_sessions': today_sessions,
        'today_session': selected_session,
        'today_detail': today_detail,
        'has_today_session': today_detail is not None,
        'latest_session': latest_session,
        'fallback_detail': fallback_detail,
        'is_fallback_to_latest': today_detail is None and fallback_detail is not None,
        'active_session': active_session,
        'active_detail': active_detail,
    }
