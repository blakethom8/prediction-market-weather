"""Read-oriented query helpers for live betting surfaces."""

from __future__ import annotations

from collections import Counter
from datetime import date, timedelta
from pathlib import Path
import re
from typing import Any

from ..db import connect
from ._shared import json_loads as _json_loads
from ._shared import serialize_value as _serialize_value
from ._shared import sum_numeric as _sum_numeric
from .live_orders import fetch_live_orders as _fetch_live_orders
from .persistence import fetch_strategy_proposals
from .workflow import fetch_strategy_board, summarize_strategy_board


def _fetch_dicts(
    con,
    query: str,
    params: list[Any] | tuple[Any, ...] | None = None,
    *,
    json_columns: set[str] | frozenset[str] = frozenset(),
) -> list[dict[str, Any]]:
    cursor = con.execute(query, params or [])
    columns = [column[0] for column in cursor.description]
    rows: list[dict[str, Any]] = []
    for raw_row in cursor.fetchall():
        row: dict[str, Any] = {}
        for column, value in zip(columns, raw_row):
            if column in json_columns:
                row[column] = _json_loads(value, default={})
            else:
                row[column] = _serialize_value(value)
        rows.append(row)
    return rows


def _mean(values: list[float]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


def _ratio(numerator: int | float, denominator: int | float) -> float | None:
    if not denominator:
        return None
    return float(numerator) / float(denominator)


def _to_date(value: Any) -> date | None:
    if value in (None, ''):
        return None
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _notes_summary(value: Any) -> str | None:
    if value in (None, '', {}, []):
        return None
    if isinstance(value, dict):
        parts = []
        for key, item in value.items():
            if item in (None, '', {}, []):
                continue
            parts.append(f"{_display_slug(str(key), default='note').capitalize()}: {item}")
        return '; '.join(parts) if parts else None
    if isinstance(value, (list, tuple, set)):
        parts = [str(item) for item in value if item not in (None, '')]
        return '; '.join(parts) if parts else None
    return str(value)


def _normalize_learning_text(value: str | None) -> str | None:
    if value in (None, ''):
        return None
    normalized = re.sub(r'[^a-z0-9]+', ' ', str(value).lower()).strip()
    return normalized or None


def _approval_sort_key(value: str | None) -> int:
    mapping = {
        'approved': 0,
        'adjustments_requested': 1,
        'rejected': 2,
        'pending_review': 3,
        'not_proposed': 4,
    }
    return mapping.get(value or '', 5)


def _time_bucket_sort_key(value: str | None) -> int:
    mapping = {'<2h': 0, '2-6h': 1, '6-12h': 2, '12h+': 3}
    return mapping.get(value or '', 4)


def _threshold_band(value: Any) -> str | None:
    if value in (None, ''):
        return None
    threshold = float(value)
    if threshold < 50:
        return '<50F'
    if threshold < 55:
        return '50-54F'
    if threshold < 60:
        return '55-59F'
    if threshold < 65:
        return '60-64F'
    return '65F+'


def _threshold_band_sort_key(value: str | None) -> int:
    mapping = {'<50F': 0, '50-54F': 1, '55-59F': 2, '60-64F': 3, '65F+': 4}
    return mapping.get(value or '', 5)


def _expected_edge_band(value: Any) -> str | None:
    if value in (None, ''):
        return None
    edge = float(value)
    if edge < 0.03:
        return '<+3 pts'
    if edge < 0.08:
        return '+3 to +8 pts'
    return '>=+8 pts'


def _cents_label(value: Any) -> str:
    if value in (None, ''):
        return 'n/a'
    return f'{float(value) * 100:.1f}c'


def _money_label(value: Any) -> str:
    if value in (None, ''):
        return 'n/a'
    return f"${float(value):,.2f}"


def _percent_label(value: Any) -> str:
    if value in (None, ''):
        return 'n/a'
    return f'{float(value) * 100:.0f}%'


def _latest_timestamp(*values: Any) -> str | None:
    normalized = [str(value) for value in values if value not in (None, '')]
    return max(normalized) if normalized else None


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


def list_strategy_board_learning(
    *,
    strategy_id: str | None = None,
    limit: int | None = None,
    db_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    filters: list[str] = []
    params: list[Any] = []
    if strategy_id is not None:
        filters.append('strategy_id = ?')
        params.append(strategy_id)
    where_sql = f"where {' and '.join(filters)}" if filters else ''
    limit_sql = ''
    if limit is not None:
        limit_sql = 'limit ?'
        params.append(limit)

    con = connect(db_path=db_path)
    try:
        rows = _fetch_dicts(
            con,
            f'''
            select
                strategy_id,
                strategy_date_local,
                strategy_created_at_utc,
                session_approval_status,
                session_thesis,
                session_strategy_variant,
                session_scenario_label,
                board_entry_id,
                market_ticker,
                market_title,
                city_id,
                market_date_local,
                captured_at_utc,
                minutes_to_close,
                time_to_close_bucket,
                price_yes_mid,
                price_yes_ask,
                price_yes_bid,
                fair_prob,
                edge_vs_mid,
                edge_vs_ask,
                candidate_rank,
                candidate_bucket,
                threshold_low_f,
                threshold_high_f,
                proposal_id,
                proposed_at_utc,
                proposal_final_status,
                approval_outcome,
                approval_reviewed_at_utc,
                approval_decision,
                approval_notes_json,
                proposed_side,
                observed_market_price,
                target_price,
                target_quantity,
                perceived_edge,
                strategy_variant,
                scenario_label,
                thesis,
                rationale_summary,
                paper_bet_id,
                paper_bet_created_at_utc,
                paper_bet_status,
                executed_side,
                executed_limit_price,
                executed_quantity,
                notional_dollars,
                expected_edge,
                realized_pnl,
                closed_at_utc,
                kalshi_outcome_label,
                lesson_summary,
                review_json,
                proposed_flag,
                converted_flag,
                win_flag
            from ops.v_strategy_board_learning_history
            {where_sql}
            order by strategy_date_local desc nulls last,
                captured_at_utc desc nulls last,
                candidate_rank asc nulls last,
                market_ticker asc
            {limit_sql}
            ''',
            params,
            json_columns={'approval_notes_json', 'review_json'},
        )
    finally:
        con.close()

    for row in rows:
        row['approval_notes'] = row.pop('approval_notes_json')
        row['review'] = row.pop('review_json')
    return rows


def list_paper_bet_history(
    *,
    strategy_id: str | None = None,
    limit: int | None = None,
    db_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    filters: list[str] = []
    params: list[Any] = []
    if strategy_id is not None:
        filters.append('strategy_id = ?')
        params.append(strategy_id)
    where_sql = f"where {' and '.join(filters)}" if filters else ''
    limit_sql = ''
    if limit is not None:
        limit_sql = 'limit ?'
        params.append(limit)

    con = connect(db_path=db_path)
    try:
        rows = _fetch_dicts(
            con,
            f'''
            select
                paper_bet_id,
                strategy_id,
                strategy_date_local,
                strategy_created_at_utc,
                proposal_id,
                board_entry_id,
                market_ticker,
                market_title,
                city_id,
                market_date_local,
                created_at_utc,
                created_date_utc,
                closed_at_utc,
                closed_date_utc,
                status,
                side,
                limit_price,
                quantity,
                notional_dollars,
                expected_edge,
                realized_pnl,
                outcome_label,
                lesson_summary,
                review_json,
                strategy_variant,
                scenario_label,
                thesis_at_entry,
                candidate_bucket,
                candidate_rank,
                approval_outcome,
                minutes_to_close,
                time_to_close_bucket,
                threshold_low_f,
                threshold_high_f,
                win_flag
            from ops.v_paper_bet_history
            {where_sql}
            order by coalesce(closed_at_utc, created_at_utc) desc nulls last, paper_bet_id desc
            {limit_sql}
            ''',
            params,
            json_columns={'review_json'},
        )
    finally:
        con.close()

    for row in rows:
        row['review'] = row.pop('review_json')
    return rows


def list_strategy_session_learning(
    *,
    limit: int | None = None,
    db_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    params: list[Any] = []
    limit_sql = ''
    if limit is not None:
        limit_sql = 'limit ?'
        params.append(limit)

    con = connect(db_path=db_path)
    try:
        rows = _fetch_dicts(
            con,
            f'''
            select
                strategy_id,
                created_at_utc,
                strategy_date_local,
                status,
                approval_status,
                strategy_variant,
                scenario_label,
                board_market_count,
                board_city_count,
                thesis,
                board_row_count,
                priority_candidate_count,
                watch_candidate_count,
                pass_candidate_count,
                proposal_count,
                approved_count,
                adjusted_count,
                rejected_count,
                converted_count,
                open_paper_bets,
                closed_paper_bets,
                open_notional,
                closed_realized_pnl,
                avg_closed_expected_edge,
                avg_closed_realized_pnl,
                win_rate,
                last_closed_at_utc,
                latest_lesson,
                latest_reviewed_at_utc,
                latest_review_decision,
                latest_review_notes_json
            from ops.v_strategy_session_learning
            order by strategy_date_local desc nulls last, created_at_utc desc nulls last, strategy_id desc
            {limit_sql}
            ''',
            params,
            json_columns={'latest_review_notes_json'},
        )
    finally:
        con.close()

    for row in rows:
        row['latest_review_notes'] = row.pop('latest_review_notes_json')
        row['latest_review_note_summary'] = _notes_summary(row['latest_review_notes'])
    return rows


def _summarize_learning_groups(
    *,
    rows: list[dict[str, Any]],
    key_fn,
    label_fn=None,
    sort_fn=None,
    filter_fn=None,
) -> list[dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = {}

    for row in rows:
        if filter_fn is not None and not filter_fn(row):
            continue
        raw_key = key_fn(row)
        if raw_key in (None, ''):
            raw_key = 'unknown'
        label = label_fn(raw_key) if label_fn is not None else _display_slug(str(raw_key), default='unknown')
        group_key = str(raw_key)
        aggregate = groups.setdefault(
            group_key,
            {
                'group_key': group_key,
                'label': label,
                'board_count': 0,
                'proposal_count': 0,
                'approved_count': 0,
                'adjusted_count': 0,
                'rejected_count': 0,
                'converted_count': 0,
                'closed_count': 0,
                'win_count': 0,
                'loss_count': 0,
                'realized_pnl': 0.0,
                'edge_sum': 0.0,
                'edge_count': 0,
                'expected_edge_sum': 0.0,
                'expected_edge_count': 0,
            },
        )
        aggregate['board_count'] += 1
        if row.get('proposal_id'):
            aggregate['proposal_count'] += 1
        if row.get('approval_outcome') == 'approved':
            aggregate['approved_count'] += 1
        elif row.get('approval_outcome') == 'adjustments_requested':
            aggregate['adjusted_count'] += 1
        elif row.get('approval_outcome') == 'rejected':
            aggregate['rejected_count'] += 1
        if row.get('converted_flag'):
            aggregate['converted_count'] += 1
        if row.get('paper_bet_status') == 'closed':
            aggregate['closed_count'] += 1
            aggregate['realized_pnl'] += float(row.get('realized_pnl') or 0.0)
            if row.get('win_flag') == 1:
                aggregate['win_count'] += 1
            elif row.get('win_flag') == 0:
                aggregate['loss_count'] += 1
        if row.get('edge_vs_ask') is not None:
            aggregate['edge_sum'] += float(row['edge_vs_ask'])
            aggregate['edge_count'] += 1
        if row.get('expected_edge') is not None:
            aggregate['expected_edge_sum'] += float(row['expected_edge'])
            aggregate['expected_edge_count'] += 1

    summaries: list[dict[str, Any]] = []
    for aggregate in groups.values():
        summary = {
            'group_key': aggregate['group_key'],
            'label': aggregate['label'],
            'board_count': aggregate['board_count'],
            'proposal_count': aggregate['proposal_count'],
            'approved_count': aggregate['approved_count'],
            'adjusted_count': aggregate['adjusted_count'],
            'rejected_count': aggregate['rejected_count'],
            'converted_count': aggregate['converted_count'],
            'closed_count': aggregate['closed_count'],
            'win_count': aggregate['win_count'],
            'loss_count': aggregate['loss_count'],
            'proposal_rate': _ratio(aggregate['proposal_count'], aggregate['board_count']),
            'conversion_rate': _ratio(aggregate['converted_count'], aggregate['proposal_count']),
            'win_rate': _ratio(aggregate['win_count'], aggregate['closed_count']),
            'realized_pnl': aggregate['realized_pnl'],
            'avg_board_edge': (
                aggregate['edge_sum'] / aggregate['edge_count']
                if aggregate['edge_count']
                else None
            ),
            'avg_expected_edge': (
                aggregate['expected_edge_sum'] / aggregate['expected_edge_count']
                if aggregate['expected_edge_count']
                else None
            ),
            'avg_realized_pnl': (
                aggregate['realized_pnl'] / aggregate['closed_count']
                if aggregate['closed_count']
                else None
            ),
        }
        summary['_sort_key'] = (
            (0, sort_fn(aggregate['group_key']), summary['label'])
            if sort_fn is not None
            else (
                1,
                -summary['closed_count'],
                -summary['converted_count'],
                -summary['proposal_count'],
                -summary['board_count'],
                summary['label'],
            )
        )
        summaries.append(summary)

    summaries.sort(key=lambda row: row['_sort_key'])
    for row in summaries:
        row.pop('_sort_key', None)
    return summaries


def _summarize_paper_groups(
    *,
    rows: list[dict[str, Any]],
    key_fn,
    label_fn=None,
    sort_fn=None,
    filter_fn=None,
) -> list[dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = {}

    for row in rows:
        if filter_fn is not None and not filter_fn(row):
            continue
        raw_key = key_fn(row)
        if raw_key in (None, ''):
            raw_key = 'unknown'
        label = label_fn(raw_key) if label_fn is not None else _display_slug(str(raw_key), default='unknown')
        group_key = str(raw_key)
        aggregate = groups.setdefault(
            group_key,
            {
                'group_key': group_key,
                'label': label,
                'bet_count': 0,
                'win_count': 0,
                'loss_count': 0,
                'realized_pnl': 0.0,
                'expected_edge_sum': 0.0,
                'expected_edge_count': 0,
            },
        )
        aggregate['bet_count'] += 1
        aggregate['realized_pnl'] += float(row.get('realized_pnl') or 0.0)
        if row.get('win_flag') == 1:
            aggregate['win_count'] += 1
        elif row.get('win_flag') == 0:
            aggregate['loss_count'] += 1
        if row.get('expected_edge') is not None:
            aggregate['expected_edge_sum'] += float(row['expected_edge'])
            aggregate['expected_edge_count'] += 1

    summaries: list[dict[str, Any]] = []
    for aggregate in groups.values():
        summary = {
            'group_key': aggregate['group_key'],
            'label': aggregate['label'],
            'bet_count': aggregate['bet_count'],
            'win_count': aggregate['win_count'],
            'loss_count': aggregate['loss_count'],
            'win_rate': _ratio(aggregate['win_count'], aggregate['bet_count']),
            'realized_pnl': aggregate['realized_pnl'],
            'avg_expected_edge': (
                aggregate['expected_edge_sum'] / aggregate['expected_edge_count']
                if aggregate['expected_edge_count']
                else None
            ),
            'avg_realized_pnl': aggregate['realized_pnl'] / aggregate['bet_count'],
        }
        summary['_sort_key'] = (
            (0, sort_fn(aggregate['group_key']), summary['label'])
            if sort_fn is not None
            else (1, -summary['bet_count'], -summary['realized_pnl'], summary['label'])
        )
        summaries.append(summary)

    summaries.sort(key=lambda row: row['_sort_key'])
    for row in summaries:
        row.pop('_sort_key', None)
    return summaries


def _build_period_rollups(*, closed_rows: list[dict[str, Any]], period: str) -> list[dict[str, Any]]:
    grouped: dict[date, dict[str, Any]] = {}
    for row in closed_rows:
        closed_date = _to_date(row.get('closed_date_utc'))
        if closed_date is None:
            continue
        period_start = (
            closed_date - timedelta(days=closed_date.weekday())
            if period == 'week'
            else closed_date
        )
        aggregate = grouped.setdefault(
            period_start,
            {
                'period_start': period_start,
                'closed_count': 0,
                'win_count': 0,
                'realized_pnl': 0.0,
                'expected_edges': [],
            },
        )
        aggregate['closed_count'] += 1
        aggregate['realized_pnl'] += float(row.get('realized_pnl') or 0.0)
        if row.get('win_flag') == 1:
            aggregate['win_count'] += 1
        if row.get('expected_edge') is not None:
            aggregate['expected_edges'].append(float(row['expected_edge']))

    cumulative_pnl = 0.0
    cumulative_closed = 0
    rollups: list[dict[str, Any]] = []
    for period_start in sorted(grouped):
        aggregate = grouped[period_start]
        cumulative_pnl += aggregate['realized_pnl']
        cumulative_closed += aggregate['closed_count']
        rollups.append(
            {
                'period_start': period_start.isoformat(),
                'period_label': (
                    f"Week of {period_start.isoformat()}"
                    if period == 'week'
                    else period_start.isoformat()
                ),
                'closed_count': aggregate['closed_count'],
                'win_rate': _ratio(aggregate['win_count'], aggregate['closed_count']),
                'realized_pnl': aggregate['realized_pnl'],
                'avg_expected_edge': _mean(aggregate['expected_edges']),
                'cumulative_pnl': cumulative_pnl,
                'cumulative_closed_count': cumulative_closed,
            }
        )
    return rollups


def _build_recurring_lessons(
    *,
    closed_rows: list[dict[str, Any]],
    session_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    themes: dict[str, dict[str, Any]] = {}

    for row in closed_rows:
        text = row.get('lesson_summary')
        normalized = _normalize_learning_text(text)
        if normalized is None:
            continue
        theme = themes.setdefault(
            normalized,
            {'theme': text, 'count': 0, 'last_seen_at': None},
        )
        theme['count'] += 1
        theme['last_seen_at'] = _latest_timestamp(theme['last_seen_at'], row.get('closed_at_utc'))

    for row in session_rows:
        text = row.get('latest_review_note_summary')
        normalized = _normalize_learning_text(text)
        if normalized is None:
            continue
        theme = themes.setdefault(
            normalized,
            {'theme': text, 'count': 0, 'last_seen_at': None},
        )
        theme['count'] += 1
        theme['last_seen_at'] = _latest_timestamp(theme['last_seen_at'], row.get('latest_reviewed_at_utc'))

    recurring = [row for row in themes.values() if row['count'] > 1]
    recurring.sort(key=lambda row: (-row['count'], row['last_seen_at'] or '', row['theme'] or ''))
    return recurring[:6]


def _build_edge_signal(*, closed_rows: list[dict[str, Any]]) -> dict[str, Any]:
    eligible = [
        row
        for row in closed_rows
        if row.get('expected_edge') is not None and row.get('realized_pnl') is not None
    ]
    if len(eligible) < 2:
        return {
            'headline': 'Expected edge has too little settled history yet.',
            'tone': 'neutral',
            'note': 'Need at least two settled paper bets with stored expected edge before the signal is interpretable.',
        }

    sorted_edges = sorted(float(row['expected_edge']) for row in eligible)
    median_edge = sorted_edges[len(sorted_edges) // 2]
    higher = [row for row in eligible if float(row['expected_edge']) >= median_edge]
    lower = [row for row in eligible if float(row['expected_edge']) < median_edge]
    if not higher or not lower:
        return {
            'headline': 'Expected edge values are too clustered to compare cleanly.',
            'tone': 'neutral',
            'note': 'The settled sample does not yet split into meaningfully different edge tiers.',
        }

    higher_avg_pnl = _mean([float(row['realized_pnl']) for row in higher])
    lower_avg_pnl = _mean([float(row['realized_pnl']) for row in lower])
    higher_win_rate = _ratio(sum(1 for row in higher if row.get('win_flag') == 1), len(higher))
    lower_win_rate = _ratio(sum(1 for row in lower if row.get('win_flag') == 1), len(lower))

    if (higher_avg_pnl or 0.0) > (lower_avg_pnl or 0.0):
        headline = 'Higher-edge names are leading the settled sample.'
        tone = 'good'
    elif (higher_avg_pnl or 0.0) < (lower_avg_pnl or 0.0):
        headline = 'Higher stored edge is not yet translating into better results.'
        tone = 'bad'
    else:
        headline = 'Higher-edge and lower-edge settled bets are running flat to each other.'
        tone = 'neutral'

    return {
        'headline': headline,
        'tone': tone,
        'note': (
            f"Closed bets at or above {_percent_label(median_edge)} edge are averaging "
            f"{_money_label(higher_avg_pnl)} with {_percent_label(higher_win_rate)} wins, versus "
            f"{_money_label(lower_avg_pnl)} and {_percent_label(lower_win_rate)} below that line."
        ),
    }


def _build_review_change_log(*, board_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for row in board_rows:
        if not row.get('proposal_id'):
            continue
        if row.get('approval_outcome') not in {'approved', 'adjustments_requested', 'rejected'} and not row.get(
            'paper_bet_id'
        ):
            continue

        change_summary: str
        if row.get('paper_bet_id'):
            if row.get('executed_limit_price') is not None and row.get('target_price') is not None:
                if abs(float(row['executed_limit_price']) - float(row['target_price'])) > 1e-9:
                    price_clause = (
                        f"Converted at {_cents_label(row['executed_limit_price'])} "
                        f"after proposing {_cents_label(row['target_price'])}."
                    )
                else:
                    price_clause = f"Converted at the proposed {_cents_label(row['target_price'])}."
            else:
                price_clause = 'Converted to the paper book.'

            if row.get('paper_bet_status') == 'closed':
                change_summary = (
                    f"{price_clause} Settled {row.get('kalshi_outcome_label') or 'n/a'} "
                    f"for {_money_label(row.get('realized_pnl'))}."
                )
            else:
                change_summary = f'{price_clause} Still open in the paper book.'
        elif row.get('approval_outcome') == 'rejected':
            change_summary = 'Rejected in review and never converted.'
        elif row.get('approval_outcome') == 'adjustments_requested':
            change_summary = 'Adjustment requested, with no later conversion recorded yet.'
        else:
            change_summary = 'Approved in review, but no paper conversion is recorded yet.'

        items.append(
            {
                'strategy_id': row['strategy_id'],
                'strategy_date_local': row['strategy_date_local'],
                'market_ticker': row['market_ticker'],
                'city_id': row.get('city_id'),
                'candidate_bucket': row.get('candidate_bucket'),
                'approval_outcome': row.get('approval_outcome'),
                'approval_note_summary': _notes_summary(row.get('approval_notes')),
                'change_summary': change_summary,
                'sort_at': _latest_timestamp(
                    row.get('closed_at_utc'),
                    row.get('paper_bet_created_at_utc'),
                    row.get('approval_reviewed_at_utc'),
                    row.get('proposed_at_utc'),
                ),
            }
        )

    items.sort(key=lambda row: (row['sort_at'] or '', row['strategy_date_local'] or '', row['market_ticker']), reverse=True)
    return items[:8]


def get_history_snapshot(*, db_path: str | Path | None = None) -> dict[str, Any]:
    board_rows = list_strategy_board_learning(limit=None, db_path=db_path)
    paper_rows = list_paper_bet_history(limit=None, db_path=db_path)
    session_rows = list_strategy_session_learning(limit=None, db_path=db_path)

    closed_rows = [row for row in paper_rows if row['status'] == 'closed']
    latest_lessons = [row for row in closed_rows if row.get('lesson_summary')][:5]
    biggest_wins = [row for row in closed_rows if (row.get('realized_pnl') or 0.0) > 0]
    biggest_wins.sort(key=lambda row: float(row.get('realized_pnl') or 0.0), reverse=True)
    biggest_misses = [row for row in closed_rows if (row.get('realized_pnl') or 0.0) < 0]
    biggest_misses.sort(key=lambda row: float(row.get('realized_pnl') or 0.0))

    candidate_groups = _summarize_learning_groups(
        rows=board_rows,
        key_fn=lambda row: row.get('candidate_bucket'),
        label_fn=lambda value: _display_slug(str(value), default='unknown').capitalize(),
        sort_fn=_bucket_sort_key,
    )
    variant_groups = _summarize_learning_groups(
        rows=board_rows,
        key_fn=lambda row: row.get('strategy_variant'),
        label_fn=lambda value: str(value),
    )
    scenario_groups = _summarize_learning_groups(
        rows=board_rows,
        key_fn=lambda row: row.get('scenario_label'),
        label_fn=lambda value: _display_slug(str(value), default='unknown').capitalize(),
    )
    city_groups = _summarize_learning_groups(
        rows=board_rows,
        key_fn=lambda row: row.get('city_id'),
        label_fn=lambda value: str(value).upper(),
    )
    approval_groups = _summarize_learning_groups(
        rows=board_rows,
        key_fn=lambda row: row.get('approval_outcome'),
        label_fn=lambda value: _display_slug(str(value), default='unknown').capitalize(),
        sort_fn=_approval_sort_key,
        filter_fn=lambda row: row.get('proposal_id') is not None,
    )
    time_bucket_groups = _summarize_learning_groups(
        rows=board_rows,
        key_fn=lambda row: row.get('time_to_close_bucket'),
        label_fn=lambda value: str(value),
        sort_fn=_time_bucket_sort_key,
        filter_fn=lambda row: row.get('time_to_close_bucket') is not None,
    )
    threshold_groups = _summarize_paper_groups(
        rows=closed_rows,
        key_fn=lambda row: _threshold_band(row.get('threshold_low_f')),
        label_fn=lambda value: str(value),
        sort_fn=_threshold_band_sort_key,
        filter_fn=lambda row: _threshold_band(row.get('threshold_low_f')) is not None,
    )
    edge_band_groups = _summarize_paper_groups(
        rows=closed_rows,
        key_fn=lambda row: _expected_edge_band(row.get('expected_edge')),
        label_fn=lambda value: str(value),
        sort_fn=lambda value: {'<+3 pts': 0, '+3 to +8 pts': 1, '>=+8 pts': 2}.get(str(value), 3),
        filter_fn=lambda row: row.get('expected_edge') is not None,
    )

    priority_group = next((row for row in candidate_groups if row['group_key'] == 'priority'), None)
    watch_group = next((row for row in candidate_groups if row['group_key'] == 'watch'), None)
    approved_group = next((row for row in approval_groups if row['group_key'] == 'approved'), None)
    adjusted_group = next((row for row in approval_groups if row['group_key'] == 'adjustments_requested'), None)
    rejected_group = next((row for row in approval_groups if row['group_key'] == 'rejected'), None)

    quality_cues: list[dict[str, Any]] = []
    if priority_group and watch_group:
        if priority_group['closed_count'] and watch_group['closed_count']:
            cue_headline = (
                'Priority candidates are outperforming watch names.'
                if (priority_group['avg_realized_pnl'] or 0.0) > (watch_group['avg_realized_pnl'] or 0.0)
                else 'Watch names are matching or beating priority names in the settled sample.'
            )
            cue_note = (
                f"Priority closed bets are averaging {_money_label(priority_group['avg_realized_pnl'])} "
                f"with {_percent_label(priority_group['win_rate'])} wins, versus "
                f"{_money_label(watch_group['avg_realized_pnl'])} and {_percent_label(watch_group['win_rate'])} for watch."
            )
        elif priority_group['converted_count'] and not watch_group['converted_count']:
            cue_headline = 'Priority names are the only bucket reaching the paper book so far.'
            cue_note = (
                f"Priority candidates have converted {priority_group['converted_count']} time(s). "
                'Watch names have been observed, but none were converted into tracked paper bets yet.'
            )
        else:
            cue_headline = 'Priority versus watch still needs more follow-through.'
            cue_note = 'Both buckets are being tracked, but there is not enough settled execution to compare them credibly yet.'
        quality_cues.append({'headline': cue_headline, 'tone': 'good' if priority_group['converted_count'] else 'neutral', 'note': cue_note})

    if approved_group is not None:
        if rejected_group is not None and rejected_group['converted_count'] == 0:
            cue_note = (
                f"Approved proposals converted {approved_group['converted_count']} time(s) and produced "
                f"{approved_group['closed_count']} closed bet(s). Rejections have not leaked into the paper book."
            )
        else:
            rejected_text = (
                f"Rejected proposals converted {rejected_group['converted_count']} time(s)."
                if rejected_group is not None
                else 'There are no rejected proposals on file yet.'
            )
            cue_note = (
                f"Approved proposals are carrying {_money_label(approved_group['realized_pnl'])} realized P&L. "
                f"{rejected_text}"
            )
        quality_cues.append(
            {
                'headline': 'Review outcomes are now traceable against follow-through.',
                'tone': 'good' if approved_group['converted_count'] else 'neutral',
                'note': cue_note,
            }
        )

    if adjusted_group is not None and adjusted_group['proposal_count']:
        quality_cues.append(
            {
                'headline': 'Adjustment requests are visible as a separate learning lane.',
                'tone': 'warn' if adjusted_group['converted_count'] == 0 else 'neutral',
                'note': (
                    f"{adjusted_group['proposal_count']} proposal(s) were sent back for adjustments, "
                    f"with {adjusted_group['converted_count']} later conversion(s) recorded."
                ),
            }
        )

    if board_rows or paper_rows or session_rows:
        quality_cues.append(_build_edge_signal(closed_rows=closed_rows))

    strongest_city = next((row for row in city_groups if row['closed_count'] > 0), None)
    strongest_threshold = next((row for row in threshold_groups if row['bet_count'] > 0), None)
    if strongest_city is not None:
        quality_cues.append(
            {
                'headline': f"{strongest_city['label']} is carrying the best settled city sample right now.",
                'tone': 'good' if (strongest_city['realized_pnl'] or 0.0) >= 0 else 'warn',
                'note': (
                    f"{strongest_city['closed_count']} closed bet(s), {_percent_label(strongest_city['win_rate'])} wins, "
                    f"and {_money_label(strongest_city['realized_pnl'])} realized P&L."
                ),
            }
        )
    elif strongest_threshold is not None:
        quality_cues.append(
            {
                'headline': f"{strongest_threshold['label']} is the clearest settled threshold band so far.",
                'tone': 'neutral',
                'note': (
                    f"{strongest_threshold['bet_count']} settled bet(s) with "
                    f"{_money_label(strongest_threshold['realized_pnl'])} realized P&L."
                ),
            }
        )

    daily_rollups = _build_period_rollups(closed_rows=closed_rows, period='day')
    weekly_rollups = _build_period_rollups(closed_rows=closed_rows, period='week')
    if len(weekly_rollups) < 2:
        weekly_rollups = []

    recurring_lessons = _build_recurring_lessons(closed_rows=closed_rows, session_rows=session_rows)

    recent_sessions = session_rows[:8]
    recent_settled_bets = closed_rows[:8]
    review_change_log = _build_review_change_log(board_rows=board_rows)

    return {
        'has_history': bool(session_rows or paper_rows or board_rows),
        'has_closed_history': bool(closed_rows),
        'metrics': {
            'strategy_session_count': len(session_rows),
            'board_row_count': len(board_rows),
            'paper_bet_count': len(paper_rows),
            'open_paper_bet_count': len([row for row in paper_rows if row['status'] == 'open']),
            'closed_paper_bet_count': len(closed_rows),
            'cumulative_realized_pnl': _sum_numeric(closed_rows, 'realized_pnl'),
            'win_rate': _ratio(sum(1 for row in closed_rows if row.get('win_flag') == 1), len(closed_rows)),
            'avg_closed_expected_edge': _mean(
                [float(row['expected_edge']) for row in closed_rows if row.get('expected_edge') is not None]
            ),
            'avg_closed_realized_pnl': _mean(
                [float(row['realized_pnl']) for row in closed_rows if row.get('realized_pnl') is not None]
            ),
        },
        'timeline': {
            'daily': daily_rollups,
            'weekly': weekly_rollups,
        },
        'recent_sessions': recent_sessions,
        'grouped_performance': {
            'strategy_rows': session_rows[:12],
            'variant_rows': variant_groups[:8],
            'scenario_rows': scenario_groups[:8],
            'city_rows': city_groups[:10],
            'candidate_rows': candidate_groups[:6],
            'approval_rows': approval_groups[:6],
            'time_bucket_rows': time_bucket_groups[:6],
        },
        'recommendation_quality': {
            'cues': quality_cues[:5],
            'threshold_rows': threshold_groups[:6],
            'edge_band_rows': edge_band_groups[:6],
        },
        'learning_review': {
            'latest_lessons': latest_lessons,
            'recurring_lessons': recurring_lessons,
            'biggest_wins': biggest_wins[:3],
            'biggest_misses': biggest_misses[:3],
            'recent_settled_bets': recent_settled_bets,
            'review_change_log': review_change_log,
        },
    }


def get_strategy_detail(*, strategy_id: str, db_path: str | Path | None = None) -> dict[str, Any] | None:
    session = get_strategy_session(strategy_id=strategy_id, db_path=db_path)
    if session is None:
        return None

    board_rows = fetch_strategy_board(strategy_id=strategy_id, db_path=db_path)
    proposal_rows = fetch_strategy_proposals(strategy_id=strategy_id, db_path=db_path)
    paper_bets = list_paper_bets(strategy_id=strategy_id, db_path=db_path)
    live_orders = _fetch_live_orders(db_path=db_path, strategy_id=strategy_id)
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
    settled_live_orders = [row for row in live_orders if row.get('status') == 'settled']
    open_live_orders = [row for row in live_orders if row.get('status') in {'pending', 'resting', 'executed'}]
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
    summary['live_order_count'] = len(live_orders)
    summary['open_live_orders'] = len(open_live_orders)
    summary['settled_live_orders'] = len(settled_live_orders)
    summary['live_deployed_dollars'] = _sum_numeric(live_orders, 'taker_cost_dollars')
    summary['live_realized_pnl'] = _sum_numeric(settled_live_orders, 'realized_pnl_dollars')
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
        'live_orders': live_orders,
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
