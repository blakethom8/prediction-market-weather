"""High-level day-of workflow helpers for the live betting platform."""

from __future__ import annotations

import json
from collections import Counter
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ..db import connect
from ._shared import json_loads as _json_loads
from ._shared import normalize_city_ids as _normalize_city_ids
from .persistence import (
    create_strategy_session,
    fetch_strategy_proposals,
    populate_strategy_market_board,
    replace_strategy_proposals,
    update_strategy_approval,
)


DEFAULT_SELECTION_FRAMEWORK = {
    'mode': 'paper-trading',
    'goal': 'small repeatable daily edge',
    'review_required': True,
    'compare_full_board_first': True,
}

DEFAULT_REVIEW_GUIDANCE = (
    'Scan the full live board before isolating any single contract.',
    'Treat NYC and Chicago as research-confidence anchors, not live-board limits.',
    'Record approvals, adjustments, rejections, and abstentions explicitly.',
)
def _board_scope_label(board_scope: str) -> str:
    if board_scope == 'city_subset':
        return 'City subset filter'
    return 'All available markets'


def _format_metric(value: float | None) -> str:
    if value is None:
        return 'n/a'
    return f'{value:.3f}'


def _summarize_board_city_mix(board_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts = Counter(row['city_id'] for row in board_rows if row.get('city_id'))
    return [
        {'city_id': city_id, 'market_count': counts[city_id]}
        for city_id in sorted(counts, key=lambda item: (-counts[item], item))
    ]


def _build_strategy_proposals(
    *,
    proposed_rows: list[dict[str, Any]],
    summary: dict[str, Any],
    thesis: str,
    research_focus_cities: list[str],
    selection_framework: dict[str, Any],
    strategy_variant: str,
    scenario_label: str,
) -> list[dict[str, Any]]:
    proposals: list[dict[str, Any]] = []
    for row in proposed_rows:
        rationale_summary = (
            f"{row['market_ticker']} ranks #{row['candidate_rank']} on the "
            f"{summary['board_scope'].replace('_', ' ')} board with ask {_format_metric(row['price_yes_ask'])} "
            f"vs fair {_format_metric(row['fair_prob'])}."
        )
        proposals.append(
            {
                'board_entry_id': row['board_entry_id'],
                'market_ticker': row['market_ticker'],
                'city_id': row['city_id'],
                'market_date_local': row['market_date_local'],
                'proposal_status': 'pending_review',
                'side': 'BUY_YES',
                'market_price': row['price_yes_ask'],
                'target_price': row['price_yes_ask'],
                'target_quantity': None,
                'fair_prob': row['fair_prob'],
                'perceived_edge': row['edge_vs_ask'],
                'candidate_rank': row['candidate_rank'],
                'candidate_bucket': row['candidate_bucket'],
                'forecast_snapshot_id': row['forecast_snapshot_id'],
                'strategy_variant': strategy_variant,
                'scenario_label': scenario_label,
                'thesis': thesis,
                'rationale_summary': rationale_summary,
                'rationale_json': {
                    'principle': 'compare the full live board before isolating one bet',
                    'market_title': row.get('market_title'),
                    'expected_thesis': thesis,
                    'market_snapshot': {
                        'price_yes_bid': row.get('price_yes_bid'),
                        'price_yes_ask': row.get('price_yes_ask'),
                        'price_yes_mid': row.get('price_yes_mid'),
                        'minutes_to_close': row.get('minutes_to_close'),
                    },
                    'edge_snapshot': {
                        'fair_prob': row.get('fair_prob'),
                        'edge_vs_mid': row.get('edge_vs_mid'),
                        'edge_vs_ask': row.get('edge_vs_ask'),
                    },
                    'board_context': {
                        'board_scope': summary['board_scope'],
                        'board_size': summary['board_size'],
                        'board_city_count': summary['board_city_count'],
                        'board_city_ids': summary['board_city_ids'],
                        'candidate_rank': row.get('candidate_rank'),
                        'candidate_bucket': row.get('candidate_bucket'),
                    },
                },
                'context_json': {
                    'research_focus_cities': research_focus_cities,
                    'selection_framework': selection_framework,
                    'strategy_variant': strategy_variant,
                    'scenario_label': scenario_label,
                    'city_mix': summary['board_city_mix'],
                },
            }
        )
    return proposals


def fetch_strategy_board(*, strategy_id: str, db_path: str | Path | None = None) -> list[dict[str, Any]]:
    con = connect(db_path=db_path)
    try:
        rows = con.execute(
            '''
            select
                board_entry_id,
                market_ticker,
                market_title,
                city_id,
                market_date_local,
                forecast_snapshot_id,
                minutes_to_close,
                price_yes_mid,
                price_yes_ask,
                price_yes_bid,
                fair_prob,
                edge_vs_mid,
                edge_vs_ask,
                candidate_rank,
                candidate_bucket,
                board_notes_json
            from ops.strategy_market_board
            where strategy_id = ?
            order by candidate_rank asc, market_ticker asc
            ''',
            [strategy_id],
        ).fetchall()
    finally:
        con.close()

    results: list[dict[str, Any]] = []
    for row in rows:
        results.append(
            {
                'board_entry_id': row[0],
                'market_ticker': row[1],
                'market_title': row[2],
                'city_id': row[3],
                'market_date_local': row[4].isoformat() if row[4] else None,
                'forecast_snapshot_id': row[5],
                'minutes_to_close': row[6],
                'price_yes_mid': row[7],
                'price_yes_ask': row[8],
                'price_yes_bid': row[9],
                'fair_prob': row[10],
                'edge_vs_mid': row[11],
                'edge_vs_ask': row[12],
                'candidate_rank': row[13],
                'candidate_bucket': row[14],
                'board_notes': _json_loads(row[15], default={}),
            }
        )
    return results


def summarize_strategy_board(
    *,
    board_rows: list[dict[str, Any]],
    research_focus_cities: list[str] | tuple[str, ...] | None,
    thesis: str,
    board_scope: str,
) -> dict[str, Any]:
    normalized_focus = _normalize_city_ids(research_focus_cities)
    proposed = [row for row in board_rows if row['candidate_bucket'] == 'priority']
    watch = [row for row in board_rows if row['candidate_bucket'] == 'watch']
    passes = [row for row in board_rows if row['candidate_bucket'] == 'pass']
    top = proposed[:3] if proposed else watch[:3]
    city_mix = _summarize_board_city_mix(board_rows)
    board_city_ids = [item['city_id'] for item in city_mix]

    return {
        'generated_at_utc': datetime.now(UTC).isoformat(),
        'research_focus_cities': normalized_focus,
        'focus_cities': normalized_focus,
        'thesis': thesis,
        'board_scope': board_scope,
        'board_scope_label': _board_scope_label(board_scope),
        'board_size': len(board_rows),
        'board_city_count': len(board_city_ids),
        'board_city_ids': board_city_ids,
        'board_city_mix': city_mix,
        'approval_status': 'pending_review',
        'proposed_count': len(proposed),
        'watch_count': len(watch),
        'pass_count': len(passes),
        'top_candidates': top,
        'proposed_bets': proposed,
        'watchlist': watch,
        'passes': passes,
        'operating_mode': 'paper-trading',
    }


def apply_strategy_review(
    *,
    strategy_id: str,
    decision: str,
    notes: dict[str, Any] | None = None,
    actor: str = 'blake',
    db_path: str | Path | None = None,
) -> None:
    status_map = {
        'approve': 'approved',
        'reject': 'rejected',
        'adjust': 'adjustments_requested',
    }
    if decision not in status_map:
        raise ValueError(f'Unsupported strategy review decision: {decision}')
    update_strategy_approval(
        strategy_id=strategy_id,
        approval_status=status_map[decision],
        approval_notes=notes or {},
        decision_label=decision,
        actor=actor,
        db_path=db_path,
    )


def render_daily_strategy_markdown(*, strategy_id: str, strategy_date_local: date, summary: dict[str, Any]) -> str:
    lines = [
        f'# Daily Strategy Summary — {strategy_date_local.isoformat()}',
        '',
        f'- Strategy ID: `{strategy_id}`',
        f'- Generated (UTC): {summary["generated_at_utc"]}',
        f'- Research focus cities: {", ".join(summary["research_focus_cities"]) or "none"}',
        f'- Board scope: {summary["board_scope_label"]}',
        f'- Cities on board: {", ".join(summary["board_city_ids"]) or "none"}',
        f'- Approval status: {summary.get("approval_status", "pending_review")}',
        f'- Board size: {summary["board_size"]}',
        f'- Proposed bets: {summary["proposed_count"]}',
        f'- Watchlist: {summary["watch_count"]}',
        f'- Passes: {summary["pass_count"]}',
        '',
        '## Thesis',
        summary['thesis'],
        '',
        '## Top Candidates',
    ]

    if not summary['top_candidates']:
        lines.extend(['- No strong candidates yet; likely a low-conviction day.', ''])
    else:
        for row in summary['top_candidates']:
            title = row['market_title'] or row['market_ticker']
            lines.append(
                f"- **{row['market_ticker']}** ({row['city_id']}) — {title}; "
                f"bucket `{row['candidate_bucket']}`, rank {row['candidate_rank']}, "
                f"ask={_format_metric(row['price_yes_ask'])}, fair_prob={_format_metric(row['fair_prob'])}, "
                f"edge_vs_ask={_format_metric(row['edge_vs_ask'])}, minutes_to_close={row['minutes_to_close']}"
            )
        lines.append('')

    lines.append('## Review Guidance')
    for guidance in DEFAULT_REVIEW_GUIDANCE:
        lines.append(f'- {guidance}')
    lines.append('')
    return '\n'.join(lines)


def render_daily_strategy_html(*, strategy_id: str, strategy_date_local: date, summary: dict[str, Any]) -> str:
    top_items = ''.join(
        (
            '<li>'
            f"<strong>{row['market_ticker']}</strong> ({row['city_id']})"
            f" — {row['market_title'] or row['market_ticker']}"
            f" / {row['candidate_bucket']} / rank {row['candidate_rank']}"
            f" / ask {_format_metric(row['price_yes_ask'])}"
            f" / fair {_format_metric(row['fair_prob'])}"
            f" / edge {_format_metric(row['edge_vs_ask'])}"
            '</li>'
        )
        for row in summary['top_candidates']
    ) or '<li>No strong candidates yet; likely a low-conviction day.</li>'

    review_items = ''.join(f'<li>{guidance}</li>' for guidance in DEFAULT_REVIEW_GUIDANCE)

    return f"""<!DOCTYPE html>
<html lang='en'>
<head>
  <meta charset='UTF-8' />
  <meta name='viewport' content='width=device-width, initial-scale=1.0' />
  <title>Daily Strategy Summary {strategy_date_local.isoformat()}</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #f8fafc; color: #0f172a; margin: 0; }}
    .wrap {{ max-width: 880px; margin: 0 auto; padding: 28px 20px 48px; }}
    .card {{ background: white; border-radius: 16px; padding: 24px; box-shadow: 0 8px 24px rgba(15, 23, 42, 0.08); margin-bottom: 18px; }}
    h1, h2 {{ margin-top: 0; }}
    .pill {{ display: inline-block; padding: 6px 10px; border-radius: 999px; background: #dbeafe; color: #1d4ed8; font-size: 13px; margin-right: 8px; }}
    li {{ line-height: 1.6; }}
  </style>
</head>
<body>
  <div class='wrap'>
    <div class='card'>
      <h1>Daily Strategy Summary — {strategy_date_local.isoformat()}</h1>
      <p><span class='pill'>Strategy {strategy_id}</span><span class='pill'>Paper trading</span><span class='pill'>{summary['board_scope_label']}</span></p>
      <p>{summary['thesis']}</p>
    </div>
    <div class='card'>
      <h2>Board Stats</h2>
      <ul>
        <li>Research focus cities: {', '.join(summary['research_focus_cities']) or 'none'}</li>
        <li>Cities on board: {', '.join(summary['board_city_ids']) or 'none'}</li>
        <li>Board size: {summary['board_size']}</li>
        <li>Proposed bets: {summary['proposed_count']}</li>
        <li>Watchlist: {summary['watch_count']}</li>
        <li>Passes: {summary['pass_count']}</li>
      </ul>
    </div>
    <div class='card'>
      <h2>Top Candidates</h2>
      <ul>{top_items}</ul>
    </div>
    <div class='card'>
      <h2>Review Guidance</h2>
      <ul>{review_items}</ul>
    </div>
  </div>
</body>
</html>
"""


def generate_daily_strategy_package(
    *,
    strategy_date_local: date,
    thesis: str,
    research_focus_cities: list[str] | tuple[str, ...] | None = None,
    focus_cities: list[str] | tuple[str, ...] | None = None,
    board_cities: list[str] | tuple[str, ...] | None = None,
    artifacts_dir: str | Path | None = None,
    selection_framework: dict[str, Any] | None = None,
    notes: dict[str, Any] | None = None,
    strategy_variant: str = 'baseline',
    scenario_label: str = 'live',
    db_path: str | Path | None = None,
) -> dict[str, Any]:
    resolved_focus = _normalize_city_ids(research_focus_cities if research_focus_cities is not None else focus_cities)
    resolved_board_cities = _normalize_city_ids(board_cities)
    board_scope = 'city_subset' if resolved_board_cities else 'all_markets'
    resolved_selection_framework = {**DEFAULT_SELECTION_FRAMEWORK, **(selection_framework or {})}
    session_context = {
        'operating_mode': 'paper-trading',
        'board_principle': 'scan_all_available_markets_before_selecting_bets',
        'research_focus_cities': resolved_focus,
        'board_filter_cities': resolved_board_cities,
    }

    strategy_id = create_strategy_session(
        strategy_date_local=strategy_date_local,
        thesis=thesis,
        research_focus_cities=resolved_focus,
        selection_framework=resolved_selection_framework,
        notes=notes or {},
        board_scope=board_scope,
        board_filters={'city_ids': resolved_board_cities} if resolved_board_cities else {},
        strategy_variant=strategy_variant,
        scenario_label=scenario_label,
        session_context=session_context,
        db_path=db_path,
    )
    board_count = populate_strategy_market_board(
        strategy_id=strategy_id,
        strategy_date_local=strategy_date_local,
        board_cities=resolved_board_cities or None,
        db_path=db_path,
    )
    board_rows = fetch_strategy_board(strategy_id=strategy_id, db_path=db_path)
    summary = summarize_strategy_board(
        board_rows=board_rows,
        research_focus_cities=resolved_focus,
        thesis=thesis,
        board_scope=board_scope,
    )
    replace_strategy_proposals(
        strategy_id=strategy_id,
        proposals=_build_strategy_proposals(
            proposed_rows=summary['proposed_bets'],
            summary=summary,
            thesis=thesis,
            research_focus_cities=resolved_focus,
            selection_framework=resolved_selection_framework,
            strategy_variant=strategy_variant,
            scenario_label=scenario_label,
        ),
        db_path=db_path,
    )
    proposal_rows = fetch_strategy_proposals(strategy_id=strategy_id, db_path=db_path)
    summary['proposal_count'] = len(proposal_rows)

    artifact_root = Path(artifacts_dir) if artifacts_dir else Path('artifacts') / 'daily-strategy'
    artifact_root.mkdir(parents=True, exist_ok=True)
    stem = f'{strategy_date_local.isoformat()}_{strategy_id}'

    json_path = artifact_root / f'{stem}.json'
    md_path = artifact_root / f'{stem}.md'
    html_path = artifact_root / f'{stem}.html'

    json_payload = {
        'strategy_id': strategy_id,
        'strategy_date_local': strategy_date_local.isoformat(),
        'board_count': board_count,
        'summary': summary,
        'board_rows': board_rows,
        'proposal_rows': proposal_rows,
    }
    json_path.write_text(json.dumps(json_payload, indent=2))
    md_path.write_text(render_daily_strategy_markdown(strategy_id=strategy_id, strategy_date_local=strategy_date_local, summary=summary))
    html_path.write_text(render_daily_strategy_html(strategy_id=strategy_id, strategy_date_local=strategy_date_local, summary=summary))

    return {
        'strategy_id': strategy_id,
        'board_count': board_count,
        'summary': summary,
        'board_rows': board_rows,
        'proposal_count': len(proposal_rows),
        'proposal_rows': proposal_rows,
        'json_path': str(json_path),
        'markdown_path': str(md_path),
        'html_path': str(html_path),
    }
