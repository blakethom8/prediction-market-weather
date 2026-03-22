from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ..db import connect
from ..ops.strategy import create_strategy_session, populate_strategy_market_board, update_strategy_approval


DEFAULT_SELECTION_FRAMEWORK = {
    'mode': 'paper-trading',
    'goal': 'small repeatable daily edge',
    'review_required': True,
    'compare_full_board_first': True,
}


def fetch_strategy_board(*, strategy_id: str, db_path: str | Path | None = None) -> list[dict[str, Any]]:
    con = connect(db_path=db_path)
    try:
        rows = con.execute(
            '''
            select
                market_ticker,
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
                'market_ticker': row[0],
                'city_id': row[1],
                'market_date_local': row[2].isoformat() if row[2] else None,
                'forecast_snapshot_id': row[3],
                'settlement_source': row[4],
                'price_yes_mid': row[5],
                'price_yes_ask': row[6],
                'price_yes_bid': row[7],
                'fair_prob': row[8],
                'edge_vs_mid': row[9],
                'edge_vs_ask': row[10],
                'candidate_rank': row[11],
                'candidate_bucket': row[12],
                'board_notes': json.loads(row[13]) if row[13] else {},
            }
        )
    return results


def summarize_strategy_board(*, board_rows: list[dict[str, Any]], focus_cities: list[str] | tuple[str, ...], thesis: str) -> dict[str, Any]:
    proposed = [row for row in board_rows if row['candidate_bucket'] == 'priority']
    watch = [row for row in board_rows if row['candidate_bucket'] == 'watch']
    passes = [row for row in board_rows if row['candidate_bucket'] == 'pass']

    top = proposed[:3] if proposed else watch[:3]

    return {
        'generated_at_utc': datetime.now(UTC).isoformat(),
        'focus_cities': list(focus_cities),
        'thesis': thesis,
        'board_size': len(board_rows),
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


def apply_strategy_review(*, strategy_id: str, decision: str, notes: dict[str, Any] | None = None, db_path: str | Path | None = None) -> None:
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
        db_path=db_path,
    )


def render_daily_strategy_markdown(*, strategy_id: str, strategy_date_local: date, summary: dict[str, Any]) -> str:
    lines = [
        f'# Daily Strategy Summary — {strategy_date_local.isoformat()}',
        '',
        f'- Strategy ID: `{strategy_id}`',
        f'- Generated (UTC): {summary["generated_at_utc"]}',
        f'- Focus cities: {", ".join(summary["focus_cities"])}',
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
            lines.append(
                f"- **{row['market_ticker']}** ({row['city_id']}) — bucket `{row['candidate_bucket']}`, "
                f"rank {row['candidate_rank']}, ask={row['price_yes_ask']}, fair_prob={row['fair_prob']}, "
                f"edge_vs_ask={row['edge_vs_ask']}"
            )
        lines.append('')

    lines.append('## Review Guidance')
    lines.append('- Compare the full board before approving any single paper bet.')
    lines.append('- Approve only bets that still feel strong after cross-city comparison.')
    lines.append('- Record abstentions when the board lacks clean edge.')
    lines.append('')
    return '\n'.join(lines)


def render_daily_strategy_html(*, strategy_id: str, strategy_date_local: date, summary: dict[str, Any]) -> str:
    top_items = ''.join(
        f"<li><strong>{row['market_ticker']}</strong> ({row['city_id']}) — {row['candidate_bucket']} / rank {row['candidate_rank']} / ask {row['price_yes_ask']} / fair {row['fair_prob']} / edge {row['edge_vs_ask']}</li>"
        for row in summary['top_candidates']
    ) or '<li>No strong candidates yet; likely a low-conviction day.</li>'

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
      <p><span class='pill'>Strategy {strategy_id}</span><span class='pill'>Paper trading</span><span class='pill'>{', '.join(summary['focus_cities'])}</span></p>
      <p>{summary['thesis']}</p>
    </div>
    <div class='card'>
      <h2>Board Stats</h2>
      <ul>
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
      <ul>
        <li>Compare the full board before approving any single paper bet.</li>
        <li>Approve only bets that still feel strong after cross-city comparison.</li>
        <li>Record abstentions when the board lacks clean edge.</li>
      </ul>
    </div>
  </div>
</body>
</html>
"""


def generate_daily_strategy_package(
    *,
    strategy_date_local: date,
    thesis: str,
    focus_cities: list[str] | tuple[str, ...],
    artifacts_dir: str | Path | None = None,
    selection_framework: dict[str, Any] | None = None,
    notes: dict[str, Any] | None = None,
    db_path: str | Path | None = None,
) -> dict[str, Any]:
    strategy_id = create_strategy_session(
        strategy_date_local=strategy_date_local,
        thesis=thesis,
        focus_cities=focus_cities,
        selection_framework=selection_framework or DEFAULT_SELECTION_FRAMEWORK,
        notes=notes or {},
        db_path=db_path,
    )
    board_count = populate_strategy_market_board(
        strategy_id=strategy_id,
        strategy_date_local=strategy_date_local,
        focus_cities=focus_cities,
        db_path=db_path,
    )
    board_rows = fetch_strategy_board(strategy_id=strategy_id, db_path=db_path)
    summary = summarize_strategy_board(board_rows=board_rows, focus_cities=focus_cities, thesis=thesis)

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
    }
    json_path.write_text(json.dumps(json_payload, indent=2))
    md_path.write_text(render_daily_strategy_markdown(strategy_id=strategy_id, strategy_date_local=strategy_date_local, summary=summary))
    html_path.write_text(render_daily_strategy_html(strategy_id=strategy_id, strategy_date_local=strategy_date_local, summary=summary))

    return {
        'strategy_id': strategy_id,
        'board_count': board_count,
        'summary': summary,
        'board_rows': board_rows,
        'json_path': str(json_path),
        'markdown_path': str(md_path),
        'html_path': str(html_path),
    }
