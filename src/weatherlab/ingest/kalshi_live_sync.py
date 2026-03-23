from __future__ import annotations

import argparse
from datetime import UTC, datetime
import logging
from pathlib import Path
import sys
from typing import Any

from ..build.training_rows import materialize_training_rows
from ..db import connect
from .contracts import ingest_contract
from .kalshi_live import KalshiClient, KalshiClientError
from .market_snapshots import ingest_market_snapshot

logger = logging.getLogger(__name__)


def _minutes_to_close(close_time: datetime | None, ts_utc: datetime) -> int | None:
    if close_time is None:
        return None
    delta_seconds = int((close_time - ts_utc).total_seconds())
    return max(0, delta_seconds // 60)


def _fetch_existing_contract_tickers(
    tickers: list[str],
    *,
    db_path: str | Path | None = None,
) -> set[str]:
    if not tickers:
        return set()
    placeholders = ', '.join('?' for _ in tickers)
    con = connect(db_path=db_path)
    try:
        rows = con.execute(
            f'''
            select market_ticker
            from core.weather_contracts
            where market_ticker in ({placeholders})
            ''',
            tickers,
        ).fetchall()
    finally:
        con.close()
    return {row[0] for row in rows}


def _fetch_board_size(*, db_path: str | Path | None = None) -> int:
    con = connect(db_path=db_path)
    try:
        row = con.execute(
            'select count(*) from features.v_daily_market_board'
        ).fetchone()
    finally:
        con.close()
    return int(row[0] or 0)


def fetch_top_board_rows(
    *,
    limit: int = 5,
    db_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    con = connect(db_path=db_path)
    try:
        cursor = con.execute(
            '''
            select
                market_ticker,
                market_title,
                city_id,
                market_date_local,
                price_yes_ask,
                fair_prob,
                edge_vs_ask,
                candidate_bucket
            from features.v_daily_market_board
            order by edge_vs_ask desc nulls last, market_ticker
            limit ?
            ''',
            [limit],
        )
        columns = [column[0] for column in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    finally:
        con.close()


def sync_live_weather_markets(
    *,
    client: KalshiClient | None = None,
    db_path: str | Path | None = None,
) -> dict[str, int]:
    kalshi = client or KalshiClient()
    markets = kalshi.fetch_open_weather_markets()
    tickers = [str(market['ticker']) for market in markets if market.get('ticker')]
    existing_tickers = _fetch_existing_contract_tickers(tickers, db_path=db_path)

    snapshot_ts_utc = datetime.now(UTC)
    new_contracts = 0
    updated_contracts = 0
    snapshots_synced = 0

    for market in markets:
        ticker = str(market.get('ticker') or '').strip()
        if not ticker:
            continue

        if ticker in existing_tickers:
            updated_contracts += 1
        else:
            new_contracts += 1

        ingest_contract(
            market_ticker=ticker,
            event_ticker=market.get('event_ticker'),
            title=str(market.get('title') or ticker),
            rules_text=str(market.get('rules_text') or ''),
            close_time_utc=market.get('close_time'),
            settlement_time_utc=market.get('settlement_time'),
            status=str(market.get('status') or 'open'),
            platform='kalshi',
            db_path=db_path,
        )

        ingest_market_snapshot(
            market_ticker=ticker,
            ts_utc=snapshot_ts_utc,
            price_yes_bid=market.get('yes_bid'),
            price_yes_ask=market.get('yes_ask'),
            price_no_bid=market.get('no_bid'),
            price_no_ask=market.get('no_ask'),
            last_price=market.get('last_price'),
            volume=market.get('volume'),
            open_interest=market.get('open_interest'),
            minutes_to_close=_minutes_to_close(market.get('close_time'), snapshot_ts_utc),
            db_path=db_path,
        )
        snapshots_synced += 1

    materialize_training_rows(db_path=db_path)
    board_size = _fetch_board_size(db_path=db_path)
    return {
        'contracts_synced': len(tickers),
        'snapshots_synced': snapshots_synced,
        'board_size': board_size,
        'new_contracts': new_contracts,
        'updated_contracts': updated_contracts,
    }


def _stringify(value: Any) -> str:
    if value is None:
        return ''
    if hasattr(value, 'isoformat'):
        return value.isoformat()
    if isinstance(value, float):
        return f'{value:.4f}'
    return str(value)


def _format_table(rows: list[dict[str, Any]], columns: list[tuple[str, str]]) -> str:
    headers = [label for _, label in columns]
    widths = [len(header) for header in headers]
    rendered_rows: list[list[str]] = []

    for row in rows:
        rendered = [_stringify(row.get(key)) for key, _ in columns]
        rendered_rows.append(rendered)
        widths = [max(width, len(cell)) for width, cell in zip(widths, rendered)]

    def _render(values: list[str]) -> str:
        return ' | '.join(value.ljust(width) for value, width in zip(values, widths))

    lines = [_render(headers), '-+-'.join('-' * width for width in widths)]
    lines.extend(_render(values) for values in rendered_rows)
    return '\n'.join(lines)


def print_sync_report(
    summary: dict[str, int],
    *,
    top_rows: list[dict[str, Any]] | None = None,
) -> None:
    print('Kalshi live sync summary')
    print(
        _format_table(
            [summary],
            [
                ('contracts_synced', 'contracts'),
                ('snapshots_synced', 'snapshots'),
                ('new_contracts', 'new'),
                ('updated_contracts', 'updated'),
                ('board_size', 'board'),
            ],
        )
    )

    if top_rows is None:
        return

    print()
    print('Top board rows by edge')
    if not top_rows:
        print('(no board rows available)')
        return

    print(
        _format_table(
            top_rows,
            [
                ('market_ticker', 'ticker'),
                ('city_id', 'city'),
                ('price_yes_ask', 'yes_ask'),
                ('fair_prob', 'fair_prob'),
                ('edge_vs_ask', 'edge'),
                ('candidate_bucket', 'bucket'),
            ],
        )
    )


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Fetch live Kalshi weather markets into DuckDB.')
    parser.add_argument('--db-path', default=None, help='Optional DuckDB path override')
    parser.add_argument('--top', type=int, default=5, help='Number of board rows to print')
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
    )

    try:
        summary = sync_live_weather_markets(db_path=args.db_path)
    except KalshiClientError as exc:
        print(f'Kalshi live sync failed: {exc}', file=sys.stderr)
        return 1

    top_rows = fetch_top_board_rows(limit=max(0, args.top), db_path=args.db_path)
    print_sync_report(summary, top_rows=top_rows)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
