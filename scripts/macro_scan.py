from __future__ import annotations

import argparse
from datetime import UTC, datetime
import subprocess
import sys
from typing import Any

from weatherlab.ingest.kalshi_live import KalshiClient

MAX_YES_ASK = 0.15
MIN_YES_ASK = 0.01  # exclude 0c delisted/near-settled markets
MIN_VOLUME = 500.0


def _normalize_price(value: Any) -> float | None:
    if value in (None, ''):
        return None
    price = float(value)
    return price / 100.0 if price > 1.0 else price


def _normalize_volume(value: Any) -> float | None:
    if value in (None, ''):
        return None
    return float(value)


def _days_to_close(close_time: Any, now_utc: datetime) -> float | None:
    if close_time in (None, ''):
        return None
    if isinstance(close_time, str):
        normalized = close_time.strip()
        if normalized.endswith('Z'):
            normalized = normalized[:-1] + '+00:00'
        close_dt = datetime.fromisoformat(normalized)
    elif isinstance(close_time, datetime):
        close_dt = close_time
    else:
        return None

    if close_dt.tzinfo is None:
        close_dt = close_dt.replace(tzinfo=UTC)
    close_dt = close_dt.astimezone(UTC)
    return round((close_dt - now_utc).total_seconds() / 86_400.0, 1)


def filter_coldmath_candidates(
    markets: list[dict[str, Any]],
    *,
    now_utc: datetime | None = None,
    max_yes_ask: float = MAX_YES_ASK,
    min_volume: float = MIN_VOLUME,
) -> list[dict[str, Any]]:
    resolved_now = now_utc or datetime.now(UTC)
    candidates: list[dict[str, Any]] = []

    for market in markets:
        yes_ask = _normalize_price(market.get('yes_ask'))
        volume = _normalize_volume(market.get('volume'))
        if yes_ask is None or volume is None:
            continue
        if yes_ask < MIN_YES_ASK or yes_ask > max_yes_ask or volume <= min_volume:
            continue

        candidates.append(
            {
                'ticker': market.get('ticker'),
                'title': market.get('title') or market.get('ticker'),
                'yes_ask': round(yes_ask, 4),
                'volume': volume,
                'days_to_close': _days_to_close(market.get('close_time'), resolved_now),
            }
        )

    return sorted(
        candidates,
        key=lambda row: (
            row['yes_ask'],
            -(row['volume'] or 0.0),
            row.get('days_to_close') if row.get('days_to_close') is not None else float('inf'),
            row.get('ticker') or '',
        ),
    )


def _format_price(value: float | None) -> str:
    if value is None:
        return 'n/a'
    return f'{int(round(value * 100))}c'


def _format_volume(value: float | None) -> str:
    if value is None:
        return 'n/a'
    if float(value).is_integer():
        return f'{int(value):,}'
    return f'{value:,.1f}'


def _format_days(value: float | None) -> str:
    if value is None:
        return 'n/a'
    return f'{value:.1f}'


def format_macro_report(candidates: list[dict[str, Any]], *, scan_time_utc: datetime | None = None) -> str:
    resolved_scan_time = (scan_time_utc or datetime.now(UTC)).astimezone(UTC)
    lines = [
        f'MACRO KALSHI SCAN - {resolved_scan_time.isoformat(timespec="seconds").replace("+00:00", "Z")}',
        '',
        f'Filter: yes_ask <= {_format_price(MAX_YES_ASK)} and volume > {_format_volume(MIN_VOLUME)}',
        'Human review only. No recommendations generated.',
        '',
        'CANDIDATES:',
    ]

    if not candidates:
        lines.append('No low-priced liquid open markets found.')
        return '\n'.join(lines)

    for row in candidates:
        lines.append(
            f"- {row.get('ticker') or 'UNKNOWN'} | {row.get('title') or 'Untitled'}"
        )
        lines.append(
            f"  yes_ask={_format_price(row.get('yes_ask'))} | "
            f"volume={_format_volume(row.get('volume'))} | "
            f"days_to_close={_format_days(row.get('days_to_close'))}"
        )
    return '\n'.join(lines)


def run_macro_scan(client: KalshiClient | None = None) -> dict[str, Any]:
    scan_time_utc = datetime.now(UTC).replace(microsecond=0)
    resolved_client = client or KalshiClient(timeout_seconds=15.0)
    markets = resolved_client.fetch_open_markets()
    candidates = filter_coldmath_candidates(markets, now_utc=scan_time_utc)
    return {
        'scan_time_utc': scan_time_utc,
        'candidates': candidates,
        'market_count': len(markets),
    }


def _send_notification(text: str) -> int:
    completed = subprocess.run(
        ['openclaw', 'system', 'event', '--text', text, '--mode', 'now'],
        check=False,
    )
    return int(completed.returncode)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Run the macro Kalshi ColdMath candidate scan.')
    parser.add_argument('--notify', action='store_true', help='Send the report via openclaw system event')
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    scan = run_macro_scan()
    report = format_macro_report(scan['candidates'], scan_time_utc=scan['scan_time_utc'])

    if args.notify:
        return _send_notification(report)

    print(report)
    return 0


if __name__ == '__main__':
    raise SystemExit(main(sys.argv[1:]))
