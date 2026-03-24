from __future__ import annotations

import argparse
from datetime import date, timedelta
import subprocess
import sys

from weatherlab.forecast.asos import STATION_IDS, fetch_station_daily_high, station_metadata_for_city
from weatherlab.live.live_orders import fetch_live_orders
from weatherlab.pipeline._markets import (
    display_name_for_city,
    format_bucket_label,
    outcome_for_observed_high,
    parse_weather_market,
)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Review yesterday weather market settlements.')
    parser.add_argument('--date', default=None, help='Optional settlement date in YYYY-MM-DD format')
    parser.add_argument('--db-path', default=None, help='Optional DuckDB path override')
    parser.add_argument('--notify', action='store_true', help='Send the report via openclaw system event')
    return parser.parse_args(argv)


def _default_target_date() -> date:
    return date.today() - timedelta(days=1)


def _format_report(target_date: date, grouped_rows: list[tuple[str, list[dict]]]) -> str:
    lines = [f'📊 SETTLEMENT REPORT - {target_date.strftime("%B")} {target_date.day}', '']

    if not grouped_rows:
        lines.append('No filled live orders matched the requested settlement date.')
        return '\n'.join(lines)

    for city_key, rows in grouped_rows:
        metadata = station_metadata_for_city(city_key)
        observed_high = fetch_station_daily_high(STATION_IDS[city_key], target_date)
        observed_text = 'unavailable' if observed_high is None else f'{observed_high:.1f}F'
        lines.append(f'{metadata.station_id} ({display_name_for_city(city_key)}): observed high = {observed_text}')

        if observed_high is None:
            lines.append('  Observation data unavailable; do not settle yet.')
            lines.append('')
            continue

        for row in rows:
            market = parse_weather_market({'ticker': row['ticker'], 'title': row['ticker']})
            if market is None:
                lines.append(f"  {row['ticker']} -> unable to parse ticker; settle manually")
                continue

            outcome = outcome_for_observed_high(observed_high, market)
            if outcome is None:
                lines.append(f"  {row['ticker']} ({market.label}) -> unable to evaluate")
                continue

            outcome_label = 'YES' if outcome else 'NO'
            note = f'{metadata.station_id} observed {observed_high:.1f}F'
            lines.append(
                f"  {row['ticker']} ({format_bucket_label(market.operator, market.threshold_low_f, market.threshold_high_f)}) -> "
                f"{outcome_label} (high was {observed_high:.1f}F)"
            )
            lines.append(
                f"  -> settle_live_order('{row['kalshi_order_id']}', '{'yes' if outcome else 'no'}', '{note}')"
            )
        lines.append('')

    return '\n'.join(lines).rstrip()


def _send_notification(text: str) -> int:
    completed = subprocess.run(
        ['openclaw', 'system', 'event', '--text', text, '--mode', 'now'],
        check=False,
    )
    return int(completed.returncode)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    target_date = date.fromisoformat(args.date) if args.date else _default_target_date()

    live_orders = [
        row
        for row in fetch_live_orders(
            db_path=args.db_path,
            status_filter=['pending', 'resting', 'executed'],
        )
        if int(row.get('fill_count') or 0) > 0
    ]

    grouped: dict[str, list[dict]] = {}
    for row in live_orders:
        market = parse_weather_market({'ticker': row['ticker'], 'title': row['ticker']})
        if market is None or market.market_date_local != target_date:
            continue
        grouped.setdefault(market.city_key, []).append(row)

    grouped_rows = sorted(grouped.items(), key=lambda item: item[0])
    report = _format_report(target_date, grouped_rows)

    if args.notify:
        return _send_notification(report)

    print(report)
    return 0


if __name__ == '__main__':
    raise SystemExit(main(sys.argv[1:]))
