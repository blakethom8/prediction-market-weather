from __future__ import annotations

import argparse
from datetime import date
import subprocess
import sys

from weatherlab.pipeline.auto_bet import (
    evaluate_all_auto_bet_candidates,
    format_auto_bet_notification,
    format_no_auto_bet_notification,
    run_auto_betting_session,
)
from weatherlab.pipeline.morning_scan import format_scan_report, run_morning_scan


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Run the morning weather market scan.')
    parser.add_argument('--date', default=None, help='Optional scan date in YYYY-MM-DD format')
    parser.add_argument('--db-path', default=None, help='Optional DuckDB path override for budget/order tracking')
    parser.add_argument('--notify', action='store_true', help='Send the report via openclaw system event')
    parser.add_argument('--include-all', action='store_true', help='Include skipped cities in the scan report')
    parser.add_argument('--coldmath', dest='coldmath', action='store_true', default=True, help='Include the ColdMath layer alongside the edge scan')
    parser.add_argument('--edge-only', dest='coldmath', action='store_false', help='Disable the ColdMath layer')
    parser.add_argument('--validate-only', action='store_true', help='Print station forecast validation without trade proposals')
    parser.add_argument('--auto-bet', action='store_true', help='Place real Kalshi bets when all guardrails pass')
    return parser.parse_args(argv)


def _format_validation_only(scan_results: dict) -> str:
    lines = [f'🌤 FORECAST VALIDATION - {scan_results["scan_date"]}', '']
    for row in scan_results.get('cities', {}).values():
        forecast_high = 'n/a' if row.get('forecast_high_f') is None else f"{row['forecast_high_f']}°F"
        observed_max = (
            'n/a'
            if row.get('observed_max_so_far_f') is None
            else f"{row['observed_max_so_far_f']}°F"
        )
        lines.append(
            f"{row['city_name']} / {row['station_id']}: "
            f"NWS={forecast_high}, "
            f"obs max={observed_max}, "
            f"confidence={row.get('forecast_confidence')}"
        )
        note = row.get('validation_note')
        if note:
            lines.append(f'  {note}')
    return '\n'.join(lines)


def _send_notification(text: str) -> int:
    completed = subprocess.run(
        ['openclaw', 'system', 'event', '--text', text, '--mode', 'now'],
        check=False,
    )
    return int(completed.returncode)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    target_date = date.fromisoformat(args.date) if args.date else None
    scan_results = run_morning_scan(target_date=target_date, db_path=args.db_path)
    if not args.coldmath:
        scan_results = dict(scan_results)
        scan_results['coldmath_plays'] = []
    if args.validate_only:
        report = _format_validation_only(scan_results)
    elif args.auto_bet:
        placed_bets = run_auto_betting_session(scan_results, db_path=args.db_path)
        if placed_bets:
            report = format_auto_bet_notification(scan_results, placed_bets, db_path=args.db_path)
        else:
            report = format_no_auto_bet_notification(
                scan_results,
                evaluate_all_auto_bet_candidates(scan_results, db_path=args.db_path),
            )
    else:
        report = format_scan_report(scan_results, include_all=args.include_all)

    if args.notify:
        return _send_notification(report)

    print(report)
    return 0


if __name__ == '__main__':
    raise SystemExit(main(sys.argv[1:]))
