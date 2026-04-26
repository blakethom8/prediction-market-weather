from __future__ import annotations

import argparse
from datetime import date
import subprocess
import sys

from weatherlab.pipeline.morning_scan import (
    INTRADAY_CITY_WINDOWS,
    format_intraday_scan_report,
    run_intraday_scan,
)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Run the intraday threshold weather market scan.')
    parser.add_argument('--date', default=None, help='Optional scan date in YYYY-MM-DD format')
    parser.add_argument('--db-path', default=None, help='Optional DuckDB path override for future compatibility')
    parser.add_argument(
        '--window',
        action='append',
        choices=tuple(INTRADAY_CITY_WINDOWS),
        help='City window to scan. May be passed more than once.',
    )
    parser.add_argument('--notify', action='store_true', help='Send actionable reports via openclaw system event')
    return parser.parse_args(argv)


def _send_notification(text: str) -> int:
    completed = subprocess.run(
        ['openclaw', 'system', 'event', '--text', text, '--mode', 'now'],
        check=False,
    )
    return int(completed.returncode)


def _has_actionable_plays(scan_results: dict) -> bool:
    return bool(scan_results.get('intraday_plays'))


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    target_date = date.fromisoformat(args.date) if args.date else None
    scan_results = run_intraday_scan(target_date=target_date, windows=args.window, db_path=args.db_path)
    report = format_intraday_scan_report(scan_results)

    if args.notify:
        if not _has_actionable_plays(scan_results):
            print('No actionable intraday threshold plays found; notification suppressed.')
            return 0
        return _send_notification(report)

    print(report)
    return 0


if __name__ == '__main__':
    raise SystemExit(main(sys.argv[1:]))
