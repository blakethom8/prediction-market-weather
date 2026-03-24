from __future__ import annotations

import argparse
from datetime import date
import subprocess
import sys

from weatherlab.pipeline.morning_scan import format_scan_report, run_morning_scan


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Run the morning weather market scan.')
    parser.add_argument('--date', default=None, help='Optional scan date in YYYY-MM-DD format')
    parser.add_argument('--db-path', default=None, help='Unused placeholder for interface compatibility')
    parser.add_argument('--notify', action='store_true', help='Send the report via openclaw system event')
    parser.add_argument('--include-all', action='store_true', help='Include skipped cities in the scan report')
    parser.add_argument('--validate-only', action='store_true', help='Print station forecast validation without trade proposals')
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
    report = (
        _format_validation_only(scan_results)
        if args.validate_only
        else format_scan_report(scan_results, include_all=args.include_all)
    )

    if args.notify:
        return _send_notification(report)

    print(report)
    return 0


if __name__ == '__main__':
    raise SystemExit(main(sys.argv[1:]))
