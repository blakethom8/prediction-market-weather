from __future__ import annotations

import argparse
from datetime import date, timedelta
import subprocess
import sys

from weatherlab.pipeline.learning import (
    append_insights_to_file,
    format_settlement_notification,
    generate_insights_text,
    run_settlement_and_learning,
    write_daily_memory,
)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Settle yesterday weather markets and capture learning.')
    parser.add_argument('--date', default=None, help='Optional settlement date in YYYY-MM-DD format')
    parser.add_argument('--db-path', default=None, help='Optional DuckDB path override')
    parser.add_argument('--notify', action='store_true', help='Send the report via openclaw system event')
    parser.add_argument('--auto-insights', action='store_true', help='Append generated insights to docs/BETTING_INSIGHTS.md')
    parser.add_argument('--update-memory', action='store_true', help='Append settlement memory to ~/.openclaw/workspace/memory')
    return parser.parse_args(argv)


def _default_target_date() -> date:
    return date.today() - timedelta(days=1)


def _send_notification(text: str) -> int:
    completed = subprocess.run(
        ['openclaw', 'system', 'event', '--text', text, '--mode', 'now'],
        check=False,
    )
    return int(completed.returncode)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    target_date = date.fromisoformat(args.date) if args.date else _default_target_date()

    settlement_report = run_settlement_and_learning(target_date, db_path=args.db_path)
    insights_updated = False
    if args.auto_insights:
        insights_text = generate_insights_text(settlement_report)
        append_insights_to_file(insights_text)
        settlement_report['insights_text'] = insights_text
        insights_updated = True

    if args.update_memory:
        write_daily_memory(settlement_report)

    report = format_settlement_notification(settlement_report, insights_updated=insights_updated)
    if args.notify:
        return _send_notification(report)

    print(report)
    return 0


if __name__ == '__main__':
    raise SystemExit(main(sys.argv[1:]))
