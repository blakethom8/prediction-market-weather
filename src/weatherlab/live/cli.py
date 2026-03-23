from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

from .workflow import generate_daily_strategy_package


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Generate a daily betting strategy package.')
    parser.add_argument('--date', dest='strategy_date', required=True, help='Strategy date (YYYY-MM-DD)')
    parser.add_argument(
        '--research-cities',
        '--cities',
        dest='research_cities',
        default='nyc,chi',
        help='Comma-separated research-focus cities; the live board still scans all markets unless --board-cities is set',
    )
    parser.add_argument(
        '--board-cities',
        default='',
        help='Optional comma-separated city filter for debugging or targeted runs; omit for the normal broad board',
    )
    parser.add_argument('--thesis', required=True, help='Daily strategy thesis')
    parser.add_argument('--artifacts-dir', default='artifacts/daily-strategy', help='Where to write strategy artifacts')
    parser.add_argument('--strategy-variant', default='baseline', help='Strategy variant label to store on sessions and proposals')
    parser.add_argument('--scenario', dest='scenario_label', default='live', help='Scenario label (for example: live, sandbox, replay)')
    parser.add_argument('--db-path', default=None, help='Optional DuckDB path override')
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    strategy_date_local = date.fromisoformat(args.strategy_date)
    research_cities = [city.strip().lower() for city in args.research_cities.split(',') if city.strip()]
    board_cities = [city.strip().lower() for city in args.board_cities.split(',') if city.strip()]
    result = generate_daily_strategy_package(
        strategy_date_local=strategy_date_local,
        thesis=args.thesis,
        research_focus_cities=research_cities,
        board_cities=board_cities,
        artifacts_dir=Path(args.artifacts_dir),
        strategy_variant=args.strategy_variant,
        scenario_label=args.scenario_label,
        db_path=args.db_path,
    )
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
