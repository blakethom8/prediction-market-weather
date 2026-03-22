from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

from .workflow import generate_daily_strategy_package


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Generate a daily betting strategy package.')
    parser.add_argument('--date', dest='strategy_date', required=True, help='Strategy date (YYYY-MM-DD)')
    parser.add_argument('--cities', default='nyc,chi', help='Comma-separated focus cities')
    parser.add_argument('--thesis', required=True, help='Daily strategy thesis')
    parser.add_argument('--artifacts-dir', default='artifacts/daily-strategy', help='Where to write strategy artifacts')
    parser.add_argument('--db-path', default=None, help='Optional DuckDB path override')
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    strategy_date_local = date.fromisoformat(args.strategy_date)
    focus_cities = [city.strip().lower() for city in args.cities.split(',') if city.strip()]
    result = generate_daily_strategy_package(
        strategy_date_local=strategy_date_local,
        thesis=args.thesis,
        focus_cities=focus_cities,
        artifacts_dir=Path(args.artifacts_dir),
        db_path=args.db_path,
    )
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
