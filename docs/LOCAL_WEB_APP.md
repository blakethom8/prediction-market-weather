# Local Live Web App

The first betting-platform UI is a local FastAPI app with server-rendered templates.

## What It Includes

- `/` dashboard for recent strategy sessions and paper-bet status
- `/board` for the latest captured daily board
- `/board/<YYYY-MM-DD>` for a specific strategy date
- `/strategies/<strategy_id>` for strategy summary, proposals, review history, and linked paper bets
- `/paper-bets` for open and closed paper-bet review
- `/healthz` for a simple local health check

## Run Locally

First install project dependencies:

```bash
./scripts/setup_env.sh
```

Bootstrap the database if needed:

```bash
./scripts/bootstrap_db.sh
```

Generate at least one daily strategy package so the pages have real content:

```bash
make daily-board -- --date 2026-03-23 --research-cities nyc,chi --thesis "Compare the full daily board before approving paper bets."
```

Start the app:

```bash
make live-web
```

That serves on `0.0.0.0:8000` by default. For a direct module run:

```bash
PYTHONPATH=src .venv/bin/python -m weatherlab.live.web --host 0.0.0.0 --port 8000 --reload
```

If your DuckDB warehouse lives somewhere else, set:

```bash
export WEATHER_WAREHOUSE_PATH=/path/to/weather_markets.duckdb
```

Recommended fresh-start sequence:

```bash
./scripts/setup_env.sh
export WEATHER_WAREHOUSE_PATH="$PWD/data/warehouse/weather_markets.duckdb"
make bootstrap
make daily-board -- --date 2026-03-23 --research-cities nyc,chi --thesis "Compare the full daily board before approving paper bets."
make live-web
```

Notes:
- `make daily-board` creates a strategy session even if the board is empty for that date, so the operator console can still capture the day, approval status, and later review lineage.
- `/healthz` now checks that the live tables and views required by the app are present, so it catches schema/view drift instead of reporting process-only health.
- The web app remains intentionally server-rendered and local-first; use Telegram for push summaries and the FastAPI UI for board scanning, strategy review, paper bets, and post-trade review.

## Local/Tailscale Notes

- Serving on `0.0.0.0` lets the Mac mini expose the app over the Tailscale IP or MagicDNS name.
- The app is intentionally local-app oriented: no cloud auth, no public deployment assumptions, no API token handling in the UI yet.
- Telegram remains a better push layer for summaries and alerts; this app is the review and inspection surface.
