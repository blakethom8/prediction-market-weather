# Prediction Market Weather

Prediction Market Weather is a local, paper-first weather prediction market betting assistant for Kalshi weather contracts. It syncs live Kalshi markets, stores point-in-time market and forecast data in DuckDB, ranks the current board, records strategy sessions and proposals, tracks paper bets and lessons, and serves a FastAPI review app.

Real order placement has been tested, but the default workflow is still paper bets first. The goal is disciplined daily decision-making with a clean audit trail, not hiding the process behind premature automation.

## What The Project Does Today

- Syncs live weather markets from `https://api.elections.kalshi.com/trade-api/v2`
- Signs Kalshi API requests with RSA-PSS using a local `.kalshi_private_key.pem`
- Stores data in a DuckDB warehouse with `raw`, `core`, `features`, and `ops` layers
- Builds a ranked daily board from `features.v_daily_market_board`
- Runs a paper betting workflow: strategy session -> board -> proposals -> review -> paper bets -> settlement review
- Serves a local FastAPI app with Today, Board, Strategy Session, Paper Bets, and History pages
- Tracks `strategy_variant` and `scenario_label` so runs can be compared later

## Quick Start

### 1. Create a virtualenv and install the package

```bash
python3 -m venv .venv
.venv/bin/pip install -e .
```

Equivalent helper:

```bash
./scripts/setup_env.sh
```

### 2. Create `.env`

```bash
KALSHI_API_KEY_ID=your-key-id
KALSHI_API_BASE_URL=https://api.elections.kalshi.com/trade-api/v2
KALSHI_API_PRIVATE_KEY_PATH=.kalshi_private_key.pem
WEATHER_WAREHOUSE_PATH=data/warehouse/weather_markets.duckdb
```

Notes:

- `KALSHI_API_PRIVATE_KEY_PATH` defaults to `.kalshi_private_key.pem`
- `.env` is loaded automatically by `src/weatherlab/settings.py`
- The private key file is gitignored

### 3. Put the Kalshi private key in the repo root

```bash
chmod 600 .kalshi_private_key.pem
```

The client expects an RSA private key and signs requests with RSA-PSS padding.

### 4. Bootstrap the DuckDB schema

```bash
make bootstrap
```

On a fresh warehouse, also load the static city and station registry:

```bash
PYTHONPATH=src .venv/bin/python -m weatherlab.build.registry_loader
```

### 5. Sync live Kalshi weather markets

```bash
make fetch-live
```

This writes live contract metadata and market snapshots into DuckDB, rematerializes training rows, and refreshes `features.v_daily_market_board`.

### 6. Create a daily strategy session

```bash
make daily-board -- --date 2026-03-23 --research-cities nyc,chi --strategy-variant baseline --thesis "Scan the full live board before isolating any single contract."
```

This creates:

- `ops.strategy_sessions`
- `ops.strategy_market_board`
- `ops.bet_proposals`
- strategy artifacts in `artifacts/daily-strategy/`

### 7. Start the web app

```bash
make live-web
```

Open `http://127.0.0.1:8000`.

## Key Make Targets

- `make setup` creates `.venv` and installs the project
- `make bootstrap` creates DuckDB schemas and views
- `make fetch-live` syncs open Kalshi weather markets into DuckDB
- `make daily-board -- --date YYYY-MM-DD --thesis "..."` creates a strategy session and proposal set
- `make live-web` runs the FastAPI app on `0.0.0.0:8000`
- `make test` runs the unit test suite
- `make extract-kalshi` imports historical Kalshi archive data
- `make promote` promotes raw historical data into normalized core tables
- `make backfill-forecasts` backfills historical forecast data
- `make run-eval` runs the historical evaluator

## Web App

- `/` and `/today` show the current day board, recommendations, proposal state, recent sessions, and recent paper bets
- `/board` shows the latest captured board
- `/board/{YYYY-MM-DD}` shows a specific strategy date
- `/strategies/{strategy_id}` shows one strategy session, its proposals, review events, and linked paper bets
- `/paper-bets` shows open exposure plus settled outcomes and lessons
- `/history` shows historical learning grouped by strategy, variant, scenario, city, approval outcome, time to close, and edge band
- `/healthz` checks that the required live tables and views exist

## Architecture At A Glance

Live path:

1. Kalshi API -> `weatherlab.ingest.kalshi_live_sync`
2. DuckDB core market data
3. Forecast snapshots in `core.forecast_snapshots`
4. `features.v_training_rows` -> `features.contract_training_rows`
5. `features.v_daily_market_board`
6. `ops.strategy_sessions` / `ops.bet_proposals` / `ops.paper_bets`
7. FastAPI app and history views

Historical and learning path:

1. Historical market and forecast ingestion
2. Settlement truth
3. Point-in-time training rows
4. Evaluation and learning views
5. `/history` analytics

More detail:

- [`ARCHITECTURE.md`](ARCHITECTURE.md)
- [`docs/LOCAL_WEB_APP.md`](docs/LOCAL_WEB_APP.md)
- [`OBSERVABILITY.md`](OBSERVABILITY.md)
- [`docs/HISTORICAL_FORECAST_ARCHIVE_PLAN.md`](docs/HISTORICAL_FORECAST_ARCHIVE_PLAN.md)

## Current Boundaries

- The web app is read-oriented today. It shows workflow state from DuckDB, but it does not yet expose write actions for approval, conversion, settlement, or live order management.
- Paper bets are the primary workflow.
- Real order placement is a completed milestone, not the default operating path.
- Board quality depends on having current forecast snapshots in `core.forecast_snapshots`; `make fetch-live` only handles the Kalshi side of the board.
