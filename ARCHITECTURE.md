# Prediction Market Weather Architecture

This repo started as a research and backtest project. The primary architecture now is a live, paper-first weather betting platform backed by a DuckDB warehouse and a local FastAPI app.

Historical research still matters, but it is no longer the main product surface.

## System Shape

```text
Kalshi weather API
  -> live market ingest
  -> DuckDB core market tables

Forecast sources
  -> forecast ingest
  -> DuckDB core forecast tables

Settlement sources
  -> settlement ingest
  -> DuckDB core settlement tables

core tables
  -> features.v_training_rows
  -> features.contract_training_rows
  -> features.v_daily_market_board

daily board
  -> ops.strategy_sessions
  -> ops.strategy_market_board
  -> ops.bet_proposals
  -> review / approval events
  -> ops.paper_bets
  -> settlement reviews

ops history views
  -> FastAPI app
  -> daily artifacts
  -> historical learning dashboard
```

## DuckDB Layers

### `raw`

Landing zone for source payloads.

Key tables:

- `raw.kalshi_markets`
- `raw.kalshi_market_snapshots`
- `raw.weather_forecasts`
- `raw.weather_observations`
- `raw.weather_settlement_reports`

### `core`

Normalized warehouse layer used by both research and live workflows.

Key tables:

- `core.cities`
- `core.weather_stations`
- `core.weather_contracts`
- `core.market_snapshots`
- `core.forecast_snapshots`
- `core.forecast_distributions`
- `core.settlement_observations`

### `features`

Point-in-time modeling and board-generation layer.

Key tables and views:

- `features.v_training_rows`
- `features.contract_training_rows`
- `features.v_latest_market_training_rows`
- `features.v_daily_market_board`

`features.v_daily_market_board` is the live board input. It ranks the latest row per market and buckets names into `priority`, `watch`, or `pass`.

### `ops`

Workflow, review, paper betting, and learning layer.

Key tables:

- `ops.strategy_sessions`
- `ops.strategy_market_board`
- `ops.strategy_review_events`
- `ops.bet_proposals`
- `ops.bet_proposal_events`
- `ops.paper_bets`
- `ops.paper_bet_reviews`
- `ops.pipeline_runs`
- `ops.decision_journal`
- `ops.bet_executions`
- `ops.bet_reviews`

Key views:

- `ops.v_strategy_proposal_outcomes`
- `ops.v_strategy_board_learning_history`
- `ops.v_paper_bet_history`
- `ops.v_strategy_session_learning`

`ops.bet_executions` exists for real execution records, but the default operating workflow is still paper betting.

## Data Pipeline

### 1. Kalshi live ingest

`src/weatherlab/ingest/kalshi_live.py` and `src/weatherlab/ingest/kalshi_live_sync.py`:

- authenticate against `https://api.elections.kalshi.com/trade-api/v2`
- sign requests with RSA-PSS
- fetch open weather markets
- normalize contract metadata and market prices
- write contract rows into `core.weather_contracts`
- write market snapshots into `core.market_snapshots`
- rematerialize `features.contract_training_rows`

The current live sync path writes normalized core rows directly. The `raw` schema still exists for source payload landing and historical ingest paths, but it is not the primary live entry point.

### 2. Forecast ingest

Forecast sources write into:

- `core.forecast_snapshots`
- `core.forecast_distributions`

Current repo paths include:

- Open-Meteo helpers for forecast snapshots
- historical archive work under `weatherlab.ingest.historical_forecasts`
- archived NWS text work under `weatherlab.ingest.archived_nws_forecasts`

The live board depends on this layer being populated. Without current forecast snapshots, the board can show markets but not useful fair values.

### 3. Settlement ingest

Settlement truth writes into `core.settlement_observations`.

That layer supports:

- historical evaluation
- learning views
- paper bet review
- future calibration work

### 4. Feature materialization

`sql/views/001_training_view.sql` builds `features.v_training_rows` by joining:

- contracts
- market snapshots
- latest available forecast snapshot at decision time
- settlement truth

`src/weatherlab/build/training_rows.py` then materializes `features.contract_training_rows`.

### 5. Daily board build

`sql/views/004_live_betting_views.sql` defines `features.v_daily_market_board`, which:

- selects the latest point-in-time row per market
- carries fair probability and edge metrics forward
- ranks candidates across the day
- assigns `priority`, `watch`, or `pass`

This is the board consumed by the live workflow.

## Live Workflow

The live workflow lives under `src/weatherlab/live/`.

Canonical flow:

1. Create a strategy session in `ops.strategy_sessions`
2. Copy the relevant board rows into `ops.strategy_market_board`
3. Generate candidate proposals in `ops.bet_proposals`
4. Record review and approval changes in `ops.strategy_review_events` and `ops.bet_proposal_events`
5. Convert approved names into `ops.paper_bets`
6. Settle paper bets and write `ops.paper_bet_reviews`
7. Read the learning views back through the web app and history dashboard

Important fields carried through the workflow:

- `strategy_variant`
- `scenario_label`
- `forecast_snapshot_id`
- board rank and bucket
- thesis and rationale payloads

Those fields are what make two-strategy comparison and later learning possible.

## Web App

The app lives in `src/weatherlab/live/web/`.

Routes:

- `/` and `/today`
  - current board summary
  - top recommendations
  - proposal and approval state
  - recent sessions and paper bets
- `/board`
  - latest captured board
- `/board/{strategy_date}`
  - a specific date's board scan and available runs
- `/strategies/{strategy_id}`
  - one session's thesis, board summary, proposals, review events, and linked paper bets
- `/paper-bets`
  - open exposure plus settled outcomes and lessons
- `/history`
  - historical learning by strategy, variant, scenario, city, approval outcome, time to close, and expected edge band
- `/healthz`
  - schema readiness check for the app's required tables and views

The app is currently read-oriented. It presents workflow state already stored in DuckDB; it does not yet perform approvals, conversions, settlement, or live order management through the browser.

## Module Layout

- `src/weatherlab/ingest/`
  - API clients and source ingest paths
- `src/weatherlab/build/`
  - bootstrap, registry load, materialization, promotion helpers
- `src/weatherlab/live/`
  - strategy workflow, persistence, queries, CLI entry points
- `src/weatherlab/live/web/`
  - FastAPI app, templates, static assets
- `src/weatherlab/research/`
  - research-facing entry points for replay and evaluation
- `src/weatherlab/ops/`
  - compatibility imports for older live paths
- `src/weatherlab/parse/`
  - contract parsing and audits

## Key Config And State Files

- `config/cities.yml`
  - city registry used for canonical city IDs and default primary stations
- `config/stations.yml`
  - station registry used for settlement alignment and future airport-specific forecast work
- `.env`
  - local environment config, including Kalshi API settings and DuckDB path override
- `.kalshi_private_key.pem`
  - gitignored private key for Kalshi request signing
- `artifacts/daily-strategy/`
  - generated JSON, Markdown, and HTML summaries for daily strategy runs

## What Is No Longer Primary

- Historical parquet extraction still exists for research and replay, but it is not the primary architecture to understand first.
- The repo should not be described as a parquet-first pipeline with a small experimental live layer attached.
- The main operating model is now live board generation plus paper-trading workflow, with research and historical evaluation supporting that loop.
