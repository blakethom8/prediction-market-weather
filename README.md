# Prediction Market Weather

Weather-first research and trading infrastructure for Kalshi-style prediction markets.

## Thesis
Estimate reality first, then compare it to market prices, then decide whether any edge survives execution.

## MVP Scope
- Kalshi daily weather markets first
- Clean warehouse in DuckDB
- Forecast snapshots + settlement truth + market snapshots
- Fixed `eval.py`, editable signal logic under `weatherlab.signal`
- Transparent decision journaling for bet review and future agent audits

## Core Principle
Forecast first, bid second.

## Developer Workflow

### Setup
```bash
./scripts/setup_env.sh
```

### Bootstrap the warehouse
```bash
./scripts/bootstrap_db.sh
```

### Run self-tests
```bash
make test
```

`make` targets prefer `.venv/bin/python` when that environment exists and run against `src/` directly, so the local test loop does not depend on reinstalling the package.

## Current Status
The repo now includes:
- DuckDB schema scaffolding
- a contract parser baseline for weather thresholds/buckets
- a minimal signal/evaluator baseline
- decision logging and rationale scaffolding
- unit tests covering parser behavior, evaluator scoring, schema bootstrap, registry loading, decision logging, and end-to-end training rows

## Near-Term Build Priorities
1. Expand contract parsing for real Kalshi weather titles/rules
2. Add city/station registry with settlement-aligned mappings
3. Implement forecast ingestion snapshots
4. Implement settlement truth ingestion
5. Build the first contract × timestamp training rows from real data

## Focus-City Workflow
For the honest historical pipeline, do not assume every city is equally ready.

Recommended first focus:
- `nyc`
- `chi`

These remain the default research-focus cities unless overridden. They are not a live-board filter.

You can restrict historical forecast backfills with:
```bash
WEATHER_FOCUS_CITIES=nyc,chi .venv/bin/python -m weatherlab.ingest.historical_forecasts
```

Archive-source planning notes live in:
- `docs/HISTORICAL_FORECAST_ARCHIVE_PLAN.md`

## Real Archived Forecast Ingestion
The repo now includes a first real issued-time historical forecast path for the focus cities:
- source: `iem-zfp`
- NYC via `ZFPOKX` Manhattan zone block
- Chicago via `ZFPLOT` Central Cook zone block

This is not the full NDFD archive path yet, but it is a real archived forecast source with issuance timestamps and city-targeted forecast text.

Run the backfill with:
```bash
PYTHONPATH=src .venv/bin/python -c "from weatherlab.ingest.archived_nws_forecasts import backfill_archived_nws_zone_forecasts; print(backfill_archived_nws_zone_forecasts())"
```

## Live / Paper Betting Architecture
The project is now also centered around a day-of operating loop:
- create a strategy session
- compare the full daily market board across available cities/contracts
- generate a daily strategy summary
- review / approve / adjust the strategy
- record paper bets with rationale
- settle and review outcomes

Key tables/views:
- `ops.strategy_sessions`
- `ops.strategy_review_events`
- `ops.strategy_market_board`
- `ops.bet_proposals`
- `ops.bet_proposal_events`
- `ops.paper_bets`
- `ops.paper_bet_reviews`
- `ops.v_strategy_proposal_outcomes`
- `ops.v_strategy_board_learning_history`
- `ops.v_paper_bet_history`
- `ops.v_strategy_session_learning`
- `features.v_daily_market_board`

Live workflow code now lives under:
- `src/weatherlab/live/`

This path should remain structurally separate from the historical research / ML path.

Current package split:
- `src/weatherlab/live/` for day-of workflow orchestration and persistence helpers
- `src/weatherlab/research/` for historical/replay/evaluation entry points
- `src/weatherlab/ops/` as a compatibility layer for older live workflow imports

Betting platform system design doc:
- `docs/BETTING_PLATFORM_ARCHITECTURE.md`

Generate a day-of package with:
```bash
make daily-board -- --date 2026-03-23 --research-cities nyc,chi --thesis "Compare the full daily board before approving paper bets."
```

The live board now scans all available markets by default. Use `--board-cities` only for targeted replays or debugging slices.

Run the first local web surface with:
```bash
make live-web
```

The app is FastAPI with server-rendered Jinja templates. Main routes:
- `/` dashboard landing page
- `/history` historical performance / strategy learning review
- `/board` latest daily board
- `/board/<YYYY-MM-DD>` board for a specific strategy date
- `/strategies/<strategy_id>` strategy session summary
- `/paper-bets` paper bet / review page

More local run notes, including Tailscale-oriented usage, live in:
- `docs/LOCAL_WEB_APP.md`

Recommended local boot sequence for a fresh DuckDB file:
```bash
./scripts/setup_env.sh
export WEATHER_WAREHOUSE_PATH="$PWD/data/warehouse/weather_markets.duckdb"
make bootstrap
make daily-board -- --date 2026-03-23 --research-cities nyc,chi --thesis "Compare the full daily board before approving paper bets."
make live-web
```

If there is not yet any market data for that strategy date, the daily package still creates a reviewable strategy session and the web app renders the empty-state board, strategy, paper-bet, and health pages cleanly.

Use city-level coverage diagnostics to see which cities currently have:
- enough contracts/snapshots
- official settlement coverage
- live-ish forecast coverage vs archive-only proxy coverage

## Current Audit Tooling
- `scripts/run_parser_audit.py <input.csv> [output.json]` will batch-audit market titles
- `src/weatherlab/build/registry_loader.py` loads city/station registries into DuckDB
- tests now cover parser behavior, parser audit summaries, schema bootstrap, registry loading, and decision logging
