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

These are now the default focus cities unless overridden.

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
- compare the full daily market board across cities
- generate a daily strategy summary
- review / approve / adjust the strategy
- record paper bets with rationale
- settle and review outcomes

Key tables/views:
- `ops.strategy_sessions`
- `ops.strategy_market_board`
- `ops.paper_bets`
- `features.v_daily_market_board`

Live workflow code now lives under:
- `src/weatherlab/live/`

This path should remain structurally separate from the historical research / ML path.

Betting platform system design doc:
- `docs/BETTING_PLATFORM_ARCHITECTURE.md`

Generate a day-of package with:
```bash
make daily-board -- --date 2026-03-23 --cities nyc,chi --thesis "Compare the full daily board before approving paper bets."
```

Use city-level coverage diagnostics to see which cities currently have:
- enough contracts/snapshots
- official settlement coverage
- live-ish forecast coverage vs archive-only proxy coverage

## Current Audit Tooling
- `scripts/run_parser_audit.py <input.csv> [output.json]` will batch-audit market titles
- `src/weatherlab/build/registry_loader.py` loads city/station registries into DuckDB
- tests now cover parser behavior, parser audit summaries, schema bootstrap, registry loading, and decision logging
