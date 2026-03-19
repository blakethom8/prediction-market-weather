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

## Current Audit Tooling
- `scripts/run_parser_audit.py <input.csv> [output.json]` will batch-audit market titles
- `src/weatherlab/build/registry_loader.py` loads city/station registries into DuckDB
- tests now cover parser behavior, parser audit summaries, schema bootstrap, registry loading, and decision logging
