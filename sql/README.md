# SQL Layout

The DuckDB bootstrap applies SQL in this order:

1. `ddl/*.sql`
2. `views/*.sql`

## DDL Files

- `ddl/001_raw.sql`
  - raw landing tables for Kalshi markets, market snapshots, forecast payloads, observations, and settlement reports
- `ddl/002_core.sql`
  - normalized city, station, contract, market snapshot, forecast snapshot, and settlement tables
- `ddl/003_features.sql`
  - materialized training rows plus general ops tables
- `ddl/004_live_betting.sql`
  - strategy sessions, captured boards, proposals, proposal events, paper bets, and paper bet reviews

## View Files

- `views/001_training_view.sql`
  - point-in-time training rows with fair probability and edge calculations
- `views/002_provenance_views.sql`
  - provenance and source-tracing helpers
- `views/003_city_diagnostics.sql`
  - city-level coverage diagnostics
- `views/004_live_betting_views.sql`
  - latest board and proposal outcome views
- `views/005_historical_learning_views.sql`
  - board, paper bet, and strategy-session learning views used by `/history`

## Schema Layers

- `raw`
  - source payload landing zone
- `core`
  - normalized source-of-truth tables
- `features`
  - point-in-time features and board-generation views
- `ops`
  - workflow, audit, and learning tables/views

## Checks

- `checks/`
  - integrity and leakage checks
