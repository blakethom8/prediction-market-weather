# Kalshi Weather Prediction Market

## Project Goal
Build an automated weather prediction system to profitably trade Kalshi weather markets using ML techniques, data engineering, and domain expertise. Core philosophy: **"forecast first, bid second."**

## Architecture
Three-layer decision engine:
1. **Reality estimation** — true probability distributions for weather outcomes (Open-Meteo ensemble + NWS)
2. **Market observation** — Kalshi bid/ask prices, liquidity, sibling bucket dynamics
3. **Edge evaluation** — trade only when mispricing survives spreads, fees, and execution friction (≥5% edge threshold)

Atomic unit: **contract × timestamp** (one contract at one moment in time).

## Data Stack
- **Warehouse:** DuckDB (three-layer schema: raw → core → features)
- **Forecasts:** Open-Meteo API (ensemble, GFS, HRRR)
- **Settlement truth:** NWS Daily Climate Reports (must match Kalshi's settlement source)
- **Historical Kalshi data:** `/home/dataops/prediction-market-analysis/data/kalshi/` (72M+ trades, 4.5M weather trades, parquet format)
- **Weather tickers:** ~106K distinct, dominated by temperature (HIGHNY, HIGHCHI, HIGHMIA, KXHIGH*, RAIN*, SNOW*)

## Key Decisions
- Pre-filter Kalshi parquet data to weather-only before loading into DuckDB (6.3% of trades, 99x query speedup)
- Sibling bucket contracts represent slices of one distribution — model the full shape, not isolated brackets
- Fair value must be compared against executable prices (bid/ask), not midpoints
- Phased build: forecasts → distribution features → execution-aware costs → richer signals

## Environment
- Server: Hetzner Linux (this machine)
- Python ≥3.11, DuckDB, pandas, pyarrow, requests, pydantic
- 9 cities configured: NYC, Chicago, Miami, LA, Dallas, Austin, Denver, Philadelphia, Houston
- NWS stations mapped per city in config/stations.yml

## Development
- `make setup` — create venv and install
- `make bootstrap` — init DuckDB schema and registries
- `make test` — run all tests
- `make run-eval` — run baseline evaluator
- Tests use unittest; run from repo root

## Trading Framework
- Markets open 10:00 AM ET, one day before event
- Settlement: NWS reports at 7-8 AM ET next day
- Bot cycle: 6AM model ingest → 7AM settlement logging → 10AM market open → intraday adjustments → evening final models
- Starting capital: $500-1,000; single bet ≤5% bankroll; daily loss limit 10%
