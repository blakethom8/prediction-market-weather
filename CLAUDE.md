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
- **Warehouse:** DuckDB (three-layer schema: raw → core → features), located at `data/warehouse/weather_markets.duckdb`
- **Forecasts:** Open-Meteo API (archive for historical, ensemble/GFS/HRRR for live)
- **Settlement truth:** NWS Daily Climate Reports (must match Kalshi's settlement source)
- **Historical Kalshi data:** `/home/dataops/prediction-market-analysis/data/kalshi/` (72M+ trades, 4.5M weather trades, parquet format)
- **Weather tickers:** ~48K in warehouse, dominated by temperature (HIGHNY, HIGHCHI, HIGHMIA, KXHIGH*, RAIN*, SNOW*)

## Key Decisions
- Pre-filter Kalshi parquet data to weather-only before loading into DuckDB (6.3% of trades, 99x query speedup)
- Sibling bucket contracts represent slices of one distribution — model the full shape, not isolated brackets
- Fair value must be compared against executable prices (bid/ask), not midpoints
- Phased build: forecasts → distribution features → execution-aware costs → richer signals
- For `between` contracts, fair_prob = P(>=low) - P(>=high+1), not raw P(>=threshold)
- Kalshi prices are in cents (0-100) — normalize to 0-1 for probability comparisons
- Historical trades only have `last_price` (no bid/ask) — use as fallback in training view

## Known Issues
- **Open-Meteo vs NWS source mismatch:** ~3°F average error; LA is worst at 8.7°F. Highest-priority fix.
- **LA station alignment:** Open-Meteo grid point doesn't match Kalshi's NWS station (KCQT). Consider excluding LA or learning a large bias correction.
- **Python 3.9 compat:** Analysis venv is Python 3.9; all modules need `from __future__ import annotations`. Project targets >=3.11.
- **No project venv yet:** Currently using `/home/dataops/prediction-market-analysis/.venv/bin/python`. Run `make setup` when ready.

## Environment
- Server: Hetzner Linux (this machine), running as root, project in /home/dataops/
- Python ≥3.11 target, currently running on 3.9 venv with duckdb, pyarrow, pyyaml, requests
- 9 cities configured: NYC, Chicago, Miami, LA, Dallas, Austin, Denver, Philadelphia, Houston
- NWS stations mapped per city in config/stations.yml

## Development
- `make setup` — create venv and install
- `make bootstrap` — init DuckDB schema and registries
- `make extract-kalshi` — filter Kalshi parquet archive to weather-only and load into raw tables
- `make promote` — parse raw markets → core contracts + snapshots + infer settlements
- `make backfill-forecasts` — fetch Open-Meteo historical archive data
- `make test` — run all tests
- `make run-eval` — run baseline evaluator
- Tests use unittest; run from repo root

## Pipeline Sequence (full rebuild)
```bash
make bootstrap           # Schema + views
# Load registries (cities/stations):
PYTHONPATH=src python -m weatherlab.build.registry_loader
make extract-kalshi      # Raw parquet → raw tables (~3s)
make promote             # Raw → core (~5min for 48K contracts)
make backfill-forecasts  # Open-Meteo archive → forecast tables (~1min)
```

## Warehouse Contents (as of 2026-03-22)
| Table | Records | Notes |
|-------|---------|-------|
| raw.kalshi_markets | 48,099 | Weather-filtered from 7.68M total |
| raw.kalshi_market_snapshots | 4,374,590 | Weather trades |
| core.weather_contracts | 47,112 | 41,726 fully parsed |
| core.market_snapshots | 2,577,904 | Trade-level prices (cents) |
| core.settlement_observations | 4,352 | Inferred from resolved brackets |
| core.forecast_snapshots | 5,305 | Open-Meteo archive reanalysis |
| core.forecast_distributions | 32,382 | P(>=threshold) per contract |
| features.v_training_rows | ~1.79M | Complete rows with fair_prob + edge |

## Baseline Signal
- Strategy: buy YES when edge_vs_mid >= 5%, sigma=5.0
- Result: +$68.61 across 387K simulated trades, 7/8 cities profitable
- Win rate: 12.3% (low but positive avg pnl)
- Next improvement: fix forecast source alignment (Priority 1)

## Trading Framework
- Markets open 10:00 AM ET, one day before event
- Settlement: NWS reports at 7-8 AM ET next day
- Bot cycle: 6AM model ingest → 7AM settlement logging → 10AM market open → intraday adjustments → evening final models
- Starting capital: $500-1,000; single bet ≤5% bankroll; daily loss limit 10%
