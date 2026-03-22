# Session Progress Report — March 22, 2026

## Overview

This document captures the state of the Kalshi Weather Prediction Market project as of March 22, 2026. In a single session, we built the end-to-end data pipeline from raw Kalshi historical parquet files through to a working training view with fair probability estimates, edge calculations, and a baseline profitable signal. This represents the transition from scaffolding to a functional backtesting system with real data.

---

## Accomplishments

### 1. Kalshi Historical Data Extraction Pipeline

**Module:** `src/weatherlab/ingest/kalshi_history.py`

Built a weather-only extraction pipeline that filters the full 72M-trade Kalshi archive (3.9GB) down to weather markets:

| Metric | Value |
|--------|-------|
| Raw markets extracted | 48,099 |
| Raw trades extracted | 4,374,590 |
| Distinct weather tickers | 48,099 |
| Extraction time | ~3 seconds |
| Warehouse size | 231 MB (vs ~4.2GB full) |

The pipeline uses a curated list of 23 weather ticker prefixes (HIGHNY, KXHIGHCHI, RAINNYC, SNOWNYM, HURCAT, etc.) to filter at the DuckDB scan level, achieving a 99x query speedup over scanning all trades.

**Key design choice:** Pre-filter at the parquet level rather than loading everything into DuckDB. The parquet files are chunked by row offset (not by ticker), so every query would need to scan all 7,214 files without pre-filtering.

### 2. Contract Title Parser Enhancement

**Module:** `src/weatherlab/parse/contract_parser.py`

Enhanced the regex-based parser to handle real Kalshi title formats discovered in production data:

- **Markdown bold markers** (`**high temp in NYC**`) — stripped before parsing
- **Negative temperatures** (`<-1°`) — regex now supports negative numbers
- **Direct comparison operators** (`<62°`, `>78°`) — fixed patterns that broke on `<` without word boundary
- **Early 2021 titles** ("Will the high in Chicago...") — matched alongside modern "high temp in" format

**Parse rate on single-city high temp markets: 99.4%** (33,513 of 33,711). The remaining 0.6% are HIGHUS "high temp in the US" contracts with no mappable city.

**Overall parse rate:** 85% (41,726 parsed + 5,386 partial out of 48,099). Partials are mostly multi-city KXCITIESWEATHER combos and rain/snow/hurricane contracts — intentionally deprioritized since temperature markets represent 94% of volume.

### 3. Raw → Core Promotion Pipeline

**Module:** `src/weatherlab/build/promote.py`

Built a bulk promotion pipeline that moves data from raw to normalized core tables:

| Core Table | Records | Description |
|-----------|---------|-------------|
| `core.weather_contracts` | 47,112 | Parsed contract metadata with city, station, thresholds |
| `core.market_snapshots` | 2,577,904 | Trade-level price observations (last_price) |
| `core.settlement_observations` | 4,352 | Inferred from resolved sibling buckets |

**Settlement inference:** For each event where exactly one `between` bucket resolved YES, we infer the observed temperature as the midpoint of that winning bucket. Cross-validated against Kalshi's own `result` field: **99.997% agreement** (only 52 mismatches out of 2M+ rows, all in multi-city combo contracts).

### 4. Historical Forecast Backfill

**Module:** `src/weatherlab/ingest/historical_forecasts.py`

Fetched Open-Meteo archive (reanalysis) data for all 5,305 city×date pairs across 8 cities. Stored as forecast snapshots with probability distributions computed via normal CDF.

For each contract, the distribution includes:
- P(temp >= threshold_low) for `>=` and `<=` contracts
- P(temp >= threshold_low) AND P(temp >= threshold_high + 1) for `between` contracts, enabling bracket probability = P(>=low) - P(>=high+1)

### 5. Training View with Bracket-Aware Fair Probability

**File:** `sql/views/001_training_view.sql`

Rebuilt the training view to handle the full complexity of real data:

- **Price normalization:** Kalshi prices are in cents (0-100); view converts to decimals (0-1)
- **Fallback pricing:** Historical trades lack bid/ask; view falls back to `last_price`
- **Bracket probability:** `between` contracts use P(>=low) - P(>=high+1) instead of raw P(>=threshold)
- **Operator-aware fair_prob:** `<=` contracts use `1 - P(>=threshold)`
- **Resolution logic:** Handles `between`, `>=`, and `<=` operators correctly

**Training view output:** 1.79M complete rows (with fair_prob, edge, and resolution labels).

### 6. Baseline Signal Validation

Simulated a simple trading signal: **buy YES when edge_vs_mid >= 5%**, using Open-Meteo reanalysis with sigma=5.0.

| Metric | Value |
|--------|-------|
| Simulated trades | 387,407 |
| Total P&L | **+$68.61** |
| Win rate | 12.3% |
| Profitable cities | 7 of 8 |
| Best performer | Houston (+$7.26/trade, 17.3% win rate) |
| Worst performer | LA (-$16.92 total, 8.7°F source bias) |

**Key finding:** Even with a crude normal CDF model using a mismatched data source (Open-Meteo reanalysis vs NWS settlement), the signal is directionally profitable across most cities.

---

## Critical Finding: Forecast Source Mismatch

The single largest issue discovered is the divergence between Open-Meteo reanalysis data and NWS settlement truth:

| City | Avg Bias (°F) | Std Error (°F) | MAE (°F) |
|------|--------------|----------------|----------|
| LA | +8.35 | 5.83 | 8.71 |
| Chicago | -3.21 | 2.83 | 3.52 |
| Miami | -2.63 | 1.82 | 2.74 |
| Houston | -2.10 | 2.33 | 2.59 |
| Denver | -0.89 | 3.35 | 2.55 |
| Austin | -2.03 | 2.29 | 2.53 |
| Philadelphia | -1.77 | 1.98 | 2.19 |
| NYC | -0.17 | 2.25 | 1.74 |
| **Overall** | **-1.22** | **3.84** | **2.95** |

This 3°F average error is the dominant source of miscalibration. The architecture docs explicitly warned about this: "avoid source mismatches between forecasting and settlement truth."

The LA bias (+8.35°F) is likely a station location issue — Open-Meteo uses a grid point that doesn't align with Kalshi's NWS settlement station (KCQT). Most other cities show a negative bias (Open-Meteo reads cooler than NWS).

---

## Architecture: What We Have

```
Historical Kalshi Parquet (72M trades, 3.9GB)
    │
    ▼ [extract-kalshi] weather ticker filter
raw.kalshi_markets (48K) + raw.kalshi_market_snapshots (4.3M)
    │
    ▼ [promote] parse titles, map cities/stations, infer settlements
core.weather_contracts (47K) ── core.market_snapshots (2.6M)
core.settlement_observations (4.3K)
    │
    ▼ [backfill-forecasts] Open-Meteo archive + normal CDF
core.forecast_snapshots (5.3K) ── core.forecast_distributions (32K)
    │
    ▼ [training view] join contracts × snapshots × forecasts × settlements
features.v_training_rows (1.79M complete rows)
    │
    ▼ [signal] edge >= 5% → BUY_YES
Baseline P&L: +$68.61 across 387K trades
```

### Makefile Targets

| Target | Purpose |
|--------|---------|
| `make bootstrap` | Initialize DuckDB schema and views |
| `make extract-kalshi` | Filter parquet archive to weather-only |
| `make promote` | Parse + promote raw → core + infer settlements |
| `make backfill-forecasts` | Fetch Open-Meteo historical data |
| `make test` | Run all unit tests |
| `make run-eval` | Run baseline evaluator |

---

## Next Steps

### Priority 1: Fix Forecast Source Alignment (Highest Impact)

The 3°F average error between Open-Meteo and NWS is the dominant issue. Three approaches, in order of expected impact:

**A. Learn city-specific bias corrections**
- Train a simple linear correction per city: `corrected_temp = open_meteo_temp + bias[city]`
- We already have 4,352 paired observations to learn from
- Quick win: could improve calibration significantly with minimal code
- Risk: bias may vary by season, not just city

**B. Use NWS historical data as forecast source**
- NWS Daily Climate Reports are what Kalshi uses for settlement
- If we can backfill NWS historical highs, our "forecasts" would be perfectly calibrated (for resolved contracts)
- For live trading, we'd use NWS *forecasts* rather than observations
- api.weather.gov provides both forecast and observation endpoints

**C. Use Open-Meteo ensemble spread**
- Open-Meteo offers ensemble model data with member spreads
- Instead of a fixed sigma, use the actual forecast uncertainty
- More realistic probability distributions, especially for volatile weather days

### Priority 2: Improve Probability Model (High Impact)

**A. Replace normal CDF with empirical distribution**
- The normal assumption is crude — real temperature errors are often skewed
- Build empirical error distributions from our 4,352 settlement comparisons
- Per-city, per-season distributions would capture local climatology

**B. Calibrate sigma dynamically**
- Current: fixed sigma=5.0 for all cities/dates
- Better: learn optimal sigma per city from historical forecast errors
- Even better: condition sigma on forecast confidence, season, and lead time

**C. Ensemble-based probability estimation**
- Use multiple forecast models (GFS, HRRR, NAM via Open-Meteo) as ensemble members
- Probability = fraction of members predicting the outcome
- More robust than any single model + assumed distribution

### Priority 3: Sibling Bucket Analysis (Medium Impact)

**A. Distribution shape features**
- For each event, compute the full strip of sibling bucket prices
- Compare market-implied distribution vs model-implied distribution
- Identify buckets where market pricing diverges from the model's shape

**B. Sibling entropy as a signal**
- High entropy = market uncertainty → wider sigma appropriate
- Low entropy = market consensus → our edge may be smaller
- Already have columns reserved in `features.contract_training_rows`

**C. Strip-level constraints**
- Sibling bucket probabilities must sum to ~1.0
- If market prices violate this, there's structural mispricing
- Can identify arbitrage opportunities across sibling buckets

### Priority 4: Execution Realism (Medium Impact)

**A. Spread-aware edge calculation**
- Current: using `last_price` as proxy for executable price
- Need: real bid/ask from Kalshi API snapshots (not just trades)
- Edge should be computed against the *ask* price (what you'd actually pay)

**B. Fee modeling**
- Kalshi charges fees on trades — need to subtract from edge
- Minimum edge threshold should account for round-trip fees

**C. Liquidity filters**
- Thin markets (few contracts traded) have wider effective spreads
- Filter out or penalize contracts below a volume threshold

### Priority 5: Live Trading Pipeline (Lower Priority Until Model Improves)

**A. Real-time Kalshi API integration**
- Fetch live market prices via Kalshi REST/WebSocket API
- Store in `raw.kalshi_market_snapshots` with real bid/ask data

**B. Live forecast ingestion**
- Scheduled Open-Meteo forecast fetches (6AM, noon, 6PM)
- Store multiple forecast snapshots per day for lead-time analysis

**C. Decision automation**
- Wire the signal module to the Kalshi trading API
- Start with paper trading (log decisions, don't execute)
- Graduate to live with strict position limits ($500-1K bankroll, 5% per bet)

### Priority 6: Model Sophistication (Future)

**A. ML-based probability estimation**
- Features: forecast temp, forecast uncertainty, historical bias, season, day-of-week, city
- Target: binary resolution (did the event occur?)
- Models: logistic regression → gradient boosted trees → calibrated neural nets

**B. Short-horizon repricing prediction**
- Predict whether market prices will move in the next N hours
- Enables timing-based entry/exit beyond static edge trading

**C. Multi-contract portfolio optimization**
- Correlated weather events across cities
- Position sizing based on Kelly criterion with edge uncertainty
- Hedge sibling buckets to reduce variance

---

## Environment Notes

- **Python version:** The analysis venv uses Python 3.9.25, but the project targets >=3.11. All new code uses `from __future__ import annotations` for compatibility. A proper project venv (`make setup`) should use Python 3.11+.
- **Missing venv:** No `.venv` exists in the project directory yet. Currently using `/home/dataops/prediction-market-analysis/.venv/bin/python` which has duckdb, pyarrow, pyyaml installed.
- **Database location:** `data/warehouse/weather_markets.duckdb` (231 MB with all data loaded)
- **Kalshi archive:** `/home/dataops/prediction-market-analysis/data/kalshi/` (3.9GB, 72M trades)

---

## Key Metrics to Track

| Metric | Current Baseline | Target |
|--------|-----------------|--------|
| Forecast MAE vs NWS | 2.95°F | <1.5°F |
| Fair prob calibration (80%+ bucket) | 30.6% actual | >70% actual |
| Signal P&L (edge >= 5%) | +$68.61 | +$500+ |
| Win rate | 12.3% | >25% |
| Cities profitable | 7/8 | 8/8 |
| LA bias | 8.7°F | <2°F |
