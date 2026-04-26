# Agent Task Backlog

Prioritized queue of improvements for coding/analysis agents.
Pick the top item, create a file in `active/`, do the work, then move it to `done/`.

Last updated: 2026-04-26

---

## Priority 1 — Fix Structural Bugs (Do These First)

### [TASK-001] Apply warm bias correction to forecast pipeline
**File:** `src/weatherlab/forecast/asos.py`
**What:** Add +1.5°F additive bias correction to `forecast_high_f` before any probability calculation.
**Why:** Every settled edge bet ran warmer than predicted. Systematic cool bias observed across 10+ bets.
**Code hint:**
```python
WARM_BIAS_CORRECTION_F = 1.5  # systematic cool bias, observed 2026-03-23 → 2026-04-13
if forecast_high_f is not None:
    forecast_high_f += WARM_BIAS_CORRECTION_F
```
**Tests:** Run existing test suite. Add a unit test verifying bias is applied.
**Related insight:** `insights/2026-04-23-systematic-cool-bias.md`

---

### [TASK-002] Add volume filter to recommendation engine
**File:** `src/weatherlab/signal.py` (or wherever `choose_best_market()` lives)
**What:**
- Skip contracts with <1,000 total open interest
- Flag as `MARKET_DISAGREES` (not BUY) when volume >5,000 AND market price disagrees with model by >10¢
**Why:** High-volume markets have informed traders who've already priced the airport bias. We shouldn't fight them.
**Tests:** Add unit tests for both filter conditions.

---

### [TASK-003] Use airport-specific coordinates for NWS forecast lookup
**File:** City config (check `config/` or wherever lat/lon is stored)
**What:** Replace generic metro coordinates with exact airport station coordinates.
**Correct values** (from `docs/BETTING_INSIGHTS.md`):
| City | Station | Lat | Lon |
|---|---|---|---|
| DC | DCA | 38.8521 | -77.0377 |
| Miami | MIA | 25.7959 | -80.2870 |
| Boston | BOS | 42.3601 | -71.0105 |
| Philly | PHL | 39.8721 | -75.2411 |
| LA | LAX | 33.9425 | -118.4081 |
| Chicago | MDW | 41.7868 | -87.7522 |
| Denver | DEN | 39.8561 | -104.6737 |
**Tests:** Verify NWS calls use new coordinates. Manually check that DC returns DCA-style temps.

---

## Priority 2 — Use Data We're Already Collecting

### [TASK-004] Wire observed temps into recommendation logic
**File:** `src/weatherlab/signal.py`
**What:** `obs_divergence_f` is computed but unused in `_recommendation_for_city`. Add:
- If `observed_max_so_far_f` ≥ threshold by 10 AM → strong BUY signal (observation beats forecast)
- If `observed_max_so_far_f` tracking >3°F below forecast at 11 AM → downgrade recommendation
**Why:** Observed temps as the day progresses are more reliable than morning forecasts.

---

### [TASK-005] Raise ColdMath gap threshold to 10°F for real-money mode
**File:** Wherever ColdMath threshold is configured
**What:** Only generate ColdMath proposals when `forecast_gap_f >= 10°F` AFTER bias correction.
**Why:** 2-3°F gaps are getting priced correctly by the market. 10°F gaps are near-certainties.

---

## Priority 3 — Expand Strategy

### [TASK-006] Macro ColdMath framework (GDP Apr 30 post-mortem)
**After Apr 30 GDP settlement:**
- Document the GDP thesis outcome in an `insights/` file
- Add macro market screening logic similar to weather scan
- Criteria: structural bets where outcome would require historically anomalous conditions, contract at <15¢

---

### [TASK-007] Intraday observed temp scraper
**What:** Scrape ASOS observations every 30 min for active bet cities, starting at market open.
**Why:** By 10-11 AM, observed temps often confirm or deny the forecast. Real-time edge.
**Complexity:** Medium — ASOS data is public via Iowa Environmental Mesonet.

---

## Priority 4 — Infrastructure

### [TASK-008] Settle stale paper bets (Apr 15 DC and Miami)
**What:** The Apr 15 DC B90.5 and Miami B79.5 YES bets are still marked `open`. Settle them manually.
**How:** Run `make chief -- settle` or use `scripts/settle_markets.py`
**Note:** Apr 15 DC high was well below 90.5°F. Apr 15 Miami high was near 79-80°F — check outcome.

---

### [TASK-009] Auto-calibration report after every 10 settled bets
**What:** Add a cron/hook that runs `make chief -- calibration` and writes a new `self-improvement/reviews/` file automatically when settled bet count crosses a multiple of 10.
**Why:** Removes the manual step of triggering reviews.

---

---

## Priority 5 — Alpha Expansion (Added 2026-04-26)

*Source: Strategic review — see `insights/2026-04-26-intraday-ladder-strategy.md`*

### [TASK-010] Add intraday scan crons (10:30am + 12pm ladder)
**Files:** `scripts/morning_scan.py`, OpenClaw cron config or Makefile
**What:**
- Add a `scripts/intraday_scan.py` (or flag on morning_scan) that runs at 10:30am and 12pm PDT
- 10:30am scan: East Coast cities only, threshold-contract focus, require observed_max within 3°F of threshold
- 12pm scan: Central + West Coast cities, same threshold focus
- Each scan sends a notify event only if actionable plays found (suppress empty scans)
- Add `--window east|central|west` flag to scope city set
**Why:** East Coast highs peak 11am-1pm PDT. The best confirmed-play window is 10-11am PDT, not 8am. We have the intraday logic (TASK-004) but no cron for it.
**Tests:** Unit test that `--window east` only scans East Coast cities; unit test that scan suppresses notify when no threshold plays found.

---

### [TASK-011] Threshold-only filter in recommendation engine
**File:** `src/weatherlab/pipeline/morning_scan.py`, `src/weatherlab/pipeline/_markets.py`
**What:**
- Add `is_threshold_contract(market: WeatherMarket) -> bool` — returns True for `>=` and `<=` operators, False for `between` (bucket)
- In `_recommendation_for_city()`: if contract is a bucket (`between`), downgrade any BUY to WATCH unless full ColdMath criteria met (≥10°F gap, market ≥85¢)
- Add `contract_type: threshold|bucket` field to scan output rows
- Morning scan report should note `[BUCKET - watch only]` on any bucket recommendations
**Why:** Bucket bets require ±0.5°F accuracy we don't have. Threshold bets win across a range. This prevents the scan from surfacing bucket plays as actionable BUYs.
**Tests:** Test that bucket contract with high edge gets downgraded to WATCH; test that threshold contract with same edge stays BUY.

---

### [TASK-012] Macro Kalshi market scanner
**File:** `scripts/macro_scan.py` (new), `src/weatherlab/ingest/kalshi_live.py`
**What:**
- New script that fetches all open Kalshi markets (not just weather)
- Filters for ColdMath candidates: `yes_ask <= 0.15` AND `volume > 500`
- For each candidate, outputs: ticker, title, yes_ask, volume, days_to_close
- Formats as a scannable report (similar to morning scan output)
- Add `--notify` flag to send via openclaw system event
- Does NOT make recommendations — just surfaces candidates for human review
**Why:** CPI bet was our best trade (9x). Macro structural bets have better risk/reward than weather buckets and don't require forecast accuracy.
**Tests:** Unit test that filter correctly selects low-priced, liquid markets; test report formatting.

---

### [TASK-013] Polymarket cross-platform price comparison
**File:** `scripts/arb_scan.py` (new), `src/weatherlab/ingest/polymarket.py` (new)
**What:**
- Fetch Polymarket markets via their public API (no auth needed for reads)
- Match against open Kalshi markets by event keyword/slug
- Flag pairs where price difference > 5¢ on same-side
- Output: `KALSHI {ticker} YES={price} vs POLYMARKET {slug} YES={price}, diff={delta}¢`
- Add `--notify` flag
**Why:** Pure arbitrage — no model needed. Our 24/7 monitoring is the edge.
**Note:** Start with read-only price comparison. Polymarket execution is separate.
**Tests:** Unit test price comparison logic; test that diff < 5¢ produces no output.

---

## Completed Tasks

*(Move files here from `active/` when done)*

See `done/` folder.
