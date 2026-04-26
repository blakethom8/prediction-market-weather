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

## Completed Tasks

*(Move files here from `active/` when done)*

See `done/` folder.
