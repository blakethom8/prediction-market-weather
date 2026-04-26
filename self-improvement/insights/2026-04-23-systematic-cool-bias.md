# Insight: Systematic Cool Bias in NWS Forecasts

**Date discovered:** 2026-04-23
**Source:** Post-mortem on 10+ settled paper bets
**Status:** Root cause identified, fix not yet implemented (see TASK-001)

---

## Observation

Every settled edge bet ran warmer than our model predicted. The cool bias appears across multiple cities and dates:

| Date | City | Our Forecast | Actual High | Gap |
|---|---|---|---|---|
| 2026-04-13 | Los Angeles (LAX) | ~62°F | 66.5°F | **+4.5°F** |
| 2026-04-13 | Miami (MIA) | ~80°F | 81.5°F | **+1.5°F** |
| 2026-03-25 | DC (DCA) | ~65°F | ~66.5°F | **+1.5°F** |
| 2026-03-25 | Seattle | model low | actual higher | **+2.5°F** |

## Root Cause

**NWS forecasts use metro-area coordinates, not airport-specific station coordinates.** Kalshi contracts settle against airport weather stations (DCA, MIA, LAX, etc.), which run warmer due to:
- Urban heat island effects
- River valley positioning (DCA especially)
- Tarmac/impervious surface heating

Our forecast model fetches NWS data using generic lat/lon that doesn't match the settlement station. The gap between metro forecast and airport actuals is systematic and directional: **airport stations run warmer**.

## Impact

- YES bets on lower buckets are systematically wrong — we're buying a bucket that doesn't account for the airport warming effect
- The edge we thought we had (+24-62¢) was actually negative in real terms
- ColdMath bets were less affected because the gap was large enough to absorb the bias

## Fix

Two-part fix:
1. **Immediate (1 line):** Add `+1.5°F` additive bias correction to `forecast_high_f` in `src/weatherlab/forecast/asos.py` before probability calculations
2. **Correct fix (config change):** Update city coordinates to exact airport lat/lon (see TASK-003)

The correction magnitude varies by city. `+1.5°F` is a conservative baseline. LAX likely needs `+3-5°F` given the coastal vs inland delta observed.

## Open Questions

- Does the bias vary by season? (Summer vs winter ocean influence)
- Should we track this correction per-city rather than a global constant?
- Can we calibrate the per-city correction from ASOS historical data?
