# Insight: Intraday Ladder Betting & Finding Real Alpha

**Date:** 2026-04-26
**Source:** Strategic review of single-day weather bet performance + platform capabilities
**Status:** Actionable — new tasks queued in BACKLOG (TASK-010 through TASK-013)

---

## The Core Problem

Single-day temperature **bucket** bets are the wrong product for our edge:

- Bucket bets (e.g. "Miami 79-80°F") require ±0.5°F accuracy. Our model error is ±3-5°F.
- We run scans at 8am — when NWS forecast reliability is at its *lowest* (overnight model run, no observations yet).
- We're betting at the worst time of day on the least reliable data.

**The irony:** Chief's actual advantage is 24/7 monitoring and instant data integration. We're using it once, in the morning, on the worst signal of the day.

---

## Our Real Edges

### 1. Intraday Ladder (time-sliced confirmed plays)

The optimal betting window is **not** 8am. It's when observed temps + updated forecasts converge:

| PDT | Action | Why |
|---|---|---|
| 8am | ColdMath structural only (≥10°F gap) | Forecast unreliable; only bet near-certainties |
| 10-11am | East Coast confirmed plays | Observed max near threshold, forecast updated, high not yet recorded |
| 11am-12pm | Central cities (Chicago, Dallas, Houston) | Their peak is ~1 PM CDT |
| 12-1pm | West Coast (LAX, Seattle) | Their peak is ~3 PM PDT |
| After 4pm | Nothing | All highs recorded, no edge left |

East Coast highs peak at 2-4pm EDT = **11am-1pm PDT**. The sweet spot for East Coast bets is **10-11am PDT** — observed max is already 70-80% of the day's high, forecast is reliable, but the market hasn't fully priced the intraday confirmation.

**What to build:** Add 10:30am and 12pm cron scans that specifically surface "observed max already near threshold" plays. TASK-004 built the logic — we just need the cron cadence and a scan focused on confirmed plays.

### 2. Threshold-Only Bets (not buckets)

Threshold bets (">85°F" or "<49°F") win across a *range* of outcomes.
Bucket bets need a bullseye in a 1°F window. With ±3-5°F model error, only thresholds are viable.

**Example:** On a day forecast for 84°F with observed max already 82°F by 10am, the right play is:
- ✅ Miami T80 (above 80°F) — almost certain
- ❌ Miami B82.5 (82-83°F bucket) — requires exact landing

**What to build:** Add a threshold-only filter to the recommendation engine. Refuse bucket bets unless they meet full ColdMath criteria (≥10°F gap, ≥85¢ price).

### 3. Macro ColdMath (our best risk/reward)

CPI bet: +$45 on $5 notional = 9x return.
The structural argument doesn't require forecast accuracy — just historical base rate knowledge.

Upcoming opportunities:
- **GDP Q1 (Apr 30):** GDPNow has gone negative. T2.5 NO at 42¢ is strong. T1.0/T2.0 YES positions at risk.
- **PCE (late Apr/May):** Same tariff thesis as CPI. Check for structural floor bets.
- **FOMC rate decisions:** "Cut by 50bps" is historically extreme → cheap NO.
- **Jobs report extremes:** NFP misses by 200K+ are rare → structural bets on extreme scenarios.

**What to build:** A macro market scanner that crawls non-weather Kalshi markets looking for ColdMath-type structural plays (extreme outcomes priced at 5-15¢ where historical base rate says <2%).

### 4. Cross-Platform Arbitrage

Same events on Kalshi vs Polymarket are sometimes mispriced by 3-8¢. Pure arb — no forecast model needed.
Our 24/7 monitoring is perfectly suited for this.

**What to build:** A Polymarket price comparison script that alerts when the same event differs by >5¢ across platforms.

---

## Priority Order

1. **TASK-010** — Intraday scan crons (10:30am + 12pm, threshold-focused)
2. **TASK-011** — Threshold-only filter in recommendation engine
3. **TASK-012** — Macro Kalshi market scanner (ColdMath structural plays)
4. **TASK-013** — Polymarket cross-platform comparison (arb alerts)

---

## Key Rule Going Forward

> **Never bet a bucket contract unless it meets full ColdMath criteria (≥10°F gap, ≥85¢ price).**
> **All edge plays use threshold contracts only.**
> **Timing matters: East Coast before 11am PDT, Central before 12pm, West Coast before 2pm.**
