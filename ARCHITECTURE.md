# Prediction Market Weather — Architecture

## Core Principle

**Forecast first, bid second.**

The system has three layers:

1. **Reality layer** — estimate the real-world weather distribution
2. **Market layer** — observe Kalshi prices, spreads, and sibling-bucket structure
3. **Decision layer** — only place bets when edge survives execution constraints

## Canonical Grain

The main modeling grain is:

- **one row = one contract × one timestamp**

Each row should capture:
- contract metadata
- current market state
- latest forecast available at that time
- sibling-strip context
- derived fair probability
- execution-aware edge
- final settlement label

## Main Entities

### `contracts`
Defines recurring market contracts:
- city
- weather variable
- threshold/bucket semantics
- close/settle times
- official station mapping

### `market_snapshots`
Time-series of:
- yes/no bid/ask
- midpoint
- volume
- open interest
- time-to-close

### `forecast_snapshots`
Time-series of forecast knowledge:
- forecast source
- issued time
- available time
- target date
- predicted high/low
- uncertainty/distribution info

### `settlement_observations`
Final truth aligned to official settlement source:
- station
- local market date
- final observed value
- report timestamp

### `bet_decisions`
Auditable decision records:
- why we considered the bet
- model fair probability
- market price
- threshold for action
- expected value
- rationale payload

### `bet_outcomes`
Post-trade review:
- fill info
- realized P&L
- outcome vs expectation
- regret / missed opportunity
- review notes

## Modeling Targets

We should support three target families:

1. **Resolution target**
   - Did the contract resolve YES?
2. **Mispricing target**
   - Was the contract underpriced or overpriced relative to fair probability?
3. **Price-movement target**
   - Did the market reprice over the next horizon?

## Auditability Requirement

Every bet must be explainable after the fact.

That means storing:
- exact inputs seen at decision time
- exact forecast snapshot used
- exact market snapshot used
- feature summary
- model version / signal version
- rationale text or JSON
- execution result
- retrospective review

## Self-Improvement Loop

The system should make it easy for a coding agent to review:
- what we bet
- why we bet
- what happened
- whether the signal was wrong, the execution was bad, or the market just stayed irrational

That review loop is how the system becomes self-healing rather than just self-confident.
