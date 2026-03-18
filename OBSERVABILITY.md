# Observability, Logging, and Bet Transparency

## Goal

A bet should never appear as a black box.

For every bet, we want to answer:
- What did we think would happen?
- What was the market offering?
- Why did we think this was mispriced?
- What assumptions were fragile?
- What happened afterward?
- Should we make the same bet again?

## Logging Layers

### 1. Pipeline logs
Track ingestion/build health:
- job name
- source
- start/end time
- rows fetched
- rows inserted
- failures
- data freshness

### 2. Decision logs
Track why a bet was or was not placed:
- contract ticker
- timestamp
- fair probability
- tradable bid/ask
- expected edge
- execution constraints
- confidence band
- abstention reason if skipped

### 3. Execution logs
Track trading mechanics:
- submitted order price
- side
- size
- fill status
- partial fill details
- slippage
- fees

### 4. Review logs
Track learning loop:
- result of the bet
- realized edge vs expected edge
- source mismatch?
- spread too wide?
- market was stale?
- model wrong?
- should thresholds change?

## Suggested Artifacts

### `decision_journal`
One row per considered action.

### `bet_rationale`
JSON payload stored alongside each bet:
- forecast summary
- market summary
- sibling-bucket summary
- top features
- confidence
- action rule triggered

### `postmortem_reviews`
One row per closed trade with structured review fields.

## Dashboard Concepts

### Pipeline Health
- latest ingest times
- stale sources
- failed jobs
- row deltas

### Bet Feed
- latest candidate bets
- placed bets
- skipped bets with reasons

### Bet Detail View
- market price vs fair probability over time
- forecast revisions over time
- sibling-strip chart
- explanation panel

### Review View
- expected vs realized edge
- by city
- by horizon
- by signal version
- by market regime

## Self-Healing Principle

The system should produce enough evidence that a future coding agent can improve it by reading the logs and review tables rather than guessing.
