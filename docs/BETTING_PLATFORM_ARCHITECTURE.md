# Betting Platform System Architecture

## Executive Summary

The betting platform should be treated as a distinct system from the historical research / ML pipeline.
They may live in the same repository for now, but they serve different jobs:

- **Research / ML system:** learn, backtest, calibrate, and generate intelligence
- **Betting platform:** operate the day, compare opportunities, propose bets, capture approvals, track results, and improve execution discipline

The key design rule is:

> The live betting platform may consume intelligence from research, but it must not be structurally tangled with research code.

## Product Goal

Build a transparent operating system for daily weather-market decision-making.

The immediate mission is not fully autonomous trading.
It is to create a trustworthy paper-trading and operator workflow that:

1. builds the point-in-time board for the day
2. compares opportunities across all available cities and contracts
3. proposes a strategy and candidate bets
4. routes that strategy to Blake for review / approval / adjustment
5. records the final paper decisions and later outcomes
6. improves over time through postmortem review

## System Boundaries

### 1. Research / ML domain
Owns:
- historical market ingestion
- archived forecasts
- settlement truth pipelines
- feature generation
- backtests and replay
- model development
- calibration and diagnostics

Outputs to the betting platform:
- fair probabilities
- confidence estimates
- forecast diagnostics
- source-quality signals
- city-specific caution flags

### 2. Live betting platform domain
Owns:
- day-of board generation
- strategy sessions
- candidate ranking
- daily strategy summaries
- approval / adjustment workflow
- paper bets and later live bet execution
- outcome review
- dashboard and operator surfaces

### 3. Shared domain
Owns only stable shared primitives:
- market identifiers
- registry/config references
- database connection utilities
- common schemas where necessary
- general-purpose helpers

## Operating Model

### Current target mode: daily approval loop
This is the default mode the platform should optimize for first.

Flow:
1. scheduled daily board build
2. strategy report generated automatically
3. Blake reviews proposed bets and passes
4. Blake approves, edits, or rejects
5. approval state is stored explicitly on the strategy session
6. proposed bets are stored explicitly with review history
7. paper bets are recorded from approved proposals or manual overrides
8. results are settled and reviewed later

Why this mode first:
- high trust
- high transparency
- easy to debug
- creates a clean paper-trading dataset
- makes later automation safer

## Future operating modes

### A. Suggest-only alerts
- system runs quietly
- only surfaces bets when edge appears strong
- useful once the board and ranking logic are mature

### B. Periodic intraday refresh
- board reruns every N minutes/hours
- strategy memo or alerts update during the day

### C. Semi-autonomous execution
- only after risk, sizing, approval, and audit controls are mature

## Core Live Platform Objects

### Strategy Session
Represents the day’s strategy container.

Fields should include:
- date
- status
- approval status
- approval timestamp
- last review timestamp
- approval notes
- research-focus cities
- board scope / filters
- board coverage counts
- thesis
- strategy variant
- scenario label
- session context
- selection framework
- notes / assumptions

### Strategy Market Board
Represents the captured point-in-time board of all candidate bets.

Fields should include:
- market ticker
- market title
- city
- market date
- minutes to close
- current prices
- forecast snapshot reference
- fair probability
- edge metrics
- rank / bucket
- board notes

### Bet Proposal
Represents a proposed bet captured before approval and execution.

Fields should include:
- associated strategy session
- originating board row
- market / city / date
- proposed side / price / size
- perceived edge
- candidate rank / bucket
- strategy variant / scenario label
- rationale summary + structured context
- proposal status and event history

### Paper Bet
Represents a simulated trade decision after proposal/review.

Fields should include:
- associated strategy session
- associated proposal
- associated market
- side
- price
- size
- expected edge at entry
- thesis snapshot at entry
- rationale summary
- forecast reference
- status
- realized P&L
- review payload

### Daily Strategy Summary
Human-readable artifact for Blake.

Should answer:
- what are today’s best opportunities?
- why these and not the others?
- which markets are passes?
- what is the operating thesis today?
- where is confidence low?

## User Interaction Architecture

### Phase 1: report-first interaction
Blake receives a summary report each day with:
- the day’s thesis
- top candidates
- watchlist
- passes
- suggested paper bets

Blake can then:
- approve
- adjust
- ask questions
- skip the day
- limit exposure

### Phase 2: dashboard interaction
The dashboard should become the main operator interface.

Recommended screens:
1. **Daily Board** — all candidate bets for the day
2. **Strategy Summary** — thesis, proposed bets, and passes
3. **Proposals / Approvals** — pending, adjusted, rejected, converted
4. **Paper Bets / Executions** — open, closed, outcome, P&L
5. **Review / Learning** — recurring mistakes, wins, missed opportunities

### Phase 3: workflow automation
After the report-first loop is reliable:
- allow scheduled board generation
- allow periodic refreshes
- allow threshold-based alerts

## Application Structure Recommendation

Inside the repository, keep a clean separation:

- `src/weatherlab/live/` → betting platform application logic and persistence helpers
- `src/weatherlab/research/` → research-facing namespace for replay, evaluation, and archive planning
- `src/weatherlab/ops/` → compatibility layer for legacy live workflow imports

Current practical guidance:
- new day-of board, reporting, and approval logic should go under `live`
- historical replay / backtest / archived forecast work should remain outside `live`
- avoid importing research-heavy internals directly into user-facing app flows

## Risk and Trust Principles

The platform should optimize for:
- explainability over cleverness
- consistency over hero trades
- small repeatable gains over swingy outcomes
- explicit abstentions over forced action
- reviewable decisions over hidden automation

## Immediate Build Priorities

1. **Day-of board generation workflow**
2. **Daily strategy summary generator**
3. **Approval / adjustment workflow**
4. **Paper bet settlement + review discipline**
5. **Dashboard scaffolding**
6. **Codex/cleanup pass after first full workflow exists**
7. **Eventually: convert stabilized workflow into a dedicated SKILL.md**

## Bottom Line

The betting platform is an operator product first.

It should help Blake answer, every day:
- What is the board?
- Where is the best relative edge?
- What should we do today?
- Why?
- What happened after we did it?

That is the system we should build now.
