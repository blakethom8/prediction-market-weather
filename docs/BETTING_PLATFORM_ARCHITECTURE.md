# Betting Platform Architecture

This file is now a short companion to [`ARCHITECTURE.md`](../ARCHITECTURE.md). The root architecture doc is the canonical source for the full repo layout.

## Live Operating Model

- Scan the full available live board first
- Create a strategy session for the day
- Persist proposal rows with rationale and context
- Record explicit approval, adjustment, or rejection events
- Convert approved names into paper bets by default
- Settle outcomes and store lessons
- Compare runs later with `strategy_variant` and `scenario_label`

## What The Betting Platform Owns

- day-of board generation
- strategy sessions
- proposal and review state
- paper bets
- historical learning views for operator review
- the local FastAPI app

## What It Does Not Pretend To Be Yet

- not a browser-driven execution console
- not a fully automated live trading system
- not a substitute for the research and calibration layer

Real order placement has been tested, but the default workflow is still paper-first and the repo does not yet have complete order-management surfaces.
