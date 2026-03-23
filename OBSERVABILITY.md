# Observability And Review

The project already has the right storage shape for auditability. The next job is to use it consistently and expose the missing freshness and execution checks.

## Current Observability Surfaces

### Pipeline and schema health

- `ops.pipeline_runs` exists for ingest and build job tracking
- `/healthz` checks that the web app's required live tables and views exist
- DuckDB bootstrap is deterministic: `sql/ddl/*.sql` then `sql/views/*.sql`

### Strategy and proposal review trail

- `ops.strategy_review_events`
- `ops.bet_proposal_events`
- `ops.v_strategy_proposal_outcomes`

These are the main audit records for:

- when a daily plan was reviewed
- what changed after review
- which proposals were approved, adjusted, rejected, converted, or settled

### Paper bet learning trail

- `ops.paper_bets`
- `ops.paper_bet_reviews`
- `ops.v_paper_bet_history`
- `ops.v_strategy_board_learning_history`
- `ops.v_strategy_session_learning`

These are what the `/paper-bets` and `/history` pages read.

### Execution trail

- `ops.bet_executions`
- `ops.bet_reviews`

These are the intended homes for real execution records and post-trade review. Real order placement has been tested, but the full order-management and execution-observability path is not yet wired through the main workflow.

## What A Good Record Should Answer

For any proposal or bet, the data should let you answer:

- what market did we look at?
- what did the board rank say at the time?
- which forecast snapshot did we rely on?
- what was the thesis?
- what changed in review?
- did it become a paper bet?
- how did it settle?
- what lesson should carry forward?

## Gaps Still Worth Closing

- write `ops.pipeline_runs` from every ingest and materialization command, not just keep the table available
- add freshness checks for forecasts as well as market snapshots
- store live order status changes and fills in a first-class way
- expose stale-board and stale-forecast warnings in the web app
- add a small operator dashboard for the latest successful ingest times

## Practical Standard

If a future coding agent cannot reconstruct the full chain from:

- board row
- proposal
- review event
- paper bet or live execution
- settlement review

then the observability layer is still incomplete.
