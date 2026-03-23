# Prediction Market Weather Roadmap

Paper bets remain the default path. The roadmap is about making that workflow sharper, safer, and more informative before turning live execution into the center of the product.

## Complete

- Live Kalshi weather market sync is in place
- Kalshi auth is wired with RSA-PSS signing and local private-key handling
- The local FastAPI app is in place with Today, Board, Strategy Session, Paper Bets, and History pages
- The DuckDB warehouse is organized into `raw`, `core`, `features`, and `ops`
- The paper betting workflow is in place:
  - strategy sessions
  - captured board rows
  - proposals
  - approval events
  - paper bets
  - settlement reviews
- Historical learning views and the `/history` dashboard are in place
- `strategy_variant` and `scenario_label` are stored end to end so strategy comparisons are possible
- Real order placement has been tested as a milestone
  - first live test order: `a3675221-d090-4196-b812-b393ebfdb5f1`

## Near-Term

- Airport and station-specific forecast alignment so fair values match Kalshi settlement rules more closely
- A better calibrated fair-probability model instead of relying on one rough forecast layer
- Explicit order-management plumbing
  - write live executions into `ops.bet_executions`
  - poll and store order status
  - support cancel and replace flows
- Clear forecast freshness and board freshness checks
- A cleaner operator write path for approvals, conversions, and settlement instead of relying only on Python helpers

## Medium-Term

- Multi-city scaling beyond the current confidence-anchor workflow
- Better side-by-side strategy comparison by `strategy_variant`
- Automated daily workflow
  - fetch live markets
  - refresh forecasts
  - build the board
  - create the daily strategy package
  - refresh the web app and notifications
- Intraday board refreshes instead of a single day-level snapshot
- Stronger historical calibration and learning loops tied back to live decisions

## Not Done Yet

- The web app is not yet a full order-management console
- Live execution is not the default operating mode
- Forecast ingestion for the live board is not yet a one-command operator flow
- Strategy comparison exists in the data model and history views, but the workflow around it still needs tightening

## Direction

The next useful version of this project is not "more automation everywhere." It is:

- better station alignment
- better fair values
- tighter order and review logging
- easier daily operation
- cleaner comparison between strategy variants
