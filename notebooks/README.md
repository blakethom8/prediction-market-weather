# Notebooks

Use notebooks only for exploration and audits.

Rules:
- Do not create source-of-truth transformations here.
- Any logic that matters for training or backtesting should be moved into `src/` or `sql/`.
- Good notebook uses: contract parsing audits, station mapping review, visual inspection of market behavior.
