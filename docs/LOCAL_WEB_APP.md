# Local Web App

The local web app is a FastAPI operator surface for the live betting workflow. It reads from DuckDB and shows the current board, stored strategy sessions, proposal state, paper bets, and historical learning views.

The app is read-oriented today. Session creation, proposal generation, approval changes, paper bet conversion, and settlement are still driven by CLI or Python workflow helpers.

## Prerequisites

- Python 3.11+
- `openssl` on `PATH` for Kalshi request signing
- a Kalshi API key ID
- the matching RSA private key saved locally
- optional: Tailscale if you want to reach the app from another device

## Local Setup

### 1. Create the virtualenv

```bash
python3 -m venv .venv
.venv/bin/pip install -e .
```

Equivalent helper:

```bash
./scripts/setup_env.sh
```

### 2. Create `.env`

```bash
KALSHI_API_KEY_ID=your-key-id
KALSHI_API_BASE_URL=https://api.elections.kalshi.com/trade-api/v2
KALSHI_API_PRIVATE_KEY_PATH=.kalshi_private_key.pem
WEATHER_WAREHOUSE_PATH=data/warehouse/weather_markets.duckdb
```

Notes:

- `.env` is loaded automatically on import
- `.kalshi_private_key.pem` is gitignored
- the client signs Kalshi requests with RSA-PSS

### 3. Save the private key

Put the Kalshi private key at the path referenced by `KALSHI_API_PRIVATE_KEY_PATH` and lock down its permissions:

```bash
chmod 600 .kalshi_private_key.pem
```

### 4. Bootstrap the warehouse

```bash
make bootstrap
```

For a brand-new DuckDB file, also load the static registry:

```bash
PYTHONPATH=src .venv/bin/python -m weatherlab.build.registry_loader
```

### 5. Fetch live markets

```bash
make fetch-live
```

This:

- hits Kalshi's live weather API
- stores open contract metadata and market snapshots
- rematerializes `features.contract_training_rows`
- refreshes `features.v_daily_market_board`

Important:

- `make fetch-live` covers the market side of the board
- fair values still depend on forecast snapshots already existing in `core.forecast_snapshots`

### 6. Create a strategy run

```bash
make daily-board -- --date 2026-03-23 --research-cities nyc,chi --strategy-variant baseline --thesis "Scan the full live board before isolating any single contract."
```

This writes:

- a strategy session
- the captured board rows for that session
- proposal rows
- a JSON, Markdown, and HTML strategy artifact under `artifacts/daily-strategy/`

### 7. Start the web app

```bash
make live-web
```

Direct module run:

```bash
PYTHONPATH=src .venv/bin/python -m weatherlab.live.web --host 0.0.0.0 --port 8000 --reload
```

Default bind:

- host: `0.0.0.0`
- port: `8000`

Open:

- `http://127.0.0.1:8000`

Check health:

```bash
curl http://127.0.0.1:8000/healthz
```

## Recommended Fresh Start

```bash
python3 -m venv .venv
.venv/bin/pip install -e .

cat > .env <<'EOF'
KALSHI_API_KEY_ID=your-key-id
KALSHI_API_BASE_URL=https://api.elections.kalshi.com/trade-api/v2
KALSHI_API_PRIVATE_KEY_PATH=.kalshi_private_key.pem
WEATHER_WAREHOUSE_PATH=data/warehouse/weather_markets.duckdb
EOF

chmod 600 .kalshi_private_key.pem
make bootstrap
PYTHONPATH=src .venv/bin/python -m weatherlab.build.registry_loader
make fetch-live
make daily-board -- --date 2026-03-23 --research-cities nyc,chi --strategy-variant baseline --thesis "Scan the full live board before isolating any single contract."
make live-web
```

## Tailscale Access

Because `make live-web` binds to `0.0.0.0`, the app can be reached over your Tailscale network.

Common URLs:

- `http://<tailscale-ip>:8000`
- `http://<machine-name>.<tailnet>.ts.net:8000` if MagicDNS is enabled

Notes:

- keep the app on the tailnet or local network only
- there is no built-in app auth yet
- the app is meant to be a private operator surface, not a public deployment

## Key Pages

- `/` and `/today`
  - today's board summary
  - top recommendations
  - proposal status and approval state
  - recent sessions
  - recent paper bets
- `/board`
  - latest stored board scan
- `/board/{YYYY-MM-DD}`
  - a specific date's board and any stored runs for that date
- `/strategies/{strategy_id}`
  - one strategy session's thesis, board summary, proposals, review history, and linked paper bets
- `/paper-bets`
  - open paper exposure plus settled outcomes and lessons
- `/history`
  - grouped learning views across strategy IDs, variants, scenarios, cities, approval outcomes, and edge buckets
- `/healthz`
  - confirms the live schema and required views are present

## Empty-State Notes

- If `/today` or `/board` is empty, you likely have not run `make fetch-live` yet.
- If markets show up but fair values are missing, the warehouse likely does not have current forecast snapshots for those contracts.
- If `/strategies/{strategy_id}` is empty, you likely have not run `make daily-board` for that date yet.
