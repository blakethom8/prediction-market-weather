#!/usr/bin/env bash
# Initialize the DuckDB warehouse schema for the current checkout.
# Usage: ./scripts/bootstrap_db.sh

set -euo pipefail
cd "$(dirname "$0")/.."
if [ -x .venv/bin/python ]; then
  PYTHON=.venv/bin/python
else
  PYTHON=python3
fi
PYTHONPATH=src "$PYTHON" -m weatherlab.build.bootstrap
