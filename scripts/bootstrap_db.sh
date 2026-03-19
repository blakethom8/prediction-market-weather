#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [ -f .venv/bin/activate ]; then
  . .venv/bin/activate
fi
PYTHONPATH=. python3 -m src.weatherlab.build.bootstrap
