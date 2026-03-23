#!/usr/bin/env bash
# Create the local virtualenv and install the project in editable mode.
# Usage: ./scripts/setup_env.sh

set -euo pipefail
cd "$(dirname "$0")/.."
python3 -m venv .venv
.venv/bin/python -m pip install -e .
