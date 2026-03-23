VENV_PYTHON := .venv/bin/python
PYTHON := $(if $(wildcard $(VENV_PYTHON)),$(VENV_PYTHON),python3)

bootstrap:
	PYTHONPATH=src $(PYTHON) -m weatherlab.build.bootstrap

setup:
	python3 -m venv .venv
	$(VENV_PYTHON) -m pip install -e .

test:
	PYTHONPATH=src $(PYTHON) -m unittest discover -s tests -v

selftest: test

checks:
	@echo "TODO: add SQL/data integrity checks"

extract-kalshi:
	PYTHONPATH=src $(PYTHON) -m weatherlab.ingest.kalshi_history

promote:
	PYTHONPATH=src $(PYTHON) -m weatherlab.build.promote

backfill-forecasts:
	PYTHONPATH=src $(PYTHON) -m weatherlab.ingest.historical_forecasts

run-eval:
	PYTHONPATH=src $(PYTHON) eval.py

daily-board:
	PYTHONPATH=src $(PYTHON) -m weatherlab.live.cli

live-web:
	PYTHONPATH=src $(PYTHON) -m weatherlab.live.web --host 0.0.0.0 --port 8000

fetch-live:
	@echo "Fetching live Kalshi weather markets..."
	PYTHONPATH=src $(PYTHON) -m weatherlab.ingest.kalshi_live_sync
