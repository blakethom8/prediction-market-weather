bootstrap:
	PYTHONPATH=. python3 -m src.weatherlab.build.bootstrap

setup:
	python3 -m venv .venv
	. .venv/bin/activate && pip install --upgrade pip && pip install -e .

test:
	PYTHONPATH=. python3 -m unittest discover -s tests -v

selftest: test

checks:
	@echo "TODO: add SQL/data integrity checks"

run-eval:
	PYTHONPATH=. python3 eval.py
