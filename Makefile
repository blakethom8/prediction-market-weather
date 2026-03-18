PYTHONPATH=. 

bootstrap:
	PYTHONPATH=. python3 -m src.weatherlab.build.bootstrap

checks:
	@echo "TODO: add SQL/data integrity checks"

run-eval:
	PYTHONPATH=. python3 eval.py
