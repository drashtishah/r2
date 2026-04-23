.PHONY: install update typecheck test report clean check

export PYTHONPATH := $(CURDIR)

install:
	uv sync

update:
	uv sync --upgrade

typecheck:
	uv run mypy scripts/ tests/

test:
	uv run pytest tests/

report:
	mkdir -p data outputs logs
	uv run python scripts/01_sample.py > data/01_sample.json
	uv run python scripts/02_mean.py < data/01_sample.json
	uv run python scripts/99_report.py > outputs/report.html

clean:
	rm -rf data/* outputs/* logs/*

check: typecheck test
