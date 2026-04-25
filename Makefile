.PHONY: install update typecheck test report clean check \
    setup planner critique commit-plan implementer deterministic semantic index pr \
    methbook graph enrich-graph fetch

export PYTHONPATH := $(CURDIR)

install:
	uv sync

update:
	uv sync --upgrade

typecheck:
	uv run mypy scripts/ tests/ methbooks/

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

setup:
	@test -n "$(SRC)" || (echo "SRC=<pdf-or-md-path> required" && exit 1)
	@uv run python -m methbooks.pipeline.setup "$(SRC)"

planner:
	uv run python -m methbooks.pipeline.planner --run-dir "$(RUN_DIR)"

critique:
	uv run python -m methbooks.pipeline.critique --run-dir "$(RUN_DIR)"

commit-plan:
	uv run python -m methbooks.pipeline.commit_plan --run-dir "$(RUN_DIR)"

implementer:
	uv run python -m methbooks.pipeline.implementer --run-dir "$(RUN_DIR)"

deterministic:
	uv run python -m methbooks.pipeline.deterministic --run-dir "$(RUN_DIR)"

semantic:
	uv run python -m methbooks.pipeline.semantic --run-dir "$(RUN_DIR)"

index:
	uv run python -m methbooks.pipeline.rules_index

pr:
	@test -n "$(RUN_DIR)" || (echo "RUN_DIR=<path> required" && exit 1)
	uv run python -m methbooks.pipeline.pr --run-dir "$(RUN_DIR)"

methbook:
	@test -n "$(SRC)" || (echo "SRC=<pdf-or-md-path> required" && exit 1)
	uv run python -m methbooks.pipeline.run "$(SRC)"

fetch:
	@test -n "$(CODE)" || (echo "CODE=<msci-methodology-code> required" && exit 1)
	uv run python -m methbooks.fetcher.msci_fetch --code "$(CODE)"

graph:
	graphify update methbooks/
	uv run python -m methbooks.pipeline.rules_index
	$(MAKE) enrich-graph

enrich-graph:
	uv run python -m methbooks.pipeline.enrich_graph
