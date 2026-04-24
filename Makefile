.PHONY: install update typecheck test report clean check \
    setup planner critique implementer deterministic semantic index pr \
    methbook graph

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
	uv run python -m methbooks.pipeline.planner --run-dir $(RUN_DIR)

critique:
	uv run python -m methbooks.pipeline.critique --run-dir $(RUN_DIR)

implementer:
	uv run python -m methbooks.pipeline.implementer --run-dir $(RUN_DIR)

deterministic:
	uv run python -m methbooks.pipeline.deterministic --run-dir $(RUN_DIR)

semantic:
	uv run python -m methbooks.pipeline.semantic --run-dir $(RUN_DIR)

index:
	uv run python -m methbooks.pipeline.rules_index

pr:
	@test -n "$(RUN_DIR)" || (echo "RUN_DIR=<path> required" && exit 1)
	@SLUG=$$(basename $$(dirname $(RUN_DIR))); \
	  git push -u origin methbook/$$SLUG && \
	  gh pr create --title "methbook: $$SLUG" --body "$$(cat $(RUN_DIR)/methbook_v2.json)" && \
	  gh pr merge --auto --merge --delete-branch

methbook:
	@test -n "$(SRC)" || (echo "SRC=<pdf-or-md-path> required" && exit 1)
	@RUN_DIR=$$(uv run python -m methbooks.pipeline.setup "$(SRC)") && \
	  $(MAKE) planner RUN_DIR=$$RUN_DIR && \
	  $(MAKE) critique RUN_DIR=$$RUN_DIR && \
	  $(MAKE) implementer RUN_DIR=$$RUN_DIR && \
	  $(MAKE) deterministic RUN_DIR=$$RUN_DIR && \
	  $(MAKE) semantic RUN_DIR=$$RUN_DIR && \
	  $(MAKE) index && \
	  $(MAKE) pr RUN_DIR=$$RUN_DIR

graph:
	graphify update methbooks/
