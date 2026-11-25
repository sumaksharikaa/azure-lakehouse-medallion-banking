PY        ?= python
START     ?= 2026-11-01
END       ?= 2026-11-03
DAYS      ?= 3

.PHONY: install data run demo test test-delta lint fmt wheel clean

install:            ## Install package with local Spark + dev tools
	$(PY) -m pip install -e ".[local,dev]"

data:               ## Generate synthetic landing-zone batches
	$(PY) data_generator/generate_banking_data.py --out data/landing --start-date $(START) --days $(DAYS)

run:                ## Run Bronze -> Silver -> Gold for START..END
	$(PY) -m lakehouse.pipeline --start-date $(START) --end-date $(END)

demo: clean data run ## Fresh end-to-end demo

test:               ## Unit tests (no Delta jars needed)
	$(PY) -m pytest -q

test-delta:         ## End-to-end tests on real Delta tables
	LAKEHOUSE_DELTA_TESTS=1 $(PY) -m pytest -q -m delta

lint:
	ruff check . && ruff format --check .

fmt:
	ruff check --fix . && ruff format .

wheel:
	$(PY) -m pip wheel . --no-deps -w dist/

clean:
	rm -rf data/ dist/ build/ spark-warehouse/ metastore_db/ .pytest_cache
