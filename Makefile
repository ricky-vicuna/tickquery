.PHONY: check fix fixtures freeze init-dev test test-integration test-ingestion
.DEFAULT_GOAL := check

init-dev:
	uv sync --locked --group dev --extra dolphindb

check:
	@echo "Running check..."
	uv run --locked ruff format --check src tests
	uv run --locked ruff check src tests
	uv run --locked pyright

fix:
	@echo "Running fix..."
	uv run --locked ruff check --fix src tests
	uv run --locked ruff format src tests

# Compiler, Polars reference and (if tests/data exists) market-data invariants. No database.
test:
	uv run --locked python -m unittest discover -s tests -p 'test_[omr]*.py' -v

# Build tests/data from raw tick CSVs: make fixtures RAW="a.csv b.csv"
fixtures:
	@test -n "$(RAW)" || (echo 'Usage: make fixtures RAW="raw1.csv raw2.csv"'; exit 1)
	uv run --locked python tests/fixtures.py --out tests/data $(RAW)

test-integration:
	@test -n "$$DDB_PASSWORD" || (echo "Set DDB_PASSWORD to run integration tests"; exit 1)
	uv run --locked --extra dolphindb python -m unittest discover -s tests -p 'test_[sd]*.py' -v

freeze:
	@echo "Freezing dependencies..."
	uv lock
	uv export --no-emit-project --no-hashes --no-dev --format requirements-txt -o requirements.txt
	uv export --no-emit-project --no-hashes --only-group dev --format requirements-txt -o dev.txt

# Ingest tests/data into a throwaway DolphinDB container, query it with tickquery
# and compare with the Polars reference. Needs Docker or OrbStack and `make fixtures`.
# KEEP_DB=1 leaves the container running for inspection.
DDB_TEST_PORT ?= 18848
TEST_COMPOSE := docker compose -f compose.test.yaml

test-ingestion:
	@test -f tests/data/trades.parquet || (echo 'Run make fixtures RAW="..." first'; exit 1)
	DDB_TEST_PORT=$(DDB_TEST_PORT) $(TEST_COMPOSE) up -d
	DDB_HOST=127.0.0.1 DDB_PORT=$(DDB_TEST_PORT) DDB_USER=admin \
	DDB_PASSWORD=$${DDB_TEST_PASSWORD:-123456} \
	uv run --locked --extra dolphindb python -m unittest discover -s tests -p 'test_ingestion.py' -v; \
	status=$$?; $(if $(KEEP_DB),,$(TEST_COMPOSE) down;) exit $$status
