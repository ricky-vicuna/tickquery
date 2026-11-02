.PHONY: check fix fixtures freeze init-dev test test-integration
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
