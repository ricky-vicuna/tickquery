.PHONY: check fix freeze init-dev test test-integration
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

test:
	uv run --locked python -m unittest discover -s tests -p 'test_operations.py' -v

test-integration:
	@test -n "$$DDB_PASSWORD" || (echo "Set DDB_PASSWORD to run integration tests"; exit 1)
	uv run --locked --extra dolphindb python -m unittest discover -s tests -p 'test_semantics.py' -v

freeze:
	@echo "Freezing dependencies..."
	uv lock
	uv export --no-emit-project --no-hashes --no-dev --format requirements-txt -o requirements.txt
	uv export --no-emit-project --no-hashes --only-group dev --format requirements-txt -o dev.txt
