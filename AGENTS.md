# Development checks

- Add type annotations to new Python functions and public APIs.
- Keep operation functions and tests readable; preserve transformation order.
- Use `make init-dev` to install the locked development environment.
- Before completing Python changes, run `make check` and the relevant tests.
  Ruff formatting, Ruff lint, and Pyright must pass. Use `make fix` for automatic
  fixes, then rerun `make check`.
- `make test` runs unit tests without a database.
- For query execution changes, run `make test-integration` against a test server
  with `DDB_PASSWORD` supplied through the environment. Never commit credentials.
- Update dependency exports with `make freeze` when dependencies change.
