# tickquery

A small, typed, composable Python query API for **DolphinDB**, with Pandas and
Polars results. Python 3.12+ is required. Version 0.1.0 targets DolphinDB only;
this is not yet a database-independent compiler.

## Install in another project

Install directly from GitHub with uv:

```bash
uv add 'tickquery[dolphindb] @ git+https://github.com/ricky-vicuna/tickquery.git'
```

Or with pip:

```bash
python -m pip install 'tickquery[dolphindb] @ git+https://github.com/ricky-vicuna/tickquery.git'
```

From this checkout (adjust the absolute path to your machine):

```bash
python -m pip install '/Users/ricky/data_analysis/packages/tickquery[dolphindb]'
```

For editable development:

```bash
python -m pip install -e '/Users/ricky/data_analysis/packages/tickquery[dolphindb]'
```

With uv, run from the consuming project:

```bash
uv add '/Users/ricky/data_analysis/packages/tickquery[dolphindb]'
```

The `dolphindb` extra installs the official client. Omit it if your application
already provides a session implementing `run(script: str) -> object`.
This package has not been published to PyPI; use GitHub, a local path, or a built wheel.

## Usage

```python
import os

import dolphindb as ddb
from tickquery import Query, col, count

session = ddb.session()
try:
    if not session.connect("localhost", 8849, "admin", os.environ["DDB_PASSWORD"]):
        raise ConnectionError("Could not connect to DolphinDB")
    session.run('trades = loadTable("dfs://python_ticks", "python_trades"); NULL')
    trades = Query(session, "trades")

    first_ten = (
        trades.filter(col("security_id") == "000338")
        .sort("recv_dt")
        .head(10)
        .collect()
    )

    counts = (
        trades.group_by(col("recv_dt").bucket("1m").alias("minute_dt"))
        .agg(event_count=count())
        .sort("minute_dt")
    )
    print(counts.explain())
    average = counts.select(col("event_count").mean().alias("total_events"))
    print(average.collect(engine="pandas"))
finally:
    session.close()
```

## Public API

- `Query(session, source)`: reference a table variable in the connected session.
- `col(name)`, `count()`: build column expressions and `count(*)`.
- Expressions: comparisons, `&`, `|`, `alias`, `first`, `last`, `sum`, `mean`,
  and `bucket(every, label="left" | "right")`.
- Queries: `filter`, `select`, ascending `sort`, `head`, `group_by(...).agg(...)`.
- `explain()`: inspect the generated server script without running it.
- `cache(name)`: execute and retain a named session-local table; reusing a name
  overwrites it. The caller owns the cache lifetime.
- `collect()`: execute and download a Polars DataFrame.
- `collect(engine="pandas")`: execute and download a Pandas DataFrame.
- `Session`, `Expr`, and `Grouped` are exported for type annotations.

Type annotations ship with a PEP 561 `py.typed` marker. Plans are immutable and
lazy until `cache` or `collect`. The caller owns the connection.

## Semantics and limits

All query operations execute on DolphinDB. Intermediate steps materialize in
a temporary server variable, cleaned up after execution. There is no query
optimizer: put selective filters first. Each `collect` reruns the plan.

Buckets are left-closed/right-open. `label="right"` shifts the label to the
bucket's right edge; it does not change membership. Empty buckets are not filled.
Supported interval units: `ns`, `us`, `ms`, `s`, `m`, `H`.

Sort before `last()` and include a sequence column when timestamps can tie.
Naive Python datetimes must already follow the dataset's timezone convention;
timezone-aware inputs are rejected. Schema validation is delegated to DolphinDB.
The Polars result path converts the SDK's Pandas result locally.

This is a small query DSL, not arbitrary Python-to-SQL translation. Direct `Expr`
construction accepts script fragments and is not an untrusted-code sandbox.

## Build a distributable wheel

From the standalone tickquery repository root:

```bash
uv build --out-dir dist
```

Copy `dist/tickquery-0.1.0-py3-none-any.whl` to another machine and install:

```bash
python -m pip install './tickquery-0.1.0-py3-none-any.whl[dolphindb]'
```

The wheel contains only the package, metadata, documentation metadata, and typing
marker, not research data, credentials, or notebooks.

## Operation functions and tests

Functions in `tickquery.operations` accept the current query as their first
argument. Each call adds a transformation; execution order follows call order.
The equivalent Query methods remain available.

```python
from tickquery import Query, col, count
from tickquery.operations import where, group_by, agg, having, left_join, asof_join

trades = Query(session, "trades")
positive = where(trades, col("trade_quantity") > 0)
grouped = group_by(positive, "security_id")
totals = agg(grouped, events=count(), volume=col("trade_quantity").sum())
large = having(totals, col("volume") >= 100)
df = large.collect()

enriched = left_join(trades, Query(session, "instruments"), on="security_id")
aligned = asof_join(
    trades, Query(session, "quotes"), on="recv_dt", by=["security_id"],
)
```

Contracts:

- `where` is an alias for `filter`.
- `having` filters **named output columns after aggregation**. It generates an
  outer `where` over the materialized aggregation, not a literal SQL `HAVING`
  clause. Use `col("volume") > 100`, not `col("size").sum() > 100`. It can also
  filter any other intermediate result; this POC does not validate aggregate scope.
- `left_join` and `right_join` accept one or more same-named equality keys.
  Duplicate keys produce all matches. Right join uses a reversed left join, so
  output column order starts with the original right input.
- `asof_join` matches on `right[on] <= left[on]`, with optional equality keys in
  `by`. It sorts the transformed right input by `by` and `on` automatically.
  There is no forward/nearest mode or tolerance. Avoid duplicate right timestamps
  within a group when a deterministic choice is needed.
- Both inputs must use the same session. Filter before joining large histories:
  each right-hand plan is materialized on the server. All generated temporary
  variables, including nested join branches, are cleaned up after execution.
- Join keys must have the same names on both sides. Alias overlapping non-key
  columns before joining and select explicit outputs; automatic suffix naming is
  delegated to DolphinDB and may include generated temporary-table names.
- Native DolphinDB null behavior is retained: an unmatched STRING value is the
  empty string, while a missing DOUBLE becomes NaN in Pandas / null in Polars.

### Daily tests (no server)

From this package's repository root, after installing the package:

```bash
uv run python -m unittest discover -s tests -p 'test_operations.py' -v
```

Nine tests cover where/group/count/sum/having, each join type, transformed join
inputs, ordering, expression serialization, invalid joins, and execution cleanup.
Each expected plan is written explicitly. A small recording session tests Python
behavior; it does not pretend to execute SQL.

### Optional semantic checks (test server)

```bash
uv sync --extra dolphindb
export DDB_PASSWORD='YOUR_TEST_PASSWORD'
uv run python -m unittest discover -s tests -v
```

Three additional tests create tiny session-local tables and compare results with
hand-written expected rows. They cover duplicate/unmatched join keys, unsorted
ASOF input, exact/prior matches, no future matches, per-symbol isolation, count,
sum and post-aggregation filtering. No DFS table is modified. The connection is
closed after every test. Without `DDB_PASSWORD` these tests skip; defaults are
`DDB_HOST=localhost`, `DDB_PORT=8849`, `DDB_USER=admin`.

## Development workflow

Install uv, then run from the repository root:

```bash
make init-dev       # Install locked dependencies and the DolphinDB client
make check          # Ruff formatting + lint + Pyright (default make target)
make fix            # Apply Ruff lint fixes and format Python files
make test           # Nine unit tests, no database required
make test-integration  # Three fixture tests; requires DDB_PASSWORD
make freeze         # Refresh uv.lock, requirements.txt and dev.txt
```

`make test-integration` fails early when credentials are missing, rather than
reporting a successful run of skipped tests. Export credentials in your shell;
do not put them in the Makefile or commit them.

GitHub Actions runs `make check`, `make test`, and a package build on pull requests
and pushes to `main`. Integration tests run locally against the test server;
GitHub-hosted runners cannot reach a developer's localhost DolphinDB instance.
Configure the **Ruff, Pyright and unit tests** check as required in GitHub branch
protection if merges must be blocked on failures.

The Makefile uses uv's locked environment. `requirements.txt` contains runtime
dependencies and `dev.txt` contains development tools only. They are generated
exports; update `pyproject.toml` first and run `make freeze`.
