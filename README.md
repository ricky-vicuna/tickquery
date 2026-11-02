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
  `max`, `min`, and `bucket(every, label="left" | "right")`.
- Queries: `filter`, `select`, `with_columns`, ascending `sort`, `head`,
  `group_by(...).agg(...)`.
- `reference(tables)`: run the same plan locally on Polars frames (keyed by
  source table name) and get a `pl.LazyFrame`. This is the oracle for tests,
  not a fast path.
- `tickquery.testing.assert_same_result(actual, expected, sort_by=...)`: compare a
  DolphinDB result with a reference result, ignoring row order and dtype width.
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

`first()`/`last()` require a `sort()` earlier in the plan (and not undone by a
later `group_by`); otherwise `agg()` raises. Include a sequence column in the
sort when timestamps can tie.
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
  clause. Use `col("volume") > 100`; `col("size").sum() > 100` raises, in
  `having` and `where` alike.
- `agg` only accepts aggregate expressions; group keys cannot be aggregates;
  `sort` rejects aliased or aggregate expressions.
- `left_join` and `right_join` accept one or more same-named equality keys.
  Duplicate keys produce all matches. Right join uses a reversed left join, so
  output column order starts with the original right input.
- `asof_join` matches on `right[on] <= left[on]`, with optional equality keys in
  `by`. It sorts the transformed right input by `by`, `on` and `tie_break`
  automatically. There is no forward/nearest mode or tolerance.
  - `strict=True` matches `right[on] < left[on]`: a row stamped exactly at the
    left time is not visible yet. Use it for snapshots at bar close. On
    DolphinDB this adds one tick to the right time before `aj` (exact for
    integer-tick temporal types).
  - `matched="name"` keeps the matched right time as a column, so staleness can
    be checked; it is null when nothing matched.
  - `tie_break=["seq"]` orders right rows that share a timestamp; the last one
    wins. Without it, which duplicate wins is not defined.
- Both inputs must use the same session. Filter before joining large histories:
  each right-hand plan is materialized on the server. All generated temporary
  variables, including nested join branches, are cleaned up after execution.
- Join keys must have the same names on both sides. Alias overlapping non-key
  columns before joining and select explicit outputs; automatic suffix naming is
  delegated to DolphinDB and may include generated temporary-table names.
- Native DolphinDB null behavior is retained: an unmatched STRING value is the
  empty string, while a missing DOUBLE becomes NaN in Pandas / null in Polars.

Known differences of the Polars reference from DolphinDB (representation only):
unmatched strings are null instead of `""`; counts are UInt32; overlapping
non-key join columns get a `_right` suffix instead of a table-name prefix;
`asof_join` drops the right join-time column (use `matched=`).

### How correctness is checked

Researchers should not need to read DolphinDB scripts to trust a result. Each
layer answers a different question:

| Layer | Question | File | Needs |
|---|---|---|---|
| Compiler | Is the script what we intend, and are mistakes rejected? | `test_operations.py` | nothing |
| Reference | Does the Polars backend give the hand-written answers? | `test_reference.py` | nothing |
| Market data | On real ticks, does each plan mean what it says? | `test_market_data.py` | fixtures |
| Semantics | Does DolphinDB give the hand-written answers? | `test_semantics.py` | server |
| Differential | Does DolphinDB give the reference answer on real ticks? | `test_differential.py` | server + fixtures |

The market-data tests state invariants a researcher can read and challenge:
volume and trade count are conserved by 1s bars; bars cover only continuous
trading; open/close are the first/last trade by (exchange time, receive time,
feed order); top-of-book at bar close never uses a snapshot available at or
after the bar close, and matches a brute-force search. Expected answers are
computed independently of the plan under test.

The queries themselves live in `tests/plans.py` and run unchanged on both
backends.

Some choices are spec, not code, and no test can decide them. They are written
down in `tests/fixtures.py` and `tests/plans.py` for review, for example: when an
SSE depth snapshot with a whole-second timestamp becomes visible (here: one
second later), and whether lunch-break snapshots may be used at 13:00.

### Market-data fixtures

Raw vendor data is not committed (`tests/data/` is git-ignored). Build it from
raw SSE/SZSE tick CSVs:

```bash
make fixtures RAW="20261030-Stock-0.csv 20261030-Stock-1.csv 20261030-Stock-60.csv"
```

`tests/fixtures.py` documents and fixes the raw-data traps: venue-specific
exchange-time formats (SSE has no date and no leading zero before 10:00), SZSE
depth prices scaled by 100, whole-second SSE depth stamps, and phase codes that
do not mark the lunch break. Without fixtures the market-data tests skip;
`TICKQUERY_FIXTURES` points them at another directory.

### Daily tests (no server)

```bash
make test
```

### Optional semantic checks (test server)

```bash
uv sync --extra dolphindb
export DDB_PASSWORD='YOUR_TEST_PASSWORD'
make test-integration
```

`test_differential.py` uploads the fixtures as session-local tables and compares
both plans with the reference. Three further tests create tiny session-local tables and compare results with
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
make test           # Compiler, reference and market-data tests; no database
make fixtures RAW="..."  # Build tests/data from raw tick CSVs
make test-integration  # Live semantic and differential tests; requires DDB_PASSWORD
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
