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
