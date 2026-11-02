"""Load normalized ticks into partitioned DolphinDB tables, as production would.

Follows the pattern of the team's working ingestion POC: a HASH-on-STRING
database, an explicit typed schema, typed pandas uploads and tableInsert in
batches. Only for a throwaway test server: create() drops the database first.
"""

from __future__ import annotations

import os
import time
from collections.abc import Mapping
from typing import Protocol, cast

import pandas as pd
import polars as pl

DB_PATH = "dfs://tickquery_it"
BATCH_SIZE = 50_000
LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1"}


class Server(Protocol):
    """The parts of dolphindb.Session used here (it also satisfies tickquery.Session)."""

    def run(self, script: str, /) -> object: ...
    def upload(self, objects: dict[str, object], /) -> object: ...
    def close(self) -> None: ...


# Column order is the table schema. Symbol is the partition column.
SCHEMAS: dict[str, dict[str, str]] = {
    "trades": {
        "venue": "SYMBOL",
        "symbol": "STRING",
        "exch_ts": "NANOTIMESTAMP",
        "recv_ts": "NANOTIMESTAMP",
        "seq": "LONG",
        "side": "SYMBOL",
        "price": "DOUBLE",
        "qty": "LONG",
        "session": "SYMBOL",
    },
    "depth": {
        "venue": "SYMBOL",
        "symbol": "STRING",
        "exch_ts": "NANOTIMESTAMP",
        "avail_ts": "NANOTIMESTAMP",
        "recv_ts": "NANOTIMESTAMP",
        "seq": "LONG",
        "phase_code": "SYMBOL",
        "bid1": "DOUBLE",
        "bid1_qty": "LONG",
        "ask1": "DOUBLE",
        "ask1_qty": "LONG",
        "session": "SYMBOL",
    },
}


def connect(*, wait_seconds: float = 90.0) -> Server:
    """Connect to a local test server, waiting while a fresh container starts."""
    from dolphindb.session import Session as DolphinSession

    host = os.environ.get("DDB_HOST", "localhost")
    if host not in LOCAL_HOSTS and not os.environ.get("TICKQUERY_ALLOW_REMOTE_DB"):
        raise RuntimeError(
            f"Refusing to drop and recreate {DB_PATH} on {host}. These tests are for a "
            "local throwaway server; set TICKQUERY_ALLOW_REMOTE_DB=1 to override."
        )
    port = int(os.environ.get("DDB_PORT", "8849"))
    user = os.environ.get("DDB_USER", "admin")
    password = os.environ["DDB_PASSWORD"]
    deadline = time.monotonic() + wait_seconds
    while True:
        session = DolphinSession()
        try:
            if session.connect(host, port, user, password):
                session.run("1")  # The server answers queries, not just the socket.
                return cast(Server, session)
        except Exception:
            if time.monotonic() > deadline:
                session.close()
                raise
        session.close()
        if time.monotonic() > deadline:
            raise ConnectionError(f"DolphinDB at {host}:{port} did not become ready")
        time.sleep(2)


def create_script(db_path: str = DB_PATH) -> str:
    lines = [
        f'if (existsDatabase("{db_path}")) {{ dropDatabase("{db_path}") }}',
        f'db = database("{db_path}", HASH, [STRING, 8])',
    ]
    for name, schema in SCHEMAS.items():
        columns = "`" + "`".join(schema)
        types = ",".join(schema.values())
        lines.append(f"schema_{name} = table(1:0, {columns}, [{types}])")
        lines.append(f"db.createPartitionedTable(schema_{name}, `{name}, `symbol)")
    lines.append("NULL")
    return "\n".join(lines)


def to_pandas(frame: pl.DataFrame) -> pd.DataFrame:
    """pandas for upload, with strings as plain objects (pandas 3 defaults to "str")."""
    pdf = frame.to_pandas()
    for name, dtype in frame.schema.items():
        if dtype == pl.String:
            pdf[name] = pdf[name].astype(object)
    return pdf


def typed_frame(frame: pl.DataFrame, schema: Mapping[str, str]) -> pd.DataFrame:
    """A pandas frame in schema order, tagged with DolphinDB types for upload."""
    import dolphindb.settings as keys

    if list(frame.columns) != list(schema):
        frame = frame.select(list(schema))
    pdf = to_pandas(frame)
    pdf.__dict__["__DolphinDB_Type__"] = {
        column: getattr(keys, f"DT_{kind}") for column, kind in schema.items()
    }
    return pdf


def insert(session: Server, name: str, frame: pl.DataFrame, db_path: str = DB_PATH) -> int:
    """Append a normalized frame to db_path/name in batches; returns rows inserted."""
    schema = SCHEMAS[name]
    inserted = 0
    for offset in range(0, frame.height, BATCH_SIZE):
        batch = frame.slice(offset, BATCH_SIZE)
        session.upload({"tq_incoming": typed_frame(batch, schema)})
        try:
            count = session.run(f'tableInsert(loadTable("{db_path}", "{name}"), tq_incoming)')
        finally:
            session.run("undef(`tq_incoming, VAR)")
        if count != batch.height:
            raise RuntimeError(f"{name}: inserted {count} of {batch.height} rows at {offset}")
        inserted += batch.height
    return inserted


def load(session: Server, name: str, db_path: str = DB_PATH) -> pl.DataFrame:
    """Read a whole table back, for round-trip checks."""
    result = session.run(f'select * from loadTable("{db_path}", "{name}")')
    if not isinstance(result, pd.DataFrame):
        raise TypeError(f"Expected a pandas DataFrame, got {type(result).__name__}")
    return pl.from_pandas(result)
