"""Small DolphinDB expression DSL; ordered server-side stages, no local fallback."""

from __future__ import annotations

import json
import logging
import math
import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Never, Protocol, overload
from uuid import uuid4

import pandas as pd
import polars as pl


class Session(Protocol):
    def run(self, script: str, /) -> object: ...


type Value = Expr | str | int | float | bool | datetime
type Column = str | Expr
type Columns = Column | Sequence[Column]
type Step = tuple[str, str]


@dataclass(frozen=True)
class JoinStep:
    right: Query
    keys: tuple[str, ...]
    kind: Literal["left", "right", "asof"]


def identifier(name: str) -> str:
    if not isinstance(name, str) or not re.fullmatch(r"[A-Za-z][A-Za-z_0-9]*", name):
        raise ValueError(f"Invalid identifier: {name!r}")
    return name


def literal(value: object) -> str:
    if isinstance(value, Expr):
        return value.sql
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            raise ValueError("Convert to the dataset's timezone, then remove tzinfo")
        return value.strftime("%Y.%m.%dT%H:%M:%S.%f") + "000"
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, (int, float)) and math.isfinite(value):
        return repr(value)
    raise TypeError(f"Unsupported literal: {value!r}")


@dataclass(frozen=True, eq=False)
class Expr:
    sql: str

    def _binary(self, op: str, other: object) -> Expr:
        return Expr(f"({self.sql} {op} {literal(other)})")

    def __eq__(self, other: object) -> Expr:  # type: ignore[override]
        return self._binary("=", other)

    def __ne__(self, other: object) -> Expr:  # type: ignore[override]
        return self._binary("!=", other)

    def __gt__(self, other: Value) -> Expr:
        return self._binary(">", other)

    def __ge__(self, other: Value) -> Expr:
        return self._binary(">=", other)

    def __lt__(self, other: Value) -> Expr:
        return self._binary("<", other)

    def __le__(self, other: Value) -> Expr:
        return self._binary("<=", other)

    def __and__(self, other: Expr) -> Expr:
        return self._binary("and", other)

    def __or__(self, other: Expr) -> Expr:
        return self._binary("or", other)

    def __bool__(self) -> Never:
        raise TypeError("Use (condition) & (condition), not Python and/or")

    def alias(self, name: str) -> Expr:
        return Expr(f"{self.sql} as {identifier(name)}")

    def last(self) -> Expr:
        return Expr(f"last({self.sql})")

    def first(self) -> Expr:
        return Expr(f"first({self.sql})")

    def sum(self) -> Expr:
        return Expr(f"sum({self.sql})")

    def mean(self) -> Expr:
        return Expr(f"mean({self.sql})")

    def bucket(self, every: str, *, label: Literal["left", "right"] = "left") -> Expr:
        match = re.fullmatch(r"([1-9][0-9]*)(ns|us|ms|s|m|H)", every)
        if not match or label not in ("left", "right"):
            raise ValueError("Use a positive fixed interval and label='left'/'right'")
        count, unit = match.groups()
        scale = {"ns": 1, "us": 1000, "ms": 10**6, "s": 10**9, "m": 60 * 10**9, "H": 3600 * 10**9}
        ns = int(count) * scale[unit]
        if ns >= 2**63:
            raise ValueError("Interval exceeds signed 64-bit nanoseconds")
        sql = f"bar(nanotimestamp({self.sql}), {every})"
        return Expr(f"({sql} + {ns}l)" if label == "right" else sql)


def col(name: str) -> Expr:
    return Expr(identifier(name))


def count() -> Expr:
    return Expr("count(*)")


def columns(items: Sequence[Columns]) -> str:
    flat: list[Column] = []
    for item in items:
        flat.extend([item] if isinstance(item, (str, Expr)) else item)
    if not flat:
        raise ValueError("At least one column/expression is required")
    return ", ".join(x.sql if isinstance(x, Expr) else identifier(x) for x in flat)


@dataclass(frozen=True)
class Query:
    session: Session
    source: str
    steps: tuple[Step | JoinStep, ...] = ()

    def __post_init__(self) -> None:
        identifier(self.source)

    def _step(self, template: Step) -> Query:
        return Query(self.session, self.source, self.steps + (template,))

    def select(self, *items: Columns) -> Query:
        return self._step((f"select {columns(items)} from ", ""))

    def filter(self, *predicates: Expr) -> Query:
        if not predicates or any(not isinstance(p, Expr) for p in predicates):
            raise TypeError("filter() requires Expr predicates")
        condition = " and ".join(f"({p.sql})" for p in predicates)
        return self._step(("select * from ", f" where {condition}"))

    def sort(self, *items: Columns) -> Query:
        return self._step(("select * from ", f" order by {columns(items)}"))

    def head(self, n: int = 5) -> Query:
        if isinstance(n, bool) or not isinstance(n, int) or n < 0:
            raise ValueError("head() requires a non-negative integer")
        return self._step((f"select top {n} * from ", ""))

    def group_by(self, *keys: Columns) -> Grouped:
        return Grouped(self, columns(keys))

    def where(self, *predicates: Expr) -> Query:
        return self.filter(*predicates)

    def having(self, *predicates: Expr) -> Query:
        """Filter named aggregate outputs; do not pass raw aggregate expressions."""
        return self.filter(*predicates)

    def _join(
        self, right: Query, keys: Sequence[str], kind: Literal["left", "right", "asof"]
    ) -> Query:
        if self.session is not right.session:
            raise ValueError("Joined queries must use the same session")
        if not keys or len(set(keys)) != len(keys):
            raise ValueError("Join keys must be non-empty and unique")
        checked = tuple(identifier(key) for key in keys)
        return Query(self.session, self.source, self.steps + (JoinStep(right, checked, kind),))

    def left_join(self, right: Query, *, on: str | Sequence[str]) -> Query:
        return self._join(right, [on] if isinstance(on, str) else on, "left")

    def right_join(self, right: Query, *, on: str | Sequence[str]) -> Query:
        return self._join(right, [on] if isinstance(on, str) else on, "right")

    def asof_join(self, right: Query, *, on: str, by: Sequence[str] = ()) -> Query:
        """Backward, exact matches allowed; right input sorted on matching keys."""
        if isinstance(by, str):
            raise TypeError("by must be a sequence of column names, e.g. ['symbol']")
        keys = [*by, on]
        return self._join(right.sort(*keys), keys, "asof")

    def explain(self, name: str = "q_preview") -> str:
        identifier(name)
        return "\n".join(self._compile(name)[0])

    def _compile(self, name: str) -> tuple[list[str], list[str]]:
        source = self.source
        statements: list[str] = []
        temporary = [name]
        for index, step in enumerate(self.steps or (("select * from ", ""),)):
            if isinstance(step, JoinStep):
                right_name = f"{name}_r{index}"
                right_sql, right_temps = step.right._compile(right_name)
                statements.extend(right_sql)
                temporary.extend(right_temps)
                keys = "`" + "`".join(step.keys)
                left, right = (right_name, source) if step.kind == "right" else (source, right_name)
                function = "aj" if step.kind == "asof" else "lj"
                statements.append(f"{name} = select * from {function}({left}, {right}, {keys});")
            else:
                prefix, suffix = step
                statements.append(f"{name} = {prefix}{source}{suffix};")
            source = name
        return statements, temporary

    def _run(self, target: str | None = None) -> object:
        temp = "q_" + uuid4().hex
        statements, temporary = self._compile(temp)
        ending = f"{target} = {temp}; NULL" if target else temp
        try:
            return self.session.run("\n".join([*statements, ending]))
        finally:
            try:
                self.session.run(f"undef(`{'`'.join(temporary)}, VAR)")
            except Exception:  # Cleanup must preserve the original query error.
                logging.getLogger(__name__).warning("Could not clean up %s", temp, exc_info=True)

    def cache(self, name: str) -> Query:
        """Materialize in this session; the caller owns the named server table."""
        self._run(identifier(name))
        return Query(self.session, name)

    @overload
    def collect(self, *, engine: Literal["polars"] = "polars") -> pl.DataFrame: ...
    @overload
    def collect(self, *, engine: Literal["pandas"]) -> pd.DataFrame: ...
    def collect(
        self, *, engine: Literal["polars", "pandas"] = "polars"
    ) -> pl.DataFrame | pd.DataFrame:
        if engine not in ("polars", "pandas"):
            raise ValueError("engine must be 'polars' or 'pandas'")
        result = self._run()
        if not isinstance(result, pd.DataFrame):
            raise TypeError(f"Expected a pandas DataFrame, got {type(result).__name__}")
        return pl.from_pandas(result) if engine == "polars" else result


@dataclass(frozen=True)
class Grouped:
    query: Query
    keys: str

    def agg(self, *items: Column, **named: Expr) -> Query:
        expressions = list(items) + [expr.alias(name) for name, expr in named.items()]
        return self.query._step((f"select {columns(expressions)} from ", f" group by {self.keys}"))
