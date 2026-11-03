"""Typed query plan for DolphinDB with a local Polars reference backend.

Every operation is recorded as a typed step. The same plan either compiles to a
DolphinDB script (``explain``, ``collect``, ``cache``) or runs locally on Polars
frames (``reference``). Comparing the two on the same data is how the
DolphinDB translation is checked without anyone reading the generated script.
"""

from __future__ import annotations

import json
import logging
import math
import operator
import re
from collections.abc import Callable, Mapping, Sequence
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
type Frame = pl.DataFrame | pl.LazyFrame

_ROW = "__tickquery_row"


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


def native_literal(value: object) -> pl.Expr | None:
    if isinstance(value, Expr):
        return value.native
    literal(value)  # Same validation as the DolphinDB side.
    return pl.lit(value)


@dataclass(frozen=True, eq=False)
class Expr:
    """One expression, kept in both backends.

    ``sql`` is the DolphinDB fragment; ``native`` is the equivalent Polars
    expression (``None`` for raw fragments built directly from a string).
    """

    sql: str
    native: pl.Expr | None = None
    name: str | None = None
    aggregate: bool = False
    ordered: bool = False  # Result depends on row order: first()/last().

    def _binary(
        self, op: str, combine: Callable[[pl.Expr, pl.Expr], pl.Expr], other: object
    ) -> Expr:
        right = native_literal(other)
        native = None if self.native is None or right is None else combine(self.native, right)
        other_aggregate = isinstance(other, Expr) and other.aggregate
        return Expr(
            f"({self.sql} {op} {literal(other)})",
            native,
            aggregate=self.aggregate or other_aggregate,
        )

    def __eq__(self, other: object) -> Expr:  # type: ignore[override]
        return self._binary("=", operator.eq, other)

    def __ne__(self, other: object) -> Expr:  # type: ignore[override]
        return self._binary("!=", operator.ne, other)

    def __gt__(self, other: Value) -> Expr:
        return self._binary(">", operator.gt, other)

    def __ge__(self, other: Value) -> Expr:
        return self._binary(">=", operator.ge, other)

    def __lt__(self, other: Value) -> Expr:
        return self._binary("<", operator.lt, other)

    def __le__(self, other: Value) -> Expr:
        return self._binary("<=", operator.le, other)

    def __and__(self, other: Expr) -> Expr:
        return self._binary("and", operator.and_, other)

    def __or__(self, other: Expr) -> Expr:
        return self._binary("or", operator.or_, other)

    def __bool__(self) -> Never:
        raise TypeError("Use (condition) & (condition), not Python and/or")

    def alias(self, name: str) -> Expr:
        identifier(name)
        native = None if self.native is None else self.native.alias(name)
        return Expr(f"{self.sql} as {name}", native, name, self.aggregate, self.ordered)

    def _aggregate(self, function: Literal["first", "last", "sum", "mean", "max", "min"]) -> Expr:
        if self.aggregate:
            raise ValueError(f"Nested aggregate: {function}({self.sql})")
        native = None if self.native is None else getattr(self.native, function)()
        ordered = function in ("first", "last")
        return Expr(f"{function}({self.sql})", native, aggregate=True, ordered=ordered)

    def last(self) -> Expr:
        return self._aggregate("last")

    def first(self) -> Expr:
        return self._aggregate("first")

    def sum(self) -> Expr:
        return self._aggregate("sum")

    def mean(self) -> Expr:
        return self._aggregate("mean")

    def max(self) -> Expr:
        return self._aggregate("max")

    def min(self) -> Expr:
        return self._aggregate("min")

    def bucket(self, every: str, *, label: Literal["left", "right"] = "left") -> Expr:
        """Fixed-width time bucket: [start, start + every), aligned to the epoch.

        Both backends return nanosecond timestamps.
        """
        match = re.fullmatch(r"([1-9][0-9]*)(ns|us|ms|s|m|H)", every)
        if not match or label not in ("left", "right"):
            raise ValueError("Use a positive fixed interval and label='left'/'right'")
        count, unit = match.groups()
        scale = {"ns": 1, "us": 1000, "ms": 10**6, "s": 10**9, "m": 60 * 10**9, "H": 3600 * 10**9}
        ns = int(count) * scale[unit]
        if ns >= 2**63:
            raise ValueError("Interval exceeds signed 64-bit nanoseconds")
        sql = f"bar(nanotimestamp({self.sql}), {every})"
        native = None
        if self.native is not None:
            polars_every = f"{count}{'h' if unit == 'H' else unit}"
            native = self.native.cast(pl.Datetime("ns")).dt.truncate(polars_every)
            if label == "right":
                native = native + pl.duration(nanoseconds=ns)
        return Expr(f"({sql} + {ns}l)" if label == "right" else sql, native)


def col(name: str) -> Expr:
    return Expr(identifier(name), pl.col(name), name)


def count() -> Expr:
    return Expr("count(*)", pl.len(), aggregate=True)


def expressions(items: Sequence[Columns]) -> tuple[Expr, ...]:
    flat: list[Column] = []
    for item in items:
        flat.extend([item] if isinstance(item, (str, Expr)) else item)
    if not flat:
        raise ValueError("At least one column/expression is required")
    return tuple(col(x) if isinstance(x, str) else x for x in flat)


def sql_list(items: Sequence[Expr]) -> str:
    return ", ".join(item.sql for item in items)


def native(expr: Expr) -> pl.Expr:
    if expr.native is None:
        raise TypeError(f"{expr.sql!r} is a raw script fragment with no Polars equivalent")
    return expr.native


# --- Plan steps --------------------------------------------------------------------------


@dataclass(frozen=True)
class Select:
    items: tuple[Expr, ...]

    def sql(self, source: str) -> str:
        return f"select {sql_list(self.items)} from {source}"

    def run(self, frame: pl.LazyFrame) -> pl.LazyFrame:
        return frame.select([native(e) for e in self.items])


@dataclass(frozen=True)
class WithColumns:
    items: tuple[Expr, ...]

    def sql(self, source: str) -> str:
        return f"select *, {sql_list(self.items)} from {source}"

    def run(self, frame: pl.LazyFrame) -> pl.LazyFrame:
        return frame.with_columns([native(e) for e in self.items])


@dataclass(frozen=True)
class Filter:
    predicates: tuple[Expr, ...]

    def sql(self, source: str) -> str:
        condition = " and ".join(f"({p.sql})" for p in self.predicates)
        return f"select * from {source} where {condition}"

    def run(self, frame: pl.LazyFrame) -> pl.LazyFrame:
        return frame.filter([native(p) for p in self.predicates])


@dataclass(frozen=True)
class Sort:
    items: tuple[Expr, ...]

    def sql(self, source: str) -> str:
        return f"select * from {source} order by {sql_list(self.items)}"

    def run(self, frame: pl.LazyFrame) -> pl.LazyFrame:
        return frame.sort([native(e) for e in self.items], maintain_order=True)


@dataclass(frozen=True)
class Head:
    n: int

    def sql(self, source: str) -> str:
        return f"select top {self.n} * from {source}"

    def run(self, frame: pl.LazyFrame) -> pl.LazyFrame:
        return frame.head(self.n)


@dataclass(frozen=True)
class Aggregate:
    keys: tuple[Expr, ...]
    values: tuple[Expr, ...]

    def sql(self, source: str) -> str:
        return f"select {sql_list(self.values)} from {source} group by {sql_list(self.keys)}"

    def run(self, frame: pl.LazyFrame) -> pl.LazyFrame:
        keys = [native(k) for k in self.keys]
        return frame.group_by(keys, maintain_order=True).agg([native(v) for v in self.values])


type Transform = Select | WithColumns | Filter | Sort | Head | Aggregate


@dataclass(frozen=True)
class JoinStep:
    right: Query
    keys: tuple[str, ...]
    kind: Literal["left", "right", "asof"]
    strict: bool = False  # asof only: match right[on] < left[on] instead of <=.

    def run(self, frame: pl.LazyFrame, right: pl.LazyFrame) -> pl.LazyFrame:
        keys = list(self.keys)
        if self.kind == "left":
            return frame.join(right, on=keys, how="left", maintain_order="left", coalesce=True)
        if self.kind == "right":
            return right.join(frame, on=keys, how="left", maintain_order="left", coalesce=True)
        *by, on = keys
        # Polars needs both sides sorted on `on`; DolphinDB keeps the left order.
        left = frame.with_row_index(_ROW).sort(on, maintain_order=True)
        right = right.sort(on, maintain_order=True)
        joined = left.join_asof(
            right,
            on=on,
            by=by or None,
            strategy="backward",
            allow_exact_matches=not self.strict,
            check_sortedness=False,  # Both sides were just sorted on `on`.
        )
        return joined.sort(_ROW).drop(_ROW)


type Step = Transform | JoinStep


@dataclass(frozen=True)
class Query:
    session: Session
    source: str
    steps: tuple[Step, ...] = ()

    def __post_init__(self) -> None:
        identifier(self.source)

    def _step(self, step: Step) -> Query:
        return Query(self.session, self.source, self.steps + (step,))

    # --- Transformations ----------------------------------------------------------------

    def select(self, *items: Columns) -> Query:
        return self._step(Select(expressions(items)))

    def with_columns(self, *items: Expr) -> Query:
        """Keep every column and add named, row-wise expressions."""
        exprs = expressions(items)
        for expr in exprs:
            if expr.name is None or expr.aggregate:
                raise ValueError(f"with_columns() needs named row-wise expressions: {expr.sql}")
        return self._step(WithColumns(exprs))

    def filter(self, *predicates: Expr) -> Query:
        if not predicates or any(not isinstance(p, Expr) for p in predicates):
            raise TypeError("filter() requires Expr predicates")
        for predicate in predicates:
            if predicate.aggregate:
                raise ValueError(
                    f"Aggregate inside a row filter: {predicate.sql}. "
                    "Aggregate with .agg(name=...) first, then filter on col(name)."
                )
        return self._step(Filter(tuple(predicates)))

    def sort(self, *items: Columns) -> Query:
        exprs = expressions(items)
        for expr in exprs:
            if expr.aggregate or (expr.name is not None and expr.sql != expr.name):
                raise ValueError(f"sort() takes columns or plain expressions, got {expr.sql}")
        return self._step(Sort(exprs))

    def head(self, n: int = 5) -> Query:
        if isinstance(n, bool) or not isinstance(n, int) or n < 0:
            raise ValueError("head() requires a non-negative integer")
        return self._step(Head(n))

    def group_by(self, *keys: Columns) -> Grouped:
        exprs = expressions(keys)
        for expr in exprs:
            if expr.aggregate:
                raise ValueError(f"Group key cannot be an aggregate: {expr.sql}")
        return Grouped(self, exprs)

    def where(self, *predicates: Expr) -> Query:
        return self.filter(*predicates)

    def having(self, *predicates: Expr) -> Query:
        """Filter named aggregate outputs; raw aggregate expressions are rejected."""
        return self.filter(*predicates)

    def is_ordered(self) -> bool:
        """True when the latest row order was set by sort() and not undone by group_by()."""
        for step in reversed(self.steps):
            if isinstance(step, Sort):
                return True
            if isinstance(step, Aggregate):
                return False
        return False

    # --- Joins --------------------------------------------------------------------------

    def _join(
        self,
        right: Query,
        keys: Sequence[str],
        kind: Literal["left", "right", "asof"],
        *,
        strict: bool = False,
    ) -> Query:
        if self.session is not right.session:
            raise ValueError("Joined queries must use the same session")
        if not keys or len(set(keys)) != len(keys):
            raise ValueError("Join keys must be non-empty and unique")
        checked = tuple(identifier(key) for key in keys)
        return self._step(JoinStep(right, checked, kind, strict))

    def left_join(self, right: Query, *, on: str | Sequence[str]) -> Query:
        return self._join(right, [on] if isinstance(on, str) else on, "left")

    def right_join(self, right: Query, *, on: str | Sequence[str]) -> Query:
        return self._join(right, [on] if isinstance(on, str) else on, "right")

    def asof_join(
        self,
        right: Query,
        *,
        on: str,
        by: Sequence[str] = (),
        strict: bool = False,
        matched: str | None = None,
        tie_break: Sequence[str] = (),
    ) -> Query:
        """Backward as-of join; the right input is sorted on (*by, on, *tie_break).

        strict=False matches the latest right row with right[on] <= left[on].
        strict=True requires right[on] < left[on]: a row stamped exactly at the
        left time is not yet visible (use it for bar-close snapshots).
        matched="name" keeps the matched right[on] as a column, so staleness
        (left[on] - name) can be checked. Unmatched rows have a null there.
        tie_break orders right rows that share a timestamp; the last one wins.
        Without it, which duplicate wins is not defined.
        """
        if isinstance(by, str) or isinstance(tie_break, str):
            raise TypeError("by and tie_break take sequences of column names, e.g. ['symbol']")
        keys = [*by, on]
        prepared = right.sort(*keys, *tie_break)
        if matched is not None:
            prepared = prepared.with_columns(col(on).alias(matched))
        return self._join(prepared, keys, "asof", strict=strict)

    # --- DolphinDB backend --------------------------------------------------------------

    def explain(self, name: str = "q_preview") -> str:
        identifier(name)
        return "\n".join(self._compile(name)[0])

    def _compile(self, name: str) -> tuple[list[str], list[str]]:
        source = self.source
        statements: list[str] = []
        temporary = [name]
        for index, step in enumerate(self.steps or (Select((Expr("*", pl.all()),)),)):
            if isinstance(step, JoinStep):
                right_name = f"{name}_r{index}"
                right_sql, right_temps = step.right._compile(right_name)
                statements.extend(right_sql)
                temporary.extend(right_temps)
                if step.strict:
                    # Integer-tick timestamps: right + 1 tick <= left  <=>  right < left.
                    on = step.keys[-1]
                    statements.append(f"update {right_name} set {on} = {on} + 1;")
                keys = "`" + "`".join(step.keys)
                left, right = (right_name, source) if step.kind == "right" else (source, right_name)
                function = "aj" if step.kind == "asof" else "lj"
                statements.append(f"{name} = select * from {function}({left}, {right}, {keys});")
            else:
                statements.append(f"{name} = {step.sql(source)};")
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

    # --- Polars reference backend ---------------------------------------------------------

    def reference(self, tables: Mapping[str, Frame]) -> pl.LazyFrame:
        """Run the same plan locally on Polars frames, keyed by source table name.

        This is the oracle for differential tests, not a fast path. Known
        differences from DolphinDB are listed in the README.
        """
        if self.source not in tables:
            raise KeyError(f"No reference frame for source {self.source!r}")
        frame = tables[self.source].lazy()
        for step in self.steps:
            if isinstance(step, JoinStep):
                frame = step.run(frame, step.right.reference(tables))
            else:
                frame = step.run(frame)
        return frame


@dataclass(frozen=True)
class Grouped:
    query: Query
    keys: tuple[Expr, ...]

    def agg(self, *items: Column, **named: Expr) -> Query:
        values = expressions([*items, *(expr.alias(name) for name, expr in named.items())])
        for value in values:
            if not value.aggregate:
                raise ValueError(
                    f"agg() needs aggregate expressions (sum, mean, first, ...), got {value.sql}"
                )
        if any(value.ordered for value in values) and not self.query.is_ordered():
            raise ValueError(
                "first()/last() depend on row order: call .sort(...) before group_by(), "
                "with a tie-breaker column when timestamps can repeat."
            )
        return self.query._step(Aggregate(self.keys, values))
