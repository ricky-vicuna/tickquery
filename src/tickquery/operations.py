"""Query-first functions: each call adds one transformation to an immutable plan."""

from collections.abc import Sequence

from tickquery.core import Column, Columns, Expr, Grouped, Query


def where(query: Query, *predicates: Expr) -> Query:
    return query.where(*predicates)


def with_columns(query: Query, *items: Expr) -> Query:
    return query.with_columns(*items)


def group_by(query: Query, *keys: Columns) -> Grouped:
    return query.group_by(*keys)


def agg(grouped: Grouped, *items: Column, **named: Expr) -> Query:
    return grouped.agg(*items, **named)


def having(query: Query, *predicates: Expr) -> Query:
    return query.having(*predicates)


def left_join(left: Query, right: Query, *, on: str | Sequence[str]) -> Query:
    return left.left_join(right, on=on)


def right_join(left: Query, right: Query, *, on: str | Sequence[str]) -> Query:
    return left.right_join(right, on=on)


def asof_join(
    left: Query,
    right: Query,
    *,
    on: str,
    by: Sequence[str] = (),
    strict: bool = False,
    matched: str | None = None,
    tie_break: Sequence[str] = (),
) -> Query:
    return left.asof_join(right, on=on, by=by, strict=strict, matched=matched, tie_break=tie_break)
