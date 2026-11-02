"""Helpers for comparing DolphinDB results with the Polars reference backend."""

from __future__ import annotations

from collections.abc import Sequence

import polars as pl
from polars.testing import assert_frame_equal


def normalize(frame: pl.DataFrame, *, empty_string_as_null: bool = False) -> pl.DataFrame:
    """Make dtypes comparable across backends.

    DolphinDB returns INT counts, NANOTIMESTAMP buckets and "" for unmatched
    strings; Polars returns UInt32 counts, its own time units and null. Those are
    representation differences, not semantic ones, so they are removed here.
    """
    columns: list[pl.Expr] = []
    for name, dtype in frame.schema.items():
        column = pl.col(name)
        if dtype.is_integer():
            column = column.cast(pl.Int64)
        elif dtype.is_float():
            column = column.cast(pl.Float64)
        elif isinstance(dtype, pl.Datetime):
            column = column.cast(pl.Datetime("ns"))
        elif dtype == pl.String and empty_string_as_null:
            column = pl.when(column == "").then(None).otherwise(column)
        columns.append(column.alias(name))
    return frame.select(columns)


def assert_same_result(
    actual: pl.DataFrame,
    expected: pl.DataFrame,
    *,
    sort_by: Sequence[str],
    empty_string_as_null: bool = False,
    rel_tol: float = 1e-9,
) -> None:
    """Assert two results hold the same rows, ignoring row order and dtype width."""
    left = normalize(actual, empty_string_as_null=empty_string_as_null)
    right = normalize(expected, empty_string_as_null=empty_string_as_null)
    if sorted(left.columns) != sorted(right.columns):
        raise AssertionError(f"Columns differ: {left.columns} != {right.columns}")
    # pandas turns integer columns with nulls into floats; compare those as floats.
    mixed = [
        name
        for name in right.columns
        if left.schema[name] != right.schema[name]
        and left.schema[name].is_numeric()
        and right.schema[name].is_numeric()
    ]
    left = left.with_columns(pl.col(mixed).cast(pl.Float64))
    right = right.with_columns(pl.col(mixed).cast(pl.Float64))
    keys = list(sort_by)
    assert_frame_equal(
        left.select(right.columns).sort(keys, nulls_last=True),
        right.sort(keys, nulls_last=True),
        check_exact=False,
        rel_tol=rel_tol,
    )
