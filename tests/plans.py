"""Research queries under test, written once and run on both backends."""

from __future__ import annotations

from tickquery import Query, col, count
from tickquery.operations import agg, asof_join, group_by, where


def ohlcv_1s(trades: Query) -> Query:
    """1-second bars of continuous trading, on the exchange clock.

    Bars are [t, t+1s) labelled by t. Auction prints and empty seconds are
    excluded. Ties in exchange time are ordered by receive time, then feed order.
    """
    continuous = where(trades, col("session") == "continuous")
    ordered = continuous.sort("symbol", "exch_ts", "recv_ts", "seq")
    return agg(
        group_by(ordered, "symbol", col("exch_ts").bucket("1s").alias("bar_start")),
        open=col("price").first(),
        high=col("price").max(),
        low=col("price").min(),
        close=col("price").last(),
        volume=col("qty").sum(),
        trades=count(),
    )


def depth_at(grid: Query, depth: Query, *, strict: bool = True) -> Query:
    """Top of book as it was visible just before each grid time.

    A snapshot counts from its availability time (exchange time, plus one
    second for SSE's whole-second stamps). With strict=True a snapshot
    available exactly at the grid time is not used. Several snapshots with
    the same availability time resolve to the last one in feed order.
    """
    visible = depth.select(
        "symbol", col("avail_ts").alias("ts"), "seq", "bid1", "bid1_qty", "ask1", "ask1_qty"
    )
    return asof_join(
        grid,
        visible,
        on="ts",
        by=["symbol"],
        strict=strict,
        matched="depth_ts",
        tie_break=["seq"],
    )
