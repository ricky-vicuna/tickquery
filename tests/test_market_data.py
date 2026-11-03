"""Invariants on real SSE/SZSE ticks, using the Polars reference backend.

Each check is stated so a researcher can read it without knowing SQL. The
answers are computed independently of the query plan (brute force or plain
Polars), so a plan that matches them means what it says.

Data: build it with `make fixtures RAW="..."` (see tests/fixtures.py). Market
data is not committed; without it these tests skip.
"""

from __future__ import annotations

import os
import unittest
from datetime import time
from pathlib import Path

import polars as pl
from plans import depth_at, ohlcv_1s

from tickquery import Query

FIXTURES = Path(os.environ.get("TICKQUERY_FIXTURES", Path(__file__).parent / "data"))
HAVE_DATA = (FIXTURES / "trades.parquet").exists() and (FIXTURES / "depth.parquet").exists()


class NoServer:
    def run(self, script: str, /) -> object:
        raise AssertionError("These tests run on the Polars reference backend only")


def in_continuous_hours(ts: pl.Expr) -> pl.Expr:
    t = ts.dt.time()
    return ((t >= time(9, 30)) & (t < time(11, 30))) | ((t >= time(13, 0)) & (t < time(14, 57)))


def bar_close_grid(bars: pl.DataFrame, symbols: pl.Series) -> pl.DataFrame:
    """Every bar-close time seen in the data, for every symbol, plus 13:00:00."""
    closes = bars.select((pl.col("bar_start") + pl.duration(seconds=1)).alias("ts")).unique()
    reopen = closes.select(pl.col("ts").dt.replace(hour=13, minute=0, second=0)).unique()
    times = pl.concat([closes, reopen]).unique().select(pl.col("ts").cast(pl.Datetime("ns")))
    return symbols.unique().to_frame("symbol").join(times, how="cross").sort("symbol", "ts")


@unittest.skipUnless(HAVE_DATA, f"No market-data fixtures in {FIXTURES}")
class MarketDataTests(unittest.TestCase):
    trades: pl.DataFrame
    depth: pl.DataFrame
    bars: pl.DataFrame
    grid: pl.DataFrame

    @classmethod
    def setUpClass(cls) -> None:
        cls.trades = pl.read_parquet(FIXTURES / "trades.parquet")
        cls.depth = pl.read_parquet(FIXTURES / "depth.parquet")
        session = NoServer()
        cls.bars = ohlcv_1s(Query(session, "trades")).reference({"trades": cls.trades}).collect()
        cls.grid = bar_close_grid(cls.bars, cls.trades["symbol"])
        cls.session = session

    def as_of(self, *, strict: bool) -> pl.DataFrame:
        plan = depth_at(Query(self.session, "grid"), Query(self.session, "depth"), strict=strict)
        return plan.reference({"grid": self.grid, "depth": self.depth}).collect()

    # --- The raw data, after normalization ------------------------------------------------

    def test_exchange_time_is_never_after_receive_time(self) -> None:
        for name, frame in (("trades", self.trades), ("depth", self.depth)):
            with self.subTest(table=name):
                late = frame.filter(pl.col("exch_ts") > pl.col("recv_ts"))
                self.assertEqual(late.height, 0, late.head())

    def test_sse_auction_flag_matches_auction_print_time(self) -> None:
        sse = self.trades.filter(pl.col("venue") == "SSE")
        flagged = sse["side"] == "N"
        printed = sse["session"].str.ends_with("_auction_print")
        self.assertTrue((flagged == printed).all())

    def test_szse_depth_prices_are_on_the_trade_price_scale(self) -> None:
        typical = self.trades.group_by("symbol").agg(pl.col("price").median().alias("trade_px"))
        quoted = (
            self.depth.filter(pl.col("session") == "continuous", pl.col("bid1") > 0)
            .join(typical, on="symbol")
            .select(ratio=pl.col("bid1") / pl.col("trade_px"))
        )
        self.assertTrue(quoted["ratio"].is_between(0.8, 1.25).all(), quoted.describe())

    def test_phase_codes_do_not_mark_the_lunch_break(self) -> None:
        # Why sessions come from the clock: SSE keeps reporting T111 at lunch.
        lunch = self.depth.filter(pl.col("venue") == "SSE", pl.col("session") == "lunch")
        self.assertGreater(lunch.height, 0)
        self.assertEqual(lunch["phase_code"].unique().to_list(), ["T111"])

    # --- 1s OHLCV bars ----------------------------------------------------------------------

    def test_bars_respect_price_ordering(self) -> None:
        bad = self.bars.filter(
            (pl.col("low") > pl.min_horizontal("open", "close"))
            | (pl.col("high") < pl.max_horizontal("open", "close"))
        )
        self.assertEqual(bad.height, 0, bad.head())

    def test_bars_conserve_volume_and_trade_count(self) -> None:
        raw = (
            self.trades.filter(pl.col("session") == "continuous")
            .group_by("symbol")
            .agg(pl.col("qty").sum().alias("volume"), pl.len().alias("trades"))
        )
        bars = self.bars.group_by("symbol").agg(pl.col("volume").sum(), pl.col("trades").sum())
        joined = raw.join(bars, on="symbol", how="full", suffix="_bars")
        diff = joined.filter(
            (pl.col("volume") != pl.col("volume_bars"))
            | (pl.col("trades") != pl.col("trades_bars"))
        )
        self.assertEqual(diff.height, 0, diff)

    def test_bars_only_cover_continuous_trading(self) -> None:
        outside = self.bars.filter(~in_continuous_hours(pl.col("bar_start")))
        self.assertEqual(outside.height, 0, outside.head())
        lunch = self.bars.filter(
            pl.col("bar_start").dt.time().is_between(time(11, 30), time(12, 59, 59))
        )
        self.assertEqual(lunch.height, 0)

    def test_open_and_close_are_first_and_last_trade_of_each_second(self) -> None:
        # Independent answer: pick by explicit sort_by inside each group,
        # instead of relying on the plan's sort-then-first().
        order = ["exch_ts", "recv_ts", "seq"]
        expected = (
            self.trades.filter(pl.col("session") == "continuous")
            .group_by("symbol", pl.col("exch_ts").dt.truncate("1s").alias("bar_start"))
            .agg(
                pl.col("price").sort_by(order).first().alias("open"),
                pl.col("price").sort_by(order).last().alias("close"),
            )
        )
        joined = self.bars.join(expected, on=["symbol", "bar_start"], how="full", suffix="_exp")
        wrong = joined.filter(
            (pl.col("open") != pl.col("open_exp")) | (pl.col("close") != pl.col("close_exp"))
        )
        self.assertEqual(joined.height, self.bars.height)
        self.assertEqual(wrong.height, 0, wrong.head())

    # --- Top of book at bar close, without look-ahead ---------------------------------------

    def test_strict_as_of_never_uses_a_snapshot_from_the_grid_time_or_later(self) -> None:
        result = self.as_of(strict=True)
        self.assertEqual(result.height, self.grid.height)
        leaked = result.filter(pl.col("depth_ts") >= pl.col("ts"))
        self.assertEqual(leaked.height, 0, leaked.head())

    def test_inclusive_as_of_would_use_snapshots_stamped_at_the_grid_time(self) -> None:
        # The hazard strict=True removes. With whole-second SSE stamps this is common.
        result = self.as_of(strict=False)
        on_the_boundary = result.filter(pl.col("depth_ts") == pl.col("ts"))
        self.assertGreater(on_the_boundary.height, 0)

    def test_strict_as_of_matches_a_brute_force_search(self) -> None:
        # For each (symbol, grid time): the visible snapshot with the latest
        # availability time, latest in feed order among equals.
        candidates = (
            self.grid.join(self.depth, on="symbol")
            .filter(pl.col("avail_ts") < pl.col("ts"))
            .sort("avail_ts", "seq")
            .group_by("symbol", "ts")
            .agg(pl.col("seq").last(), pl.col("avail_ts").last().alias("depth_ts"))
        )
        expected = self.grid.join(candidates, on=["symbol", "ts"], how="left")
        actual = self.as_of(strict=True).select("symbol", "ts", "seq", "depth_ts")
        joined = expected.join(actual, on=["symbol", "ts"], suffix="_plan")
        self.assertEqual(joined.height, self.grid.height)
        wrong = joined.filter(
            pl.col("seq").ne_missing(pl.col("seq_plan"))
            | pl.col("depth_ts").ne_missing(pl.col("depth_ts_plan"))
        )
        self.assertEqual(wrong.height, 0, wrong.head())


if __name__ == "__main__":
    unittest.main()
