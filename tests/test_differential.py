"""DolphinDB vs the Polars reference, on the same real ticks and the same plans.

test_market_data shows the plans mean what they say (on Polars). This file
shows DolphinDB computes the same thing. Together: the DolphinDB result is
right, without anyone reading the generated script.

Needs a test server (DDB_PASSWORD) and fixtures (see tests/fixtures.py).
Uploads session-local tables only; no DFS table is touched.
"""

from __future__ import annotations

import os
import unittest
from typing import cast

import polars as pl
from plans import depth_at, ohlcv_1s
from test_market_data import FIXTURES, HAVE_DATA, bar_close_grid

from tickquery import Query, Session
from tickquery.testing import assert_same_result

DEPTH_COLUMNS = ["symbol", "ts", "seq", "bid1", "bid1_qty", "ask1", "ask1_qty", "depth_ts"]


@unittest.skipUnless(os.environ.get("DDB_PASSWORD"), "Set DDB_PASSWORD for live tests")
@unittest.skipUnless(HAVE_DATA, f"No market-data fixtures in {FIXTURES}")
class DifferentialTests(unittest.TestCase):
    def setUp(self) -> None:
        from dolphindb.session import Session as DolphinSession

        session = DolphinSession()
        self.addCleanup(session.close)
        self.assertTrue(
            session.connect(
                os.environ.get("DDB_HOST", "localhost"),
                int(os.environ.get("DDB_PORT", "8849")),
                os.environ.get("DDB_USER", "admin"),
                os.environ["DDB_PASSWORD"],
            )
        )
        self.tables = {
            "tq_trades": pl.read_parquet(FIXTURES / "trades.parquet"),
            "tq_depth": pl.read_parquet(FIXTURES / "depth.parquet"),
        }
        session.upload({name: frame.to_pandas() for name, frame in self.tables.items()})
        self.ddb = session
        self.session = cast(Session, session)

    def test_ohlcv_1s(self) -> None:
        plan = ohlcv_1s(Query(self.session, "tq_trades"))
        assert_same_result(
            plan.collect(),
            plan.reference(self.tables).collect(),
            sort_by=["symbol", "bar_start"],
        )

    def test_depth_at_bar_close(self) -> None:
        bars = ohlcv_1s(Query(self.session, "tq_trades")).reference(self.tables).collect()
        grid = bar_close_grid(bars, self.tables["tq_trades"]["symbol"])
        self.ddb.upload({"tq_grid": grid.to_pandas()})
        tables = {**self.tables, "tq_grid": grid}
        for strict in (True, False):
            with self.subTest(strict=strict):
                plan = depth_at(
                    Query(self.session, "tq_grid"), Query(self.session, "tq_depth"), strict=strict
                ).select(*DEPTH_COLUMNS)
                assert_same_result(
                    plan.collect(),
                    plan.reference(tables).collect(),
                    sort_by=["symbol", "ts"],
                )


if __name__ == "__main__":
    unittest.main()
