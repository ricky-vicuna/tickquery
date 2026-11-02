"""Ingest real ticks into DolphinDB DFS tables, then query them with tickquery.

    raw CSV -> fixtures.normalize -> DolphinDB DFS -> tickquery  -> result
    raw CSV -> fixtures.normalize ---------------> Polars reference -> expected

1. Round trip: every row and value written comes back unchanged (ingestion
   loses nothing: no truncated quantities, shifted timestamps or dropped rows).
2. The research plans run on the partitioned tables researchers actually query
   and give the reference answer.

Run with `make test-ingestion` (starts a throwaway container). Never point
this at a shared server: setUpClass drops and recreates dfs://tickquery_it.
"""

from __future__ import annotations

import os
import unittest

import polars as pl
from ingest import DB_PATH, SCHEMAS, Server, connect, create_script, insert, load, to_pandas
from plans import depth_at, ohlcv_1s
from test_market_data import FIXTURES, HAVE_DATA, bar_close_grid

from tickquery import Query
from tickquery.testing import assert_same_result

DEPTH_COLUMNS = ["symbol", "ts", "seq", "bid1", "bid1_qty", "ask1", "ask1_qty", "depth_ts"]


@unittest.skipUnless(os.environ.get("DDB_PASSWORD"), "Run via `make test-ingestion`")
@unittest.skipUnless(HAVE_DATA, f"No market-data fixtures in {FIXTURES}")
class IngestionTests(unittest.TestCase):
    server: Server
    source: dict[str, pl.DataFrame]

    @classmethod
    def setUpClass(cls) -> None:
        cls.source = {
            "trades": pl.read_parquet(FIXTURES / "trades.parquet"),
            "depth": pl.read_parquet(FIXTURES / "depth.parquet"),
        }
        cls.server = connect()
        try:
            cls.server.run(create_script())
            for name, frame in cls.source.items():
                insert(cls.server, name, frame)
            # tickquery queries session variables bound to the DFS tables.
            cls.server.run(
                f'tq_trades = loadTable("{DB_PATH}", "trades")\n'
                f'tq_depth = loadTable("{DB_PATH}", "depth")\n'
                "NULL"
            )
        except BaseException:
            cls.server.close()
            raise

    @classmethod
    def tearDownClass(cls) -> None:
        try:
            if not os.environ.get("TICKQUERY_KEEP_DB"):
                cls.server.run(f'dropDatabase("{DB_PATH}")')
        finally:
            cls.server.close()

    def reference_tables(self) -> dict[str, pl.DataFrame]:
        return {"tq_trades": self.source["trades"], "tq_depth": self.source["depth"]}

    # --- 1. Ingestion keeps every row and value --------------------------------------------

    def test_round_trip_is_exact(self) -> None:
        for name, frame in self.source.items():
            with self.subTest(table=name):
                stored = load(self.server, name)
                self.assertEqual(stored.height, frame.height)
                self.assertEqual(stored.columns, list(SCHEMAS[name]))
                assert_same_result(stored, frame, sort_by=["seq"], rel_tol=0)

    def test_volume_per_symbol_survives_ingestion(self) -> None:
        # The researcher-level version of the round trip, through tickquery itself.
        from tickquery import col, count
        from tickquery.operations import agg, group_by

        plan = agg(
            group_by(Query(self.server, "tq_trades"), "venue", "symbol"),
            volume=col("qty").sum(),
            trades=count(),
        )
        assert_same_result(
            plan.collect(),
            plan.reference(self.reference_tables()).collect(),
            sort_by=["venue", "symbol"],
        )

    # --- 2. Research plans on the DFS tables give the reference answer ------------------------

    def test_ohlcv_1s(self) -> None:
        plan = ohlcv_1s(Query(self.server, "tq_trades"))
        assert_same_result(
            plan.collect(),
            plan.reference(self.reference_tables()).collect(),
            sort_by=["symbol", "bar_start"],
        )

    def test_depth_at_bar_close(self) -> None:
        tables = self.reference_tables()
        bars = ohlcv_1s(Query(self.server, "tq_trades")).reference(tables).collect()
        grid = bar_close_grid(bars, self.source["trades"]["symbol"])
        self.server.upload({"tq_grid": to_pandas(grid)})
        tables["tq_grid"] = grid
        for strict in (True, False):
            with self.subTest(strict=strict):
                plan = depth_at(
                    Query(self.server, "tq_grid"), Query(self.server, "tq_depth"), strict=strict
                ).select(*DEPTH_COLUMNS)
                assert_same_result(
                    plan.collect(),
                    plan.reference(tables).collect(),
                    sort_by=["symbol", "ts"],
                )


if __name__ == "__main__":
    unittest.main()
