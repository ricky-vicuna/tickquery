"""Small opt-in live checks against hand-written answers, not reference SQL."""

from __future__ import annotations

import os
import unittest
from typing import cast

from tickquery import Query, Session, col, count
from tickquery.operations import (
    agg,
    asof_join,
    group_by,
    having,
    left_join,
    right_join,
    where,
)


@unittest.skipUnless(os.environ.get("DDB_PASSWORD"), "Set DDB_PASSWORD for live fixture tests")
class SemanticsTests(unittest.TestCase):
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
        self.session = cast(Session, session)
        # Only session-local memory tables; no persistent market data is changed.
        self.session.run("""
            trades = table(`A`A`B`C as symbol, 1 3 2 1 as ts, 10 20 5 7 as size);
            quotes = table(`A`D`A as symbol, 3 1 2 as ts, 101.0 200.0 100.0 as bid);
            reference = table(`A`A`D as symbol, "x" "y" "z" as label);
            NULL
        """)
        self.trades = Query(self.session, "trades")

    def test_where_group_count_sum_having(self) -> None:
        positive = where(self.trades, col("size") >= 7)
        totals = agg(group_by(positive, "symbol"), events=count(), volume=col("size").sum())
        result = having(totals, col("volume") > 10).collect()
        self.assertEqual(result.to_dicts(), [{"symbol": "A", "events": 2, "volume": 30}])
        self.assertEqual(self.trades.select(count().alias("n")).collect()["n"].to_list(), [4])

    def test_left_and_right_join_duplicates_and_unmatched_rows(self) -> None:
        reference = Query(self.session, "reference")
        left = left_join(self.trades, reference, on="symbol").select("symbol", "ts", "label")
        self.assertEqual(
            left.sort("symbol", "ts", "label").collect().to_dicts(),
            [
                {"symbol": "A", "ts": 1, "label": "x"},
                {"symbol": "A", "ts": 1, "label": "y"},
                {"symbol": "A", "ts": 3, "label": "x"},
                {"symbol": "A", "ts": 3, "label": "y"},
                {"symbol": "B", "ts": 2, "label": ""},
                {"symbol": "C", "ts": 1, "label": ""},
            ],
        )
        right = right_join(self.trades, reference, on="symbol").select("symbol", "label")
        self.assertEqual(
            right.sort("symbol", "label").collect().to_dicts(),
            [
                {"symbol": "A", "label": "x"},
                {"symbol": "A", "label": "x"},
                {"symbol": "A", "label": "y"},
                {"symbol": "A", "label": "y"},
                {"symbol": "D", "label": "z"},
            ],
        )

    def test_asof_exact_prior_and_no_future_or_cross_symbol_match(self) -> None:
        self.session.run("""
            events = table(`A`A`A`B as symbol, 1 3 4 4 as ts); NULL
        """)
        result = (
            asof_join(
                Query(self.session, "events"),
                Query(self.session, "quotes"),
                on="ts",
                by=["symbol"],
            )
            .select("symbol", "ts", "bid")
            .sort("symbol", "ts")
            .collect()
        )
        self.assertEqual(
            result.to_dicts(),
            [
                {"symbol": "A", "ts": 1, "bid": None},
                {"symbol": "A", "ts": 3, "bid": 101.0},
                {"symbol": "A", "ts": 4, "bid": 101.0},
                {"symbol": "B", "ts": 4, "bid": None},
            ],
        )


if __name__ == "__main__":
    unittest.main()
