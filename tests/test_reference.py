"""The Polars reference backend against the same hand-written answers as test_semantics.

No server is needed. If DolphinDB (test_semantics) and Polars (here) both agree
with the hand-written rows, the translation is right for these cases.
"""

from __future__ import annotations

import unittest
from datetime import datetime
from typing import Literal

import polars as pl

from tickquery import Query, col, count
from tickquery.operations import agg, asof_join, group_by, having, left_join, right_join, where


class NoServer:
    def run(self, script: str, /) -> object:
        raise AssertionError("The reference backend must not touch the server")


TABLES = {
    "trades": pl.DataFrame(
        {"symbol": ["A", "A", "B", "C"], "ts": [1, 3, 2, 1], "size": [10, 20, 5, 7]}
    ),
    "quotes": pl.DataFrame(
        {"symbol": ["A", "D", "A"], "ts": [3, 1, 2], "bid": [101.0, 200.0, 100.0]}
    ),
    "reference": pl.DataFrame({"symbol": ["A", "A", "D"], "label": ["x", "y", "z"]}),
    "events": pl.DataFrame({"symbol": ["A", "A", "A", "B"], "ts": [1, 3, 4, 4]}),
}


class ReferenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.session = NoServer()
        self.trades = Query(self.session, "trades")

    def run_plan(self, query: Query) -> list[dict[str, object]]:
        return query.reference(TABLES).collect().to_dicts()

    def test_where_group_count_sum_having(self) -> None:
        positive = where(self.trades, col("size") >= 7)
        totals = agg(group_by(positive, "symbol"), events=count(), volume=col("size").sum())
        result = having(totals, col("volume") > 10)
        self.assertEqual(self.run_plan(result), [{"symbol": "A", "events": 2, "volume": 30}])

    def test_left_and_right_join_duplicates_and_unmatched_rows(self) -> None:
        reference = Query(self.session, "reference")
        left = left_join(self.trades, reference, on="symbol").select("symbol", "ts", "label")
        self.assertEqual(
            self.run_plan(left.sort("symbol", "ts", "label")),
            [
                {"symbol": "A", "ts": 1, "label": "x"},
                {"symbol": "A", "ts": 1, "label": "y"},
                {"symbol": "A", "ts": 3, "label": "x"},
                {"symbol": "A", "ts": 3, "label": "y"},
                # DolphinDB returns "" here; Polars returns null. See README.
                {"symbol": "B", "ts": 2, "label": None},
                {"symbol": "C", "ts": 1, "label": None},
            ],
        )
        right = right_join(self.trades, reference, on="symbol").select("symbol", "label")
        self.assertEqual(
            self.run_plan(right.sort("symbol", "label")),
            [
                {"symbol": "A", "label": "x"},
                {"symbol": "A", "label": "x"},
                {"symbol": "A", "label": "y"},
                {"symbol": "A", "label": "y"},
                {"symbol": "D", "label": "z"},
            ],
        )

    def test_asof_exact_prior_and_no_future_or_cross_symbol_match(self) -> None:
        quotes = Query(self.session, "quotes")
        result = asof_join(Query(self.session, "events"), quotes, on="ts", by=["symbol"])
        self.assertEqual(
            self.run_plan(result.select("symbol", "ts", "bid")),
            [
                {"symbol": "A", "ts": 1, "bid": None},
                {"symbol": "A", "ts": 3, "bid": 101.0},
                {"symbol": "A", "ts": 4, "bid": 101.0},
                {"symbol": "B", "ts": 4, "bid": None},
            ],
        )

    def test_strict_asof_skips_exact_timestamp_and_reports_matched_time(self) -> None:
        quotes = Query(self.session, "quotes")
        result = asof_join(
            Query(self.session, "events"),
            quotes,
            on="ts",
            by=["symbol"],
            strict=True,
            matched="quote_ts",
        )
        self.assertEqual(
            self.run_plan(result.select("symbol", "ts", "bid", "quote_ts")),
            [
                {"symbol": "A", "ts": 1, "bid": None, "quote_ts": None},
                # The quote at ts=3 is not visible at ts=3; the ts=2 quote is.
                {"symbol": "A", "ts": 3, "bid": 100.0, "quote_ts": 2},
                {"symbol": "A", "ts": 4, "bid": 101.0, "quote_ts": 3},
                {"symbol": "B", "ts": 4, "bid": None, "quote_ts": None},
            ],
        )

    def test_asof_takes_the_later_of_duplicate_right_timestamps(self) -> None:
        # Documented DolphinDB behaviour: of two right rows with the same time,
        # the one later in the (sorted) right input wins.
        tables = {
            "left": pl.DataFrame({"ts": [5]}),
            "right": pl.DataFrame({"ts": [5, 5], "qty": [3, 4]}),
        }
        result = asof_join(Query(self.session, "left"), Query(self.session, "right"), on="ts")
        self.assertEqual(result.reference(tables).collect().to_dicts(), [{"ts": 5, "qty": 4}])

    def test_bucket_is_left_closed_and_right_label_shifts_only_the_label(self) -> None:
        tables = {
            "ticks": pl.DataFrame(
                {
                    "ts": [
                        datetime(2026, 10, 30, 9, 30, 0, 0),
                        datetime(2026, 10, 30, 9, 30, 0, 999999),
                        datetime(2026, 10, 30, 9, 30, 1, 0),
                    ],
                    "qty": [1, 2, 4],
                }
            )
        }
        ticks = Query(self.session, "ticks")
        cases: tuple[tuple[Literal["left", "right"], int], ...] = (("left", 0), ("right", 1))
        for label, first_bar in cases:
            with self.subTest(label=label):
                bars = agg(
                    group_by(ticks, col("ts").bucket("1s", label=label).alias("bar")),
                    volume=col("qty").sum(),
                )
                rows = bars.reference(tables).collect().to_dicts()
                self.assertEqual(
                    [(row["bar"].second, row["volume"]) for row in rows],
                    [(first_bar, 3), (first_bar + 1, 4)],
                )


if __name__ == "__main__":
    unittest.main()
