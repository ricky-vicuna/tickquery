"""Readable compiler and lifecycle tests. No DolphinDB server is required."""

# ruff: noqa: FLY002 -- one SQL statement per line keeps expected plans readable.
from __future__ import annotations

import unittest
from datetime import datetime

import pandas as pd

from tickquery import Query, col, count
from tickquery.operations import (
    agg,
    asof_join,
    group_by,
    having,
    left_join,
    right_join,
    where,
)


class RecordingSession:
    def __init__(self, *, fail: bool = False) -> None:
        self.calls: list[str] = []
        self.fail = fail

    def run(self, script: str, /) -> object:
        self.calls.append(script)
        if script.startswith("undef(") or script.endswith("; NULL"):
            return None
        if self.fail:
            raise RuntimeError("query failed")
        return pd.DataFrame({"volume": [30]})


class OperationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.session = RecordingSession()
        self.trades = Query(self.session, "trades")
        self.quotes = Query(self.session, "quotes")

    def test_where_then_group_count_sum_then_having(self) -> None:
        # Each named function is one step. HAVING uses the resulting column name.
        filtered = where(self.trades, col("venue") == "SZSE", col("size") > 0)
        grouped = group_by(filtered, "symbol")
        totals = agg(grouped, events=count(), volume=col("size").sum())
        result = having(totals, col("volume") >= 100)

        self.assertEqual(
            result.explain(),
            "\n".join(
                [
                    'q_preview = select * from trades where ((venue = "SZSE")) and ((size > 0));',
                    "q_preview = select count(*) as events, sum(size) as volume from q_preview group by symbol;",
                    "q_preview = select * from q_preview where ((volume >= 100));",
                ]
            ),
        )
        self.assertEqual(self.trades.steps, ())
        self.assertEqual(self.session.calls, [])

    def test_left_join_keeps_both_input_transformations(self) -> None:
        left = where(self.trades, col("size") > 0)
        right = self.quotes.select("symbol", "bid")
        result = left_join(left, right, on="symbol").select("symbol", "bid")
        self.assertEqual(
            result.explain(),
            "\n".join(
                [
                    "q_preview = select * from trades where ((size > 0));",
                    "q_preview_r1 = select symbol, bid from quotes;",
                    "q_preview = select * from lj(q_preview, q_preview_r1, `symbol);",
                    "q_preview = select symbol, bid from q_preview;",
                ]
            ),
        )

    def test_right_join_preserves_right_input_by_reversing_left_join(self) -> None:
        result = right_join(self.trades, self.quotes, on=["venue", "symbol"])
        self.assertEqual(
            result.explain(),
            "\n".join(
                [
                    "q_preview_r0 = select * from quotes;",
                    "q_preview = select * from lj(q_preview_r0, trades, `venue`symbol);",
                ]
            ),
        )

    def test_asof_sorts_filtered_right_input_before_matching(self) -> None:
        quotes = where(self.quotes, col("bid") > 0)
        result = asof_join(self.trades, quotes, on="ts", by=["symbol"])
        self.assertEqual(
            result.explain(),
            "\n".join(
                [
                    "q_preview_r0 = select * from quotes where ((bid > 0));",
                    "q_preview_r0 = select * from q_preview_r0 order by symbol, ts;",
                    "q_preview = select * from aj(trades, q_preview_r0, `symbol`ts);",
                ]
            ),
        )

    def test_transformation_order_is_not_rearranged(self) -> None:
        self.assertEqual(
            self.trades.sort("ts").head(2).explain(),
            "\n".join(
                [
                    "q_preview = select * from trades order by ts;",
                    "q_preview = select top 2 * from q_preview;",
                ]
            ),
        )
        self.assertEqual(
            self.trades.head(2).sort("ts").explain(),
            "\n".join(
                [
                    "q_preview = select top 2 * from trades;",
                    "q_preview = select * from q_preview order by ts;",
                ]
            ),
        )

    def test_datetime_bucket_and_boolean_expressions(self) -> None:
        predicate = (col("ts") >= datetime(2026, 10, 27, 9, 49)) & (col("size") != 0)
        self.assertEqual(predicate.sql, "((ts >= 2026.10.27T09:49:00.000000000) and (size != 0))")
        self.assertEqual(
            col("ts").bucket("1m", label="right").alias("minute").sql,
            "(bar(nanotimestamp(ts), 1m) + 60000000000l) as minute",
        )
        with self.assertRaises(TypeError):
            bool(predicate)
        with self.assertRaises(ValueError):
            col("ts").bucket("0s")

    def test_invalid_joins_fail_without_execution(self) -> None:
        with self.assertRaises(ValueError):
            left_join(self.trades, Query(RecordingSession(), "quotes"), on="symbol")
        for keys in ([], ["symbol", "symbol"], ["symbol;drop"]):
            with self.subTest(keys=keys), self.assertRaises(ValueError):
                left_join(self.trades, self.quotes, on=keys)
        with self.assertRaises(ValueError):
            asof_join(self.trades, self.quotes, on="ts", by=["ts"])
        self.assertEqual(self.session.calls, [])

    def test_collect_and_cache_clean_up_nested_join_temporaries(self) -> None:
        right = left_join(self.quotes, Query(self.session, "venues"), on="venue")
        query = left_join(self.trades, right, on="symbol")
        self.assertEqual(query.collect()["volume"].to_list(), [30])
        generated_name = self.session.calls[0].split(" = ")[0].removesuffix("_r0_r0")
        self.assertEqual(
            self.session.calls[1],
            f"undef(`{generated_name}`{generated_name}_r0`{generated_name}_r0_r0, VAR)",
        )
        cached = query.cache("saved")
        self.assertEqual(cached.source, "saved")
        self.assertEqual(cached.steps, ())
        self.assertTrue(self.session.calls[2].endswith("; NULL"))
        pd.testing.assert_frame_equal(
            cached.collect(engine="pandas"), pd.DataFrame({"volume": [30]})
        )

    def test_query_error_is_preserved_and_cleanup_still_runs(self) -> None:
        session = RecordingSession(fail=True)
        query = left_join(Query(session, "trades"), Query(session, "quotes"), on="symbol")
        with self.assertRaisesRegex(RuntimeError, "query failed"):
            query.collect()
        self.assertEqual(len(session.calls), 2)
        self.assertTrue(session.calls[1].startswith("undef("))


if __name__ == "__main__":
    unittest.main()
