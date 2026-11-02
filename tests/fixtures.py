"""Normalize raw SSE/SZSE tick CSVs into the tables the market-data tests use.

Raw rows (no header), one of four record types:

    SseTrade,  recv, HMMSSff,           symbol, side(B/S/N), price, qty
    SzseTrade, recv, YYYYMMDDHHMMSSmmm, symbol, F,           price, qty
    SseDepth,  recv, HMMSS,             symbol, phase, bid1, bid1_qty, ask1, ask1_qty
    SzseDepth, recv, YYYYMMDDHHMMSSmmm, symbol, phase, bid1*100, qty, ask1*100, qty

Every trap found in the raw data is handled here, once, so tests and
researchers never see it:

* SSE exchange times have no date and no leading zero before 10:00
  ("9250000" vs "14565591"); the date comes from the receive time.
* SSE depth exchange time is whole seconds. A snapshot stamped 09:30:05 may
  have been published at 09:30:05.9, so it is only *available* from 09:30:06.
* SZSE depth prices are scaled by 100; trade prices are not.
* Exchange phase codes are not reliable session labels: SSE depth reports
  continuous trading (T111) through the lunch break and 09:25-09:30. Sessions
  are derived from the exchange clock instead.
* Auction prints are stamped exactly 09:25:00.000 and 15:00:00.000.

Usage:
    uv run python tests/fixtures.py --out tests/data RAW.csv [RAW.csv ...]
"""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from datetime import time
from pathlib import Path

import polars as pl

RAW_COLUMNS = [f"c{i}" for i in range(9)]


def session(clock: pl.Expr) -> pl.Expr:
    """Trading session from the exchange clock time (Asia/Shanghai)."""
    t = clock.dt.time()
    return (
        pl.when(t == time(9, 25))
        .then(pl.lit("open_auction_print"))
        .when(t == time(15, 0))
        .then(pl.lit("close_auction_print"))
        .when(t < time(9, 15))
        .then(pl.lit("pre_open"))
        .when(t < time(9, 25))
        .then(pl.lit("open_auction"))
        .when(t < time(9, 30))
        .then(pl.lit("pause"))
        .when(t < time(11, 30))
        .then(pl.lit("continuous"))
        .when(t < time(13, 0))
        .then(pl.lit("lunch"))
        .when(t < time(14, 57))
        .then(pl.lit("continuous"))
        .when(t < time(15, 0))
        .then(pl.lit("close_auction"))
        .otherwise(pl.lit("closed"))
    )


def read_raw(paths: Sequence[Path]) -> pl.DataFrame:
    frames = [
        pl.read_csv(
            path,
            has_header=False,
            new_columns=RAW_COLUMNS,
            schema_overrides=dict.fromkeys(RAW_COLUMNS, pl.String),
            truncate_ragged_lines=True,
        )
        for path in paths
    ]
    # seq = position in the feed, the only tie-breaker the data offers.
    return (
        pl.concat(frames)
        .with_row_index("seq")
        .with_columns(
            pl.col("seq").cast(pl.Int64),
            pl.col("c1").str.to_datetime("%Y-%m-%d %H:%M:%S%.f", time_unit="ns").alias("recv_ts"),
        )
    )


def exchange_time(record: str) -> pl.Expr:
    raw = pl.col("c2")
    date = pl.col("recv_ts").dt.strftime("%Y%m%d")
    if record.startswith("Szse"):
        text = raw
    elif record == "SseTrade":
        text = date + raw.str.zfill(8) + pl.lit("0")  # HHMMSSff -> HHMMSSmmm
    else:
        text = date + raw.str.zfill(6) + pl.lit("000")  # HHMMSS -> HHMMSSmmm
    return text.str.to_datetime("%Y%m%d%H%M%S%3f", time_unit="ns")


def normalize(paths: Sequence[Path]) -> tuple[pl.DataFrame, pl.DataFrame]:
    raw = read_raw(paths)
    trade_parts, depth_parts = [], []
    for record in ("SseTrade", "SzseTrade", "SseDepth", "SzseDepth"):
        part = raw.filter(pl.col("c0") == record).with_columns(
            exchange_time(record).alias("exch_ts")
        )
        venue = pl.lit("SSE" if record.startswith("Sse") else "SZSE").alias("venue")
        if record.endswith("Trade"):
            trade_parts.append(
                part.select(
                    venue,
                    pl.col("c3").alias("symbol"),
                    "exch_ts",
                    "recv_ts",
                    "seq",
                    pl.col("c4").alias("side"),
                    pl.col("c5").cast(pl.Float64).alias("price"),
                    pl.col("c6").cast(pl.Int64).alias("qty"),
                    session(pl.col("exch_ts")).alias("session"),
                )
            )
        else:
            scale = 100.0 if record == "SzseDepth" else 1.0
            delay = "1s" if record == "SseDepth" else "0s"
            depth_parts.append(
                part.select(
                    venue,
                    pl.col("c3").alias("symbol"),
                    "exch_ts",
                    pl.col("exch_ts").dt.offset_by(delay).alias("avail_ts"),
                    "recv_ts",
                    "seq",
                    pl.col("c4").str.strip_chars().alias("phase_code"),
                    (pl.col("c5").cast(pl.Float64) / scale).alias("bid1"),
                    pl.col("c6").cast(pl.Int64).alias("bid1_qty"),
                    (pl.col("c7").cast(pl.Float64) / scale).alias("ask1"),
                    pl.col("c8").cast(pl.Int64).alias("ask1_qty"),
                    session(pl.col("exch_ts")).alias("session"),
                )
            )
    return pl.concat(trade_parts).sort("seq"), pl.concat(depth_parts).sort("seq")


def main() -> None:
    parser = argparse.ArgumentParser(description="Normalize raw SSE/SZSE tick CSVs.")
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("raw", type=Path, nargs="+")
    args = parser.parse_args()
    trades, depth = normalize(args.raw)
    args.out.mkdir(parents=True, exist_ok=True)
    trades.write_parquet(args.out / "trades.parquet")
    depth.write_parquet(args.out / "depth.parquet")
    print(f"{trades.height} trades, {depth.height} depth rows -> {args.out}")


if __name__ == "__main__":
    main()
