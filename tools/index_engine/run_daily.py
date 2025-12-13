"""Wrapper to run daily SC_IDX backfill up to the latest completed trading day."""
from __future__ import annotations

import argparse
import datetime as _dt
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.index_engine import ingest_prices
from index_engine.db import fetch_constituent_tickers

DEFAULT_START = _dt.date(2025, 1, 2)
CHUNK_SIZE = 2  # keep under 8 credits/minute
SLEEP_SECONDS = 15


def _parse_date(value: str) -> _dt.date:
    return _dt.date.fromisoformat(value)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run SC_IDX daily backfill (TwelveData)")
    parser.add_argument(
        "--start",
        help="start date (YYYY-MM-DD), default 2025-01-02",
    )
    parser.add_argument(
        "--end",
        help="end date (YYYY-MM-DD), default yesterday UTC",
    )
    parser.add_argument(
        "--tickers",
        help="comma-separated ticker override (optional)",
    )

    args = parser.parse_args(argv)

    start_date = _parse_date(args.start) if args.start else DEFAULT_START
    if args.end:
        end_date = _parse_date(args.end)
    else:
        end_date = _dt.datetime.utcnow().date() - _dt.timedelta(days=1)

    if end_date < start_date:
        print(f"Invalid date range: start={start_date} end={end_date}")
        return 1

    if args.tickers:
        tickers = ingest_prices._split_tickers(args.tickers)  # type: ignore[attr-defined]
    else:
        tickers = fetch_constituent_tickers(end_date)

    if not tickers:
        print("No tickers returned; aborting")
        return 1

    print(f"Running SC_IDX backfill from {start_date} to {end_date} for {len(tickers)} tickers")
    rc = 0
    for idx in range(0, len(tickers), CHUNK_SIZE):
        chunk = tickers[idx : idx + CHUNK_SIZE]
        print(f"  chunk {idx // CHUNK_SIZE + 1}: {', '.join(chunk)}")
        cli_args = [
            "--start",
            start_date.isoformat(),
            "--end",
            end_date.isoformat(),
            "--tickers",
            ",".join(chunk),
        ]
        result = ingest_prices.main(cli_args)
        rc = rc or result
        if idx + CHUNK_SIZE < len(tickers):
            time.sleep(SLEEP_SECONDS)
    return rc


if __name__ == "__main__":
    sys.exit(main())
