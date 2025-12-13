"""Wrapper to run daily SC_IDX backfill with Twelve Data budgeting."""
from __future__ import annotations

import argparse
import datetime as _dt
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.index_engine import ingest_prices
from index_engine.db import fetch_constituent_tickers
from providers import twelvedata

DEFAULT_START = _dt.date(2025, 1, 2)
DEFAULT_BUFFER = 25
BUFFER_ENV = "SC_IDX_TWELVEDATA_CREDIT_BUFFER"
PROBE_SYMBOL_ENV = "SC_IDX_PROBE_SYMBOL"


def _parse_date(value: str) -> _dt.date:
    return _dt.date.fromisoformat(value)


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _select_end_date(probe_symbol: str, today_utc: _dt.date) -> _dt.date:
    try:
        if twelvedata.has_eod_for_date(probe_symbol, today_utc):
            return today_utc
    except Exception as exc:
        print(
            f"warning: falling back to yesterday; could not probe {probe_symbol}: {exc}",
            file=sys.stderr,
        )
    return today_utc - _dt.timedelta(days=1)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run SC_IDX daily backfill (TwelveData)")
    parser.add_argument("--start", help="start date (YYYY-MM-DD), default 2025-01-02")
    parser.add_argument("--end", help="end date (YYYY-MM-DD); default probes today vs yesterday")
    parser.add_argument("--tickers", help="comma-separated ticker override (optional)")

    args = parser.parse_args(argv)

    try:
        usage = twelvedata.fetch_api_usage()
    except Exception as exc:
        print(f"error fetching Twelve Data usage: {exc}", file=sys.stderr)
        return 1

    remaining = twelvedata.remaining_credits(usage)
    buffer = _env_int(BUFFER_ENV, DEFAULT_BUFFER)
    max_provider_calls = max(0, remaining - buffer - 1)

    today_utc = _dt.datetime.now(_dt.timezone.utc).date()
    end_date = _parse_date(args.end) if args.end else _select_end_date(os.getenv(PROBE_SYMBOL_ENV, "AAPL"), today_utc)
    start_date = _parse_date(args.start) if args.start else DEFAULT_START

    print(
        f"index_engine_daily: end={end_date.isoformat()} remaining={remaining} "
        f"buffer={buffer} max_provider_calls={max_provider_calls}"
    )

    if max_provider_calls <= 0:
        print("budget_stop: provider_calls_used=0 max_provider_calls=0")
        return 0

    if start_date > end_date:
        print(f"Invalid date range: start={start_date} end={end_date}")
        return 1

    if args.tickers:
        tickers = ingest_prices._split_tickers(args.tickers)  # type: ignore[attr-defined]
    else:
        tickers = fetch_constituent_tickers(end_date)

    if not tickers:
        print("No tickers returned; aborting")
        return 1

    ingest_args = [
        "--backfill",
        "--start",
        start_date.isoformat(),
        "--end",
        end_date.isoformat(),
        "--tickers",
        ",".join(tickers),
        "--max-provider-calls",
        str(max_provider_calls),
    ]

    return ingest_prices.main(ingest_args)


if __name__ == "__main__":
    sys.exit(main())

