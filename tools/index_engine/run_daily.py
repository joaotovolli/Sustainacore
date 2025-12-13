import datetime as _dt
import importlib.util
import os
import sys
from pathlib import Path


DEFAULT_START = _dt.date(2025, 1, 2)
DEFAULT_BUFFER = 25
BUFFER_ENV = "SC_IDX_TWELVEDATA_CREDIT_BUFFER"
PROBE_SYMBOL_ENV = "SC_IDX_PROBE_SYMBOL"


def _load_provider_module():
    module_path = Path(__file__).resolve().parents[2] / "app" / "providers" / "twelvedata.py"
    spec = importlib.util.spec_from_file_location("twelvedata_provider", module_path)
    if spec is None or spec.loader is None:
        raise ImportError("Unable to load Twelve Data provider module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[arg-type]
    return module


def _load_ingest_module():
    module_path = Path(__file__).resolve().parent / "ingest_prices.py"
    spec = importlib.util.spec_from_file_location("index_engine_ingest", module_path)
    if spec is None or spec.loader is None:
        raise ImportError("Unable to load ingest_prices module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[arg-type]
    return module


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _select_end_date(probe_symbol: str, today_utc: _dt.date) -> _dt.date:
def _select_end_date(provider, probe_symbol: str, today_utc: _dt.date) -> _dt.date:
    try:
        if provider.has_eod_for_date(probe_symbol, today_utc):
            return today_utc
    except Exception as exc:
        print(
            f"warning: falling back to yesterday; could not probe {probe_symbol}: {exc}",
            file=sys.stderr,
        )
    return today_utc - _dt.timedelta(days=1)


def main() -> int:
    provider = _load_provider_module()
    ingest_module = _load_ingest_module()
    try:
        usage = provider.fetch_api_usage()
    except Exception as exc:
        print(f"error fetching Twelve Data usage: {exc}", file=sys.stderr)
        return 1

    remaining = provider.remaining_credits(usage)
    buffer = _env_int(BUFFER_ENV, DEFAULT_BUFFER)
    max_provider_calls = max(0, remaining - buffer - 1)

    today_utc = _dt.datetime.now(_dt.timezone.utc).date()
    probe_symbol = os.getenv(PROBE_SYMBOL_ENV, "AAPL")
    end_date = _select_end_date(provider, probe_symbol, today_utc)

    print(
        f"index_engine_daily: end={end_date.isoformat()} remaining={remaining} "
        f"buffer={buffer} max_provider_calls={max_provider_calls}"
    )

    if max_provider_calls <= 0:
        print("budget_stop: provider_calls_used=0 max_provider_calls=0")
        return 0

    ingest_args = [
        "--backfill",
        "--start",
        DEFAULT_START.isoformat(),
        "--end",
        end_date.isoformat(),
        "--max-provider-calls",
        str(max_provider_calls),
    ]

    tickers_env = os.getenv("SC_IDX_TICKERS")
    if tickers_env:
        ingest_args.extend(["--tickers", tickers_env])

    return ingest_module.main(ingest_args)
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
