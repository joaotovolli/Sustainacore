"""CLI helper for deterministic SC_IDX price backfills."""
from __future__ import annotations

import argparse
import datetime as _dt
import http.client  # preload stdlib http to avoid local http shadowing
import pathlib
import sys

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
APP_ROOT = REPO_ROOT / "app"
for path in (REPO_ROOT, APP_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from tools.oracle.env_bootstrap import load_env_files

from tools.index_engine import ingest_prices


def _parse_date(value: str | None) -> _dt.date | None:
    if not value:
        return None
    return _dt.date.fromisoformat(value)


def _resolve_range(args: argparse.Namespace) -> tuple[_dt.date, _dt.date]:
    if args.date:
        single = _parse_date(args.date)
        if single is None:
            raise ValueError("invalid --date")
        return single, single
    start = _parse_date(args.start)
    end = _parse_date(args.end)
    if start is None or end is None:
        raise ValueError("Provide --date or both --start and --end")
    if end < start:
        raise ValueError("end date must be on or after start date")
    return start, end


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Backfill SC_IDX prices (single day or range).")
    parser.add_argument("--date", help="Single trade date (YYYY-MM-DD)")
    parser.add_argument("--start", help="Start date inclusive (YYYY-MM-DD)")
    parser.add_argument("--end", help="End date inclusive (YYYY-MM-DD)")
    parser.add_argument("--tickers", help="Comma-separated ticker list override")
    parser.add_argument("--missing-only", action="store_true", help="Only fetch missing ticker/dates")
    parser.add_argument("--debug", action="store_true", help="Print ingest diagnostics")
    parser.add_argument(
        "--max-provider-calls",
        type=int,
        default=None,
        help="Maximum provider requests to issue in backfill mode.",
    )
    parser.add_argument(
        "--provider",
        default=ingest_prices.PROVIDER,
        help="Provider key (default MARKET_DATA).",
    )
    args = parser.parse_args(argv)

    if args.provider != ingest_prices.PROVIDER:
        print(f"Unsupported provider: {args.provider}")
        return 2

    try:
        start_date, end_date = _resolve_range(args)
    except ValueError as exc:
        print(f"Invalid date arguments: {exc}")
        return 1

    load_env_files(
        paths=(
            "/etc/sustainacore/db.env",
            "/etc/sustainacore-ai/app.env",
            "/etc/sustainacore-ai/secrets.env",
        )
    )

    ingest_args = argparse.Namespace(
        date=None,
        start=start_date.isoformat(),
        end=end_date.isoformat(),
        backfill=not args.missing_only,
        backfill_missing=args.missing_only,
        tickers=args.tickers,
        debug=args.debug,
        max_provider_calls=args.max_provider_calls,
    )

    if args.missing_only:
        code, _summary = ingest_prices._run_backfill_missing(ingest_args)
    else:
        code, _summary = ingest_prices._run_backfill(ingest_args)
    return code


if __name__ == "__main__":
    sys.exit(main())
