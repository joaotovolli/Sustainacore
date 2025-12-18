from __future__ import annotations

import argparse
import datetime as _dt
import importlib.util
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
APP_ROOT = REPO_ROOT / "app"
for path in (REPO_ROOT, APP_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from tools.index_engine.env_loader import load_default_env

from index_engine.db import fetch_latest_trading_day, upsert_trading_days
from db_helper import get_connection

BASE_DATE = _dt.date(2025, 1, 2)
SOURCE = "TWELVEDATA_SPY"


def _load_provider_module():
    module_path = Path(__file__).resolve().parents[2] / "app" / "providers" / "twelvedata.py"
    spec = importlib.util.spec_from_file_location("twelvedata_provider", module_path)
    if spec is None or spec.loader is None:
        raise ImportError("Unable to load Twelve Data provider module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[arg-type]
    return module


def _extract_trade_date(entry: dict) -> _dt.date | None:
    for key in ("trade_date", "datetime", "date"):
        raw = entry.get(key)
        if raw:
            text = str(raw)
            try:
                return _dt.date.fromisoformat(text[:10])
            except ValueError:
                continue
    return None


def _fetch_total_count() -> int:
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM SC_IDX_TRADING_DAYS")
        row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else 0


def update_trading_days(
    start_date: _dt.date | None = None,
    *,
    auto_extend: bool = False,
) -> tuple[int, int, _dt.date, _dt.date | None, _dt.date | None]:
    load_default_env()
    provider = _load_provider_module()
    latest = provider.fetch_latest_eod_date("SPY")
    if latest < BASE_DATE:
        raise RuntimeError("latest_eod_before_base_date")

    max_before = fetch_latest_trading_day()
    if auto_extend:
        if max_before:
            start = max(max_before + _dt.timedelta(days=1), BASE_DATE)
        else:
            start = BASE_DATE
    else:
        start = start_date or BASE_DATE
    if start < BASE_DATE:
        start = BASE_DATE
    if latest < start:
        if auto_extend:
            inserted = 0
            total = _fetch_total_count()
            max_after = fetch_latest_trading_day()
            return inserted, total, latest, max_before, max_after
        raise RuntimeError("latest_eod_before_start_date")

    values = provider.fetch_time_series("SPY", start, latest)
    dates = sorted(
        {
            trade_date
            for entry in values
            if (trade_date := _extract_trade_date(entry)) is not None
        }
    )
    if not dates and auto_extend and start == latest:
        # Provider reported latest EOD but time_series returned empty for that day.
        dates = [latest]
    if not dates:
        if auto_extend:
            inserted = 0
            total = _fetch_total_count()
            max_after = fetch_latest_trading_day()
            return inserted, total, latest, max_before, max_after
        raise RuntimeError("no_trading_days_returned")

    inserted = upsert_trading_days(dates, SOURCE)
    total = _fetch_total_count()
    max_after = fetch_latest_trading_day()
    return inserted, total, latest, max_before, max_after


def main() -> int:
    parser = argparse.ArgumentParser(description="Update SC_IDX trading days calendar.")
    parser.add_argument("--start", help="Start date YYYY-MM-DD")
    parser.add_argument("--auto", action="store_true", help="Auto-extend from current max")
    parser.add_argument("--debug", action="store_true", help="Print debug context")
    args = parser.parse_args()

    start_date = _dt.date.fromisoformat(args.start) if args.start else None
    inserted, total, latest, max_before, max_after = update_trading_days(
        start_date,
        auto_extend=args.auto,
    )
    if args.debug:
        print(f"trading_days_max_before={max_before}")
        print(f"trading_days_max_after={max_after}")
    print(f"latest_eod_date_spy={latest.isoformat()}")
    print(f"inserted_count={inserted} total_count={total}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
