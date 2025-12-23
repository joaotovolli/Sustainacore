from __future__ import annotations

import argparse
import datetime as _dt
import importlib.util
import sys
from pathlib import Path

import http.client  # preload stdlib http to avoid app.http shadowing

REPO_ROOT = Path(__file__).resolve().parents[2]
APP_ROOT = REPO_ROOT / "app"
for path in (REPO_ROOT, APP_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from tools.oracle.env_bootstrap import load_env_files
from index_engine.db import fetch_latest_trading_day, upsert_trading_days

BASE_DATE = _dt.date(2025, 1, 2)
SOURCE = "MARKET_DATA_SPY"
DEFAULT_WINDOW = 30
MAX_WINDOW = 365


def _load_provider_module():
    module_path = Path(__file__).resolve().parents[2] / "app" / "providers" / "market_data_provider.py"
    spec = importlib.util.spec_from_file_location("market_data_provider", module_path)
    if spec is None or spec.loader is None:
        raise ImportError("Unable to load market data provider module")
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


def update_trading_days(
    start_date: _dt.date | None = None,
    *,
    auto_extend: bool = False,
) -> tuple[int, int, _dt.date, _dt.date | None, _dt.date | None]:
    load_env_files(
        paths=(
            "/etc/sustainacore/db.env",
            "/etc/sustainacore-ai/app.env",
            "/etc/sustainacore-ai/secrets.env",
        )
    )
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
        inserted = 0
        total = max_before or latest
        return inserted, 0, latest, max_before, total

    window = max(DEFAULT_WINDOW, (latest - start).days + 5)
    window = min(window, MAX_WINDOW)
    values = provider.fetch_daily_window_desc("SPY", window=window)
    dates = sorted(
        {
            trade_date
            for entry in values
            if (trade_date := _extract_trade_date(entry)) is not None
            and start <= trade_date <= latest
        }
    )
    if not dates and start == latest:
        dates = [latest]
    if not dates:
        inserted = 0
        return inserted, 0, latest, max_before, max_before

    inserted = upsert_trading_days(dates, SOURCE)
    max_after = fetch_latest_trading_day()
    return inserted, len(dates), latest, max_before, max_after


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
