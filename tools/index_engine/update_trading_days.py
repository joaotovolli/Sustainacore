from __future__ import annotations

import argparse
import datetime as _dt
import importlib.util
import random
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
APP_ROOT = REPO_ROOT / "app"
if str(APP_ROOT) in sys.path:
    sys.path.remove(str(APP_ROOT))
import http.client  # preload stdlib http to avoid local http shadowing
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.oracle.env_bootstrap import load_env_files
from index_engine.db import fetch_latest_trading_day, upsert_trading_days
from db_helper import get_connection

BASE_DATE = _dt.date(2025, 1, 2)
SOURCE = "MARKET_DATA_SPY"
DEFAULT_WINDOW = 30
MAX_WINDOW = 365
DEFAULT_RETRY_ATTEMPTS = 3
DEFAULT_RETRY_BASE_SEC = 1.0


def _is_market_data_403(exc: Exception) -> bool:
    return "market_data_http_error:403" in str(exc)


def _sleep_backoff(attempt: int, *, base_sec: float = DEFAULT_RETRY_BASE_SEC, cap_sec: float = 30.0) -> None:
    delay = min(cap_sec, base_sec * (2 ** max(0, attempt - 1)))
    jitter = random.random() * 0.5
    time.sleep(delay + jitter)


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


def _fetch_total_count() -> int:
    sql = "SELECT COUNT(1) FROM SC_IDX_TRADING_DAYS"
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        if row and row[0] is not None:
            return int(row[0])
    return 0


def update_trading_days(
    start_date: _dt.date | None = None,
    *,
    auto_extend: bool = False,
) -> tuple[int, int, _dt.date, _dt.date | None, _dt.date | None]:
    load_env_files(
        paths=(
            "/etc/sustainacore/db.env",
            "/etc/sustainacore/index.env",
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

    values = []
    if hasattr(provider, "fetch_time_series"):
        values = provider.fetch_time_series("SPY", start, latest)
    else:
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
    if not dates and hasattr(provider, "fetch_daily_window_desc"):
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
    total_count = _fetch_total_count()
    return inserted, total_count, latest, max_before, max_after


def update_trading_days_with_retry(
    *,
    auto_extend: bool,
    max_attempts: int = DEFAULT_RETRY_ATTEMPTS,
    backoff_base_sec: float = DEFAULT_RETRY_BASE_SEC,
    allow_cached_on_403: bool = True,
) -> tuple[bool, str | None]:
    """Retry trading-day refresh and fall back to cached calendar on persistent 403."""

    for attempt in range(1, max_attempts + 1):
        try:
            update_trading_days(auto_extend=auto_extend)
            return True, None
        except Exception as exc:
            if _is_market_data_403(exc):
                print(
                    "update_trading_days_retry: attempt={attempt}/{total} error={error}".format(
                        attempt=attempt,
                        total=max_attempts,
                        error=exc,
                    ),
                    file=sys.stderr,
                )
                if attempt < max_attempts:
                    _sleep_backoff(attempt, base_sec=backoff_base_sec)
                    continue
                if allow_cached_on_403:
                    print(
                        "update_trading_days_fallback: cached_calendar reason=market_data_http_error:403",
                        file=sys.stderr,
                    )
                    return False, "market_data_http_error:403"
            raise
    return False, "update_trading_days_failed"


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
