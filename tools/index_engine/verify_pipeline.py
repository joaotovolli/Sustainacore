"""Verification helper for the SC_IDX ingest pipeline."""
from __future__ import annotations

import datetime as _dt
import pathlib
import sys
from typing import Any, Dict, List

ROOT_PATH = pathlib.Path(__file__).resolve().parents[2]
APP_PATH = ROOT_PATH / "app"
for path in (ROOT_PATH, APP_PATH):
    if str(path) not in sys.path:
        sys.path.append(str(path))

from tools.index_engine.env_loader import load_default_env

load_default_env()

from db_helper import get_connection
from providers.market_data_provider import (
    MarketDataProviderError,
    fetch_api_usage,
    fetch_latest_bar,
)


def _fetch_latest_bar_for_symbol(symbol: str) -> tuple[list[dict], str | None, str | None]:
    try:
        rows = fetch_latest_bar(symbol)
    except Exception as exc:  # pragma: no cover - network / env driven
        return [], None, f"provider_error: {exc}"
    if not rows:
        return [], None, "provider_empty: no rows returned"

    first = rows[0] if isinstance(rows[0], dict) else {}
    latest_dt = str(first.get("datetime") or first.get("date") or first.get("trade_date") or "").strip() or None
    return rows, latest_dt, None


def _query_oracle() -> Dict[str, Any]:
    with get_connection() as conn:
        cur = conn.cursor()

        cur.execute("SELECT USER FROM dual")
        user_row = cur.fetchone()
        oracle_user = str(user_row[0]).strip() if user_row and user_row[0] is not None else None

        cur.execute(
            "SELECT MAX(ingested_at) FROM SC_IDX_PRICES_RAW WHERE provider = 'MARKET_DATA'"
        )
        ingested_row = cur.fetchone()
        max_ingested = ingested_row[0] if ingested_row else None

        cur.execute(
            "SELECT MAX(trade_date) FROM SC_IDX_PRICES_RAW "
            "WHERE status = 'OK' AND provider = 'MARKET_DATA'"
        )
        max_ok_row = cur.fetchone()
        max_ok_trade_date = max_ok_row[0] if max_ok_row else None

        cur.execute(
            "SELECT trade_date, status, COUNT(*) "
            "FROM SC_IDX_PRICES_RAW "
            "WHERE trade_date >= TRUNC(SYSDATE) - 3 "
            "AND provider = 'MARKET_DATA' "
            "GROUP BY trade_date, status "
            "ORDER BY trade_date DESC, status"
        )
        daily_counts = [
            {"trade_date": row[0], "status": row[1], "count": row[2]}
            for row in cur.fetchall()
        ]

        cur.execute("SELECT COUNT(*) FROM SC_IDX_PRICES_RAW WHERE provider = 'MARKET_DATA'")
        raw_count = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM SC_IDX_PRICES_CANON")
        canon_count = cur.fetchone()[0]

    return {
        "oracle_user": oracle_user,
        "max_ingested_at": max_ingested,
        "max_ok_trade_date": max_ok_trade_date,
        "daily_counts": daily_counts,
        "raw_count": raw_count,
        "canon_count": canon_count,
    }


def _coerce_datetime(value: Any) -> _dt.datetime | None:
    if value is None:
        return None
    if isinstance(value, _dt.datetime):
        return value
    if isinstance(value, _dt.date):
        return _dt.datetime.combine(value, _dt.time.min)
    return None


def _print_usage(label: str, usage: Dict[str, Any]) -> None:
    current = usage.get("current_usage")
    limit = usage.get("plan_limit")
    category = usage.get("plan_category")
    ts = usage.get("timestamp")
    print(f"{label}: current_usage={current} plan_limit={limit} plan_category={category} as_of={ts}")


def _load_env_files() -> None:
    load_default_env()


def main() -> int:
    _load_env_files()
    print("== Market data provider usage check ==")
    try:
        usage_before = fetch_api_usage()
    except MarketDataProviderError as exc:
        print(f"FAIL unable to fetch provider usage before probe: {exc}")
        return 1
    _print_usage("usage_before", usage_before)

    provider_rows, latest_dt, provider_error = _fetch_latest_bar_for_symbol("AAPL")

    try:
        usage_after = fetch_api_usage()
    except MarketDataProviderError as exc:
        print(f"FAIL unable to fetch provider usage after probe: {exc}")
        return 1
    _print_usage("usage_after", usage_after)

    print(
        "provider_call: symbol=AAPL provider_rows={rows} latest_datetime={latest_dt} provider_error={error}".format(
            rows=len(provider_rows),
            latest_dt=latest_dt,
            error=provider_error,
        )
    )

    before_value = usage_before.get("current_usage") or 0
    after_value = usage_after.get("current_usage") or 0
    delta = after_value - before_value

    oracle_payload = _query_oracle()
    max_ingested_at = _coerce_datetime(oracle_payload.get("max_ingested_at"))
    max_ok_trade_date = oracle_payload.get("max_ok_trade_date")

    print("\n== Oracle ==")
    print(f"oracle_user={oracle_payload.get('oracle_user')}")
    print(f"max_ingested_at={max_ingested_at}")
    print(f"max_ok_trade_date={max_ok_trade_date}")
    print(f"raw_count={oracle_payload.get('raw_count')} canon_count={oracle_payload.get('canon_count')}")

    print("\n== Delta ==")
    print(f"usage_delta={delta}")

    overall_pass = provider_error is None and len(provider_rows) > 0
    print("\n== Overall ==")
    print("OVERALL: PASS" if overall_pass else "OVERALL: FAIL")

    return 0 if overall_pass else 2


if __name__ == "__main__":
    sys.exit(main())
