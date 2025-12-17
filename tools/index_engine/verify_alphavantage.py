"""Verify Alpha Vantage connectivity + Oracle ingest path (no deps)."""
from __future__ import annotations

import datetime as _dt
import http.client  # preload stdlib http to avoid app.http shadowing
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.index_engine.env_loader import load_default_env

load_default_env()

APP_PATH = REPO_ROOT / "app"
if str(APP_PATH) not in sys.path:
    sys.path.insert(0, str(APP_PATH))

from db_helper import get_connection
from index_engine.db import upsert_prices_raw
from providers.alphavantage import fetch_daily_adjusted
from tools.index_engine.oracle_preflight import (
    collect_wallet_diagnostics,
    format_wallet_diagnostics,
    probe_oracle_user,
)


def _oracle_preflight_or_exit() -> str:
    try:
        user = probe_oracle_user()
        if user:
            return user
        return "UNKNOWN"
    except Exception as exc:
        print("oracle_preflight_failed:", str(exc), file=sys.stderr)
        diagnostics = collect_wallet_diagnostics()
        print(format_wallet_diagnostics(diagnostics), file=sys.stderr)
        raise SystemExit(2)


def main() -> int:
    oracle_user = _oracle_preflight_or_exit()
    print(f"oracle_user={oracle_user}")

    rows = fetch_daily_adjusted("AAPL", outputsize="compact")
    if not rows:
        print("FAIL: Alpha Vantage returned no rows for AAPL")
        return 1

    max_date = max(row.get("trade_date") for row in rows if row.get("trade_date"))
    points = len(rows)
    print(f"alphavantage: symbol=AAPL points={points} max_date={max_date}")

    latest = None
    for row in rows:
        if row.get("trade_date") == max_date:
            latest = row
            break
    if not latest:
        print("FAIL: unable to locate max_date row in payload")
        return 1

    trade_date = _dt.date.fromisoformat(str(max_date))
    raw_row = {
        "ticker": "AAPL",
        "trade_date": trade_date,
        "provider": "ALPHAVANTAGE",
        "close_px": latest.get("close"),
        "adj_close_px": latest.get("adj_close") if latest.get("adj_close") is not None else latest.get("close"),
        "volume": latest.get("volume"),
        "currency": None,
        "status": "OK",
        "error_msg": None,
    }
    upsert_prices_raw([raw_row])

    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT status, close_px, adj_close_px FROM SC_IDX_PRICES_RAW "
            "WHERE ticker = :ticker AND trade_date = :trade_date AND provider = :provider",
            {"ticker": "AAPL", "trade_date": trade_date, "provider": "ALPHAVANTAGE"},
        )
        fetched = cur.fetchone()

    if not fetched:
        print("FAIL: unable to read back SC_IDX_PRICES_RAW row")
        return 1

    status, close_px, adj_close_px = fetched
    if status != "OK":
        print(f"FAIL: readback status={status}")
        return 1

    print(f"PASS: raw_upsert_and_readback status={status} close_px={close_px} adj_close_px={adj_close_px}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
