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
from db_helper import get_connection


def _load_provider_module():
    module_path = REPO_ROOT / "app" / "providers" / "market_data_provider.py"
    spec = importlib.util.spec_from_file_location("market_data_provider", module_path)
    if spec is None or spec.loader is None:
        raise ImportError("Unable to load market data provider provider module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[arg-type]
    return module


def _load_run_daily_module():
    module_path = REPO_ROOT / "tools" / "index_engine" / "run_daily.py"
    spec = importlib.util.spec_from_file_location("index_engine_run_daily", module_path)
    if spec is None or spec.loader is None:
        raise ImportError("Unable to load run_daily module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[arg-type]
    return module


def _oracle_max_dates() -> dict[str, _dt.date | None]:
    sql_overall = "SELECT MAX(trade_date) FROM SC_IDX_PRICES_CANON"
    sql_real = "SELECT MAX(trade_date) FROM SC_IDX_PRICES_CANON WHERE quality != 'IMPUTED'"
    sql_trading_days = "SELECT MAX(trade_date) FROM SC_IDX_TRADING_DAYS"
    sql_impacted = (
        "SELECT MAX(c.trade_date) "
        "FROM SC_IDX_PRICES_CANON c "
        "WHERE EXISTS ("
        "  SELECT 1 FROM tech11_ai_gov_eth_index t "
        "  WHERE t.ticker = c.ticker "
        "  AND t.port_weight > 0 "
        "  AND t.port_date = ("
        "    SELECT MAX(port_date) "
        "    FROM tech11_ai_gov_eth_index "
        "    WHERE port_date <= c.trade_date"
        "  ) "
        "  AND t.rank_index <= 25"
        ")"
    )

    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql_overall)
        overall = cur.fetchone()[0]
        cur.execute(sql_real)
        real = cur.fetchone()[0]
        cur.execute(sql_impacted)
        impacted = cur.fetchone()[0]
        cur.execute(sql_trading_days)
        trading_days = cur.fetchone()[0]

    def _to_date(value):
        if value is None:
            return None
        if isinstance(value, _dt.datetime):
            return value.date()
        return value

    return {
        "overall": _to_date(overall),
        "real": _to_date(real),
        "impacted": _to_date(impacted),
        "trading_days": _to_date(trading_days),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Debug latest EOD vs Oracle.")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    load_default_env()
    provider = _load_provider_module()
    run_daily = _load_run_daily_module()

    spy_latest = provider.fetch_latest_eod_date("SPY")
    aapl_latest = provider.fetch_latest_eod_date("AAPL")

    oracle_dates = _oracle_max_dates()
    trading_days = run_daily.fetch_trading_days(run_daily.DEFAULT_START, spy_latest)
    effective_end = run_daily.select_effective_end_date(spy_latest, trading_days)

    print("== Provider latest EOD ==")
    print(f"SPY latest_eod_date={spy_latest.isoformat()}")
    print(f"AAPL latest_eod_date={aapl_latest.isoformat()}")

    print("\n== Oracle latest canon ==")
    print(f"canon_max_trade_date={oracle_dates['overall']}")
    print(f"canon_max_trade_date_real={oracle_dates['real']}")
    print(f"canon_max_trade_date_impacted={oracle_dates['impacted']}")

    print("\n== Trading-days max ==")
    print(f"trading_days_max_trade_date={oracle_dates['trading_days']}")

    print("\n== run_daily effective end date ==")
    print(f"effective_end_date={effective_end}")

    if effective_end and oracle_dates["overall"]:
        if oracle_dates["trading_days"] and oracle_dates["trading_days"] < spy_latest:
            hint = "Trading-day calendar is behind provider latest; run update_trading_days --auto."
        elif effective_end > oracle_dates["overall"]:
            hint = "Provider latest ahead of Oracle canon; ingest should catch up."
        elif effective_end < oracle_dates["overall"]:
            hint = "Oracle canon ahead of effective_end; check trading day calendar."
        else:
            hint = "Provider and Oracle canon aligned."
    else:
        hint = "Missing data to compare provider vs Oracle."

    print("\n== Diagnosis hint ==")
    print(hint)

    if args.debug:
        print(f"trading_days_count={len(trading_days)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
