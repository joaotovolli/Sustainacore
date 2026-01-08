from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[2]
APP_PATH = REPO_ROOT / "app"
for path in (REPO_ROOT, APP_PATH):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from tools.oracle.env_bootstrap import load_env_files
from tools.oracle import preflight_oracle
from db_helper import get_connection
from index_engine.index_calc_v1 import compute_stats
from index_engine import db_index_calc as db


def _parse_date(value: str) -> dt.date:
    return dt.date.fromisoformat(value)


def _fetch_levels(start: dt.date, end: dt.date) -> dict[dt.date, float]:
    sql = (
        "SELECT trade_date, level_tr FROM SC_IDX_LEVELS "
        "WHERE index_code = :index_code AND trade_date BETWEEN :start_date AND :end_date"
    )
    levels: dict[dt.date, float] = {}
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, {"index_code": "TECH100", "start_date": start, "end_date": end})
        for trade_date, level in cur.fetchall():
            if isinstance(trade_date, dt.datetime):
                trade_date = trade_date.date()
            if isinstance(trade_date, dt.date):
                levels[trade_date] = float(level)
    return levels


def _fetch_weights(trading_days: list[dt.date]) -> dict[dt.date, dict[str, float]]:
    if not trading_days:
        return {}
    placeholders = ",".join(f":d{i}" for i in range(len(trading_days)))
    sql = (
        "SELECT trade_date, ticker, weight "
        "FROM SC_IDX_CONSTITUENT_DAILY "
        f"WHERE trade_date IN ({placeholders})"
    )
    params = {f"d{i}": d for i, d in enumerate(trading_days)}
    weights: dict[dt.date, dict[str, float]] = {}
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        for trade_date, ticker, weight in cur.fetchall():
            if isinstance(trade_date, dt.datetime):
                trade_date = trade_date.date()
            if not isinstance(trade_date, dt.date) or ticker is None:
                continue
            try:
                value = float(weight)
            except (TypeError, ValueError):
                continue
            weights.setdefault(trade_date, {})[str(ticker).strip().upper()] = value
    return weights


def _fetch_imputed_counts(start: dt.date, end: dt.date) -> dict[dt.date, int]:
    sql = (
        "SELECT trade_date, COUNT(*) "
        "FROM SC_IDX_CONSTITUENT_DAILY "
        "WHERE trade_date BETWEEN :start_date AND :end_date "
        "AND price_quality = 'IMPUTED' "
        "GROUP BY trade_date"
    )
    counts: dict[dt.date, int] = {}
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, {"start_date": start, "end_date": end})
        for trade_date, count in cur.fetchall():
            if isinstance(trade_date, dt.datetime):
                trade_date = trade_date.date()
            if isinstance(trade_date, dt.date):
                counts[trade_date] = int(count)
    return counts


def _compute_returns(trading_days: list[dt.date], levels: dict[dt.date, float]) -> dict[dt.date, float]:
    returns: dict[dt.date, float] = {}
    for idx in range(1, len(trading_days)):
        prev_date = trading_days[idx - 1]
        trade_date = trading_days[idx]
        prev_level = levels.get(prev_date)
        curr_level = levels.get(trade_date)
        if prev_level in (None, 0) or curr_level is None:
            continue
        returns[trade_date] = curr_level / prev_level - 1.0
    return returns


def main() -> int:
    parser = argparse.ArgumentParser(description="Recompute SC_IDX stats for a date range")
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    args = parser.parse_args()

    load_env_files()
    if preflight_oracle.main() != 0:
        return 1

    start_date = _parse_date(args.start)
    end_date = _parse_date(args.end)
    if end_date < start_date:
        raise SystemExit("end_before_start")

    trading_days = db.fetch_trading_days(start_date, end_date)
    if not trading_days:
        print("no_trading_days")
        return 1

    lookback = db.fetch_trading_days_before(trading_days[0], limit=25)
    full_days = lookback + trading_days

    levels = _fetch_levels(full_days[0], full_days[-1])
    weights_by_date = _fetch_weights(full_days)
    returns_1d = _compute_returns(full_days, levels)
    stats = compute_stats(
        trading_days=full_days,
        levels=levels,
        weights_by_date=weights_by_date,
        returns_1d=returns_1d,
    )
    imputed_counts = _fetch_imputed_counts(start_date, end_date)

    stats_rows = []
    for trade_date in trading_days:
        row = stats.get(trade_date, {})
        stats_rows.append(
            {
                "trade_date": trade_date,
                "level_tr": row.get("level_tr"),
                "ret_1d": row.get("ret_1d"),
                "ret_5d": row.get("ret_5d"),
                "ret_20d": row.get("ret_20d"),
                "vol_20d": row.get("vol_20d"),
                "max_drawdown_252d": None,
                "n_constituents": row.get("n_constituents"),
                "n_imputed": imputed_counts.get(trade_date, 0),
                "top5_weight": row.get("top5_weight"),
                "herfindahl": row.get("herfindahl"),
            }
        )

    db.upsert_stats_daily(stats_rows)
    print(f"stats_rows_written={len(stats_rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
