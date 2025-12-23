from __future__ import annotations

import argparse
import datetime as _dt
import http.client  # preload stdlib http to avoid app.http shadowing
import sys
from pathlib import Path
from typing import Dict, List

REPO_ROOT = Path(__file__).resolve().parents[2]
APP_ROOT = REPO_ROOT / "app"
for path in (REPO_ROOT, APP_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from tools.index_engine.run_lock import run_lock
from tools.oracle.env_bootstrap import load_env_files

from index_engine.index_calc_v1 import compute_contributions, compute_constituent_daily, compute_stats
from index_engine import db_index_calc as db
from db_helper import get_connection

BASE_DATE = _dt.date(2025, 1, 2)


def _parse_date(value: str) -> _dt.date:
    text = value.strip().lower()
    if text == "today":
        return _dt.date.today()
    return _dt.date.fromisoformat(text)


def _fetch_holdings(end_date: _dt.date) -> dict[_dt.date, dict[str, float]]:
    sql = (
        "SELECT rebalance_date, ticker, shares "
        "FROM SC_IDX_HOLDINGS "
        "WHERE index_code = :index_code "
        "AND rebalance_date <= :end_date "
        "ORDER BY rebalance_date"
    )
    holdings: dict[_dt.date, dict[str, float]] = {}
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, {"index_code": db.INDEX_CODE, "end_date": end_date})
        for rebalance_date, ticker, shares in cur.fetchall():
            if isinstance(rebalance_date, _dt.datetime):
                rebalance_date = rebalance_date.date()
            if rebalance_date is None or ticker is None or shares is None:
                continue
            holdings.setdefault(rebalance_date, {})[str(ticker).strip().upper()] = float(shares)
    return holdings


def _fetch_divisors(end_date: _dt.date) -> dict[_dt.date, float]:
    sql = (
        "SELECT effective_date, divisor "
        "FROM SC_IDX_DIVISOR "
        "WHERE index_code = :index_code "
        "AND effective_date <= :end_date "
        "ORDER BY effective_date"
    )
    divisors: dict[_dt.date, float] = {}
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, {"index_code": db.INDEX_CODE, "end_date": end_date})
        for eff_date, divisor in cur.fetchall():
            if isinstance(eff_date, _dt.datetime):
                eff_date = eff_date.date()
            if eff_date is None or divisor is None:
                continue
            divisors[eff_date] = float(divisor)
    return divisors


def _latest_key_before(keys: List[_dt.date], target: _dt.date) -> _dt.date | None:
    latest = None
    for key in keys:
        if key <= target:
            latest = key
        else:
            break
    return latest


def _collect_prices(
    trade_date: _dt.date,
    tickers: list[str],
    *,
    allow_close: bool,
) -> tuple[dict[str, float], dict[str, str | None]]:
    prices = db.fetch_prices(trade_date, tickers, allow_close=allow_close)
    price_map: dict[str, float] = {}
    quality_map: dict[str, str | None] = {}
    for ticker, payload in prices.items():
        price = payload.get("price")
        if price is None:
            continue
        price_map[ticker] = float(price)
        quality_map[ticker] = payload.get("quality")
    return price_map, quality_map


def _collect_missing(tickers: list[str], prices: dict[str, float]) -> list[str]:
    return [ticker for ticker in tickers if prices.get(ticker) is None]


def main() -> int:
    parser = argparse.ArgumentParser(description="TECH100 index calc v1")
    parser.add_argument("--start", help="Start date YYYY-MM-DD")
    parser.add_argument("--end", help="End date YYYY-MM-DD (default: max trading day)")
    parser.add_argument("--since-base", action="store_true", help="Use base date 2025-01-02")
    parser.add_argument("--strict", action="store_true")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--allow-close", action="store_true")
    args = parser.parse_args()

    load_env_files(
        paths=(
            "/etc/sustainacore/db.env",
            "/etc/sustainacore-ai/app.env",
            "/etc/sustainacore-ai/secrets.env",
        )
    )

    end_date = _parse_date(args.end) if args.end else db.fetch_max_trading_day()
    if end_date is None:
        print("no_trading_days")
        return 1

    if args.since_base:
        start_date = BASE_DATE
    elif args.start:
        start_date = _parse_date(args.start)
    else:
        start_date = BASE_DATE

    trading_days = db.fetch_trading_days(start_date, end_date)
    if not trading_days:
        print("no_trading_days_in_range")
        return 1

    holdings_by_reb = _fetch_holdings(end_date)
    if not holdings_by_reb:
        print("no_holdings")
        return 1

    divisors_by_reb = _fetch_divisors(end_date)
    if not divisors_by_reb:
        print("no_divisors")
        return 1

    rebalance_dates = sorted(holdings_by_reb.keys())
    divisor_dates = sorted(divisors_by_reb.keys())

    prices_by_date: dict[_dt.date, dict[str, float]] = {}
    prices_quality_by_date: dict[_dt.date, dict[str, str | None]] = {}
    missing_by_date: dict[_dt.date, list[str]] = {}

    for trade_date in trading_days:
        rebalance_date = _latest_key_before(rebalance_dates, trade_date)
        divisor_date = _latest_key_before(divisor_dates, trade_date)
        if rebalance_date is None or divisor_date is None:
            continue
        tickers = list(holdings_by_reb[rebalance_date].keys())
        price_map, quality_map = _collect_prices(trade_date, tickers, allow_close=args.allow_close)
        missing = _collect_missing(tickers, price_map)
        if missing:
            missing_by_date[trade_date] = missing
            if args.strict:
                print(f"missing_prices trade_date={trade_date.isoformat()} count={len(missing)}")
                return 2
        prices_by_date[trade_date] = price_map
        prices_quality_by_date[trade_date] = quality_map

    with run_lock():
        levels = {}
        for trade_date in trading_days:
            rebalance_date = _latest_key_before(rebalance_dates, trade_date)
            divisor_date = _latest_key_before(divisor_dates, trade_date)
            if rebalance_date is None or divisor_date is None:
                continue
            shares = holdings_by_reb[rebalance_date]
            divisor = divisors_by_reb[divisor_date]
            prices_today = prices_by_date.get(trade_date, {})
            market_value = sum(shares[t] * prices_today[t] for t in shares if t in prices_today)
            if divisor:
                levels[trade_date] = market_value / divisor

        levels_rows = [{"trade_date": date, "level_tr": level} for date, level in levels.items()]
        db.upsert_levels(levels_rows)

        weights_by_date = compute_constituent_daily(
            trading_days=sorted(levels.keys()),
            holdings_by_rebalance={d: holdings_by_reb[d] for d in rebalance_dates},
            prices_by_date=prices_by_date,
        )

        constituent_rows = []
        for trade_date, weights in weights_by_date.items():
            rebalance_date = _latest_key_before(rebalance_dates, trade_date)
            if rebalance_date is None:
                continue
            shares_map = holdings_by_reb[rebalance_date]
            prices_today = prices_by_date.get(trade_date, {})
            for ticker, weight in weights.items():
                price_used = prices_today.get(ticker)
                shares = shares_map.get(ticker)
                if price_used is None or shares is None:
                    continue
                market_value = price_used * shares
                constituent_rows.append(
                    {
                        "trade_date": trade_date,
                        "ticker": ticker,
                        "rebalance_date": rebalance_date,
                        "shares": shares,
                        "price_used": price_used,
                        "market_value": market_value,
                        "weight": weight,
                        "price_quality": prices_quality_by_date.get(trade_date, {}).get(ticker),
                    }
                )
        db.upsert_constituent_daily(constituent_rows)

        ordered_levels = sorted(levels.keys())
        contributions = compute_contributions(
            trading_days=ordered_levels,
            weights_by_date=weights_by_date,
            prices_by_date=prices_by_date,
        )
        contrib_rows = []
        for trade_date, rows in contributions.items():
            prev_date = ordered_levels[ordered_levels.index(trade_date) - 1] if trade_date in ordered_levels[1:] else None
            for ticker, contribution in rows.items():
                weight_prev = weights_by_date.get(prev_date, {}).get(ticker) if prev_date else None
                ret_1d = None
                if prev_date:
                    p0 = prices_by_date.get(prev_date, {}).get(ticker)
                    p1 = prices_by_date.get(trade_date, {}).get(ticker)
                    if p0:
                        ret_1d = p1 / p0 - 1.0
                contrib_rows.append(
                    {
                        "trade_date": trade_date,
                        "ticker": ticker,
                        "weight_prev": weight_prev,
                        "ret_1d": ret_1d,
                        "contribution": contribution,
                    }
                )
        db.upsert_contribution_daily(contrib_rows)

        returns_1d = {}
        for idx in range(1, len(ordered_levels)):
            prev_date = ordered_levels[idx - 1]
            trade_date = ordered_levels[idx]
            prev_level = levels.get(prev_date)
            curr_level = levels.get(trade_date)
            if prev_level in (None, 0) or curr_level is None:
                continue
            returns_1d[trade_date] = curr_level / prev_level - 1.0

        stats = compute_stats(
            trading_days=ordered_levels,
            levels=levels,
            weights_by_date=weights_by_date,
            returns_1d=returns_1d,
        )
        stats_rows = []
        for trade_date, row in stats.items():
            n_imputed = sum(
                1 for quality in prices_quality_by_date.get(trade_date, {}).values() if quality == "IMPUTED"
            )
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
                    "n_imputed": n_imputed,
                    "top5_weight": row.get("top5_weight"),
                    "herfindahl": row.get("herfindahl"),
                }
            )
        db.upsert_stats_daily(stats_rows)

    if args.debug:
        print(f"levels_written={len(levels_rows)}")
        print(f"constituent_rows={len(constituent_rows)}")
        print(f"contribution_rows={len(contrib_rows)}")
        print(f"stats_rows={len(stats_rows)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
