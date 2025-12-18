from __future__ import annotations

import argparse
import datetime as _dt
import sys
from pathlib import Path
from typing import Dict, List

from tools.index_engine.env_loader import load_default_env

REPO_ROOT = Path(__file__).resolve().parents[2]
APP_ROOT = REPO_ROOT / "app"
for path in (REPO_ROOT, APP_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from index_engine.alert_state import should_send_alert_once_per_day
from index_engine.alerts import send_email
from index_engine.index_calc_v1 import (
    compute_constituent_daily,
    compute_contributions,
    compute_holdings_at_rebalance,
    compute_levels,
    compute_stats,
)
from index_engine.run_log import finish_run, start_run
from index_engine import db_index_calc as db


BASE_DATE = _dt.date(2025, 1, 2)
BASE_LEVEL = 1000.0


def _parse_date(value: str) -> _dt.date:
    text = value.strip().lower()
    if text == "today":
        return _dt.date.today()
    return _dt.date.fromisoformat(text)


def _next_trading_day(trading_days: List[_dt.date], date: _dt.date) -> _dt.date | None:
    for day in trading_days:
        if day > date:
            return day
    return None


def _collect_missing(
    trade_date: _dt.date,
    tickers: List[str],
    prices: Dict[str, Dict[str, object]],
) -> list[str]:
    missing = []
    for ticker in tickers:
        price = prices.get(ticker, {}).get("price")
        if price is None:
            missing.append(ticker)
    return missing


def main() -> int:
    parser = argparse.ArgumentParser(description="TECH100 index calc v1")
    parser.add_argument("--start", help="Start date YYYY-MM-DD")
    parser.add_argument("--end", help="End date YYYY-MM-DD (default: max trading day)")
    parser.add_argument("--since-base", action="store_true", help="Use base date 2025-01-02")
    parser.add_argument("--rebuild", action="store_true")
    parser.add_argument("--strict", action="store_true")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--allow-close", action="store_true")
    args = parser.parse_args()

    load_default_env()

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

    if not args.rebuild:
        last_date, _ = db.fetch_last_level_before(start_date)
        if last_date:
            next_day = _next_trading_day(trading_days, last_date)
            if next_day:
                trading_days = [d for d in trading_days if d >= next_day]

    if args.rebuild:
        db.delete_index_range(start_date, end_date)
        db.delete_holdings_divisor(trading_days)

    run_id = start_run(
        "index_calc_v1",
        end_date=end_date,
        provider="CANON",
        max_provider_calls=0,
        meta={"start_date": start_date},
    )

    holdings_by_reb: Dict[_dt.date, Dict[str, float]] = {}
    divisors_by_reb: Dict[_dt.date, float] = {}
    prices_by_date: Dict[_dt.date, Dict[str, float]] = {}
    prices_quality_by_date: Dict[_dt.date, Dict[str, str]] = {}
    weights_by_date: Dict[_dt.date, Dict[str, float]] = {}
    levels: Dict[_dt.date, float] = {}
    returns_1d: Dict[_dt.date, float] = {}
    missing_by_date: Dict[_dt.date, list[str]] = {}

    prev_trade: _dt.date | None = None
    prev_port_date: _dt.date | None = None
    current_reb: _dt.date | None = None
    total_constituent_rows = 0
    total_contrib_rows = 0

    for trade_date in trading_days:
        port_date, tickers = db.fetch_universe(trade_date)
        if port_date is None or not tickers:
            continue

        is_rebalance = port_date != prev_port_date

        if is_rebalance:
            prev_date = prev_trade or trade_date
            prev_level = levels.get(prev_date, BASE_LEVEL)
            prev_divisor = divisors_by_reb.get(current_reb or trade_date, 1.0)
            prices_prev = db.fetch_prices(prev_date, tickers, allow_close=args.allow_close)
            missing_prev = _collect_missing(prev_date, tickers, prices_prev)
            if missing_prev:
                missing_by_date[prev_date] = missing_prev
                current_reb = None
                continue
            price_map_prev = {
                ticker: float(info["price"])
                for ticker, info in prices_prev.items()
                if info.get("price") is not None
            }
            shares, divisor = compute_holdings_at_rebalance(
                tickers=tickers,
                prices_prev=price_map_prev,
                level_prev=prev_level,
                divisor_prev=prev_divisor,
            )
            current_reb = trade_date
            prev_port_date = port_date
            holdings_by_reb[trade_date] = shares
            divisors_by_reb[trade_date] = divisor

            holdings_rows = [
                {"ticker": ticker, "target_weight": 1.0 / len(tickers), "shares": shares[ticker]}
                for ticker in shares
            ]
            db.upsert_holdings(trade_date, holdings_rows)
            db.upsert_divisor(trade_date, divisor, reason="rebalance")

        if current_reb is None:
            continue

        prices_now = db.fetch_prices(trade_date, list(holdings_by_reb[current_reb].keys()), allow_close=args.allow_close)
        missing_now = _collect_missing(trade_date, list(holdings_by_reb[current_reb].keys()), prices_now)
        if missing_now:
            missing_by_date[trade_date] = missing_now
            if args.strict:
                continue

        price_map = {
            ticker: float(info["price"])
            for ticker, info in prices_now.items()
            if info.get("price") is not None
        }
        quality_map = {
            ticker: str(info.get("quality") or "").upper()
            for ticker, info in prices_now.items()
            if info.get("price") is not None
        }
        prices_by_date[trade_date] = price_map
        prices_quality_by_date[trade_date] = quality_map

        level = compute_levels(
            trading_days=[trade_date],
            holdings_by_rebalance={current_reb: holdings_by_reb[current_reb]},
            divisors_by_rebalance={current_reb: divisors_by_reb[current_reb]},
            prices_by_date={trade_date: price_map},
        ).get(trade_date)
        if level is None:
            continue
        levels[trade_date] = level
        if prev_trade and prev_trade in levels:
            returns_1d[trade_date] = level / levels[prev_trade] - 1.0

        weights_by_date.update(
            compute_constituent_daily(
                trading_days=[trade_date],
                holdings_by_rebalance={current_reb: holdings_by_reb[current_reb]},
                prices_by_date={trade_date: price_map},
            )
        )

        constituent_rows = []
        for ticker, shares in holdings_by_reb[current_reb].items():
            info = prices_now.get(ticker, {})
            price_used = info.get("price")
            if price_used is None:
                continue
            quality = str(info.get("quality") or "").upper()
            price_quality = "IMPUTED" if quality == "IMPUTED" else "REAL"
            market_value = shares * float(price_used)
            weight = weights_by_date.get(trade_date, {}).get(ticker)
            constituent_rows.append(
                {
                    "trade_date": trade_date,
                    "ticker": ticker,
                    "rebalance_date": current_reb,
                    "shares": shares,
                    "price_used": price_used,
                    "market_value": market_value,
                    "weight": weight,
                    "price_quality": price_quality,
                }
            )
        db.upsert_constituent_daily(constituent_rows)
        total_constituent_rows += len(constituent_rows)
        prev_trade = trade_date

    if args.strict and missing_by_date:
        missing_preview = ", ".join(
            f"{date.isoformat()}:{','.join(tickers[:5])}" for date, tickers in list(missing_by_date.items())[:5]
        )
        if should_send_alert_once_per_day("index_calc_missing_prices", detail=missing_preview, status="ERROR"):
            send_email("SC_IDX index calc missing prices", missing_preview)
        finish_run(run_id, status="ERROR", error="missing_prices")
        return 2

    levels_rows = [{"trade_date": date, "level_tr": level} for date, level in levels.items()]
    db.upsert_levels(levels_rows)

    ordered_levels = sorted(levels.keys())
    contributions = compute_contributions(
        trading_days=ordered_levels,
        weights_by_date=weights_by_date,
        prices_by_date=prices_by_date,
    )
    contrib_rows = []
    for trade_date, rows in contributions.items():
        prev_date = _prev_trading_day(ordered_levels, trade_date)
        for ticker, contribution in rows.items():
            weight_prev = weights_by_date.get(prev_date, {}).get(ticker)
            ret_1d = None
            if prev_date and ticker in prices_by_date.get(prev_date, {}) and ticker in prices_by_date.get(trade_date, {}):
                p0 = prices_by_date[prev_date][ticker]
                p1 = prices_by_date[trade_date][ticker]
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
            total_contrib_rows += 1
    db.upsert_contribution_daily(contrib_rows)

    stats = compute_stats(
        trading_days=sorted(levels.keys()),
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

    finish_run(run_id, status="OK", error=None)

    if args.debug:
        print(f"levels_written={len(levels_rows)}")
        print(f"constituent_rows={total_constituent_rows}")
        print(f"contribution_rows={total_contrib_rows}")
        print(f"stats_rows={len(stats_rows)}")

    return 0


def _prev_trading_day(trading_days: List[_dt.date], trade_date: _dt.date) -> _dt.date | None:
    for idx, day in enumerate(trading_days):
        if day == trade_date and idx > 0:
            return trading_days[idx - 1]
    return None


if __name__ == "__main__":
    sys.exit(main())
