from __future__ import annotations

import argparse
import datetime as _dt
import os
import sys
from pathlib import Path
from typing import Dict, List, Tuple

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
from index_engine import db as engine_db


BASE_DATE = _dt.date(2025, 1, 2)
BASE_LEVEL = 1000.0
UNIVERSE_DEF = "top25_port_weight_gt0_latest_port_date_le_trade_date"


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


def _collect_impacted_universe(trading_days: List[_dt.date]) -> tuple[dict[_dt.date, list[str]], list[str]]:
    impacted_cache: dict[_dt.date, list[str]] = {}
    impacted_by_date: dict[_dt.date, list[str]] = {}
    union: list[str] = []
    seen = set()
    for trade_date in trading_days:
        tickers = engine_db.fetch_impacted_tickers_for_trade_date(trade_date, cache=impacted_cache)
        impacted_by_date[trade_date] = tickers
        for ticker in tickers:
            if ticker in seen:
                continue
            seen.add(ticker)
            union.append(ticker)
    return impacted_by_date, union


def _collect_missing_diagnostics(
    *,
    start_date: _dt.date,
    end_date: _dt.date,
    max_dates: int,
    max_tickers: int,
    max_samples: int,
) -> dict[str, object]:
    return db.diagnose_missing_canon_sql(
        start=start_date,
        end=end_date,
        max_dates=max_dates,
        max_tickers=max_tickers,
        max_samples=max_samples,
    )


def _render_missing_diagnostics(
    *,
    start_date: _dt.date,
    end_date: _dt.date,
    diagnostics: dict[str, object],
) -> str:
    lines = [
        "index_calc_missing_diagnostics:",
        f"date_range={start_date.isoformat()}..{end_date.isoformat()}",
        f"impacted_universe={UNIVERSE_DEF}",
    ]

    worst_dates: List[Tuple[_dt.date, int, int, int]] = diagnostics.get("missing_by_date", [])
    worst_tickers: List[Tuple[str, int]] = diagnostics.get("missing_by_ticker", [])
    sample_missing: List[Tuple[_dt.date, str, str]] = diagnostics.get("sample_missing", [])

    lines.append("top_missing_dates:")
    if worst_dates:
        for date, missing_count, expected, imputed in worst_dates:
            lines.append(
                f"  {date.isoformat()} missing={missing_count} expected={expected} imputed={imputed}"
            )
    else:
        lines.append("  none")

    lines.append("top_missing_tickers:")
    if worst_tickers:
        for ticker, count in worst_tickers:
            lines.append(f"  {ticker} missing_days={count}")
    else:
        lines.append("  none")

    lines.append("sample_missing:")
    if sample_missing:
        for trade_date, ticker, reason in sample_missing:
            lines.append(f"  {trade_date.isoformat()} {ticker} reason={reason}")
    else:
        lines.append("  none")

    return "\n".join(lines)


def _print_missing_diagnostics(
    *,
    start_date: _dt.date,
    end_date: _dt.date,
    max_dates: int,
    max_tickers: int,
    max_samples: int,
) -> str:
    print("starting diagnostics...", flush=True)
    diagnostics = _collect_missing_diagnostics(
        start_date=start_date,
        end_date=end_date,
        max_dates=max_dates,
        max_tickers=max_tickers,
        max_samples=max_samples,
    )
    output = _render_missing_diagnostics(
        start_date=start_date,
        end_date=end_date,
        diagnostics=diagnostics,
    )
    print(output, flush=True)
    return output


def _run_self_heal(
    *,
    start_date: _dt.date,
    end_date: _dt.date,
    allow_close: bool,
    debug: bool,
) -> None:
    from tools.index_engine import run_daily

    trading_days = engine_db.fetch_trading_days(start_date, end_date)
    _impacted_by_date, union = _collect_impacted_universe(trading_days)
    previous_override = os.environ.get("SC_IDX_TICKERS")
    if union:
        os.environ["SC_IDX_TICKERS"] = ",".join(union)

    run_args: list[str] = []
    if debug:
        run_args.append("--debug")
    run_daily.main(run_args)

    # Clear override so later runs do not inherit
    if previous_override is None:
        os.environ.pop("SC_IDX_TICKERS", None)
    else:
        os.environ["SC_IDX_TICKERS"] = previous_override

    # run_daily already handles completeness + imputation; nothing else required here


def main() -> int:
    parser = argparse.ArgumentParser(description="TECH100 index calc v1")
    parser.add_argument("--start", help="Start date YYYY-MM-DD")
    parser.add_argument("--end", help="End date YYYY-MM-DD (default: max trading day)")
    parser.add_argument("--since-base", action="store_true", help="Use base date 2025-01-02")
    parser.add_argument("--rebuild", action="store_true")
    parser.add_argument("--strict", action="store_true")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--allow-close", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--email-on-fail", action="store_true")
    parser.add_argument(
        "--preflight-self-heal",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Run ingest+impute before strict calc (default: on)",
    )
    parser.add_argument(
        "--diagnose-missing",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Print missing tickers/dates on strict failure (default: on)",
    )
    parser.add_argument(
        "--diagnose-missing-sql",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Use SQL-based diagnostics (default: on)",
    )
    parser.add_argument("--max-dates", type=int, default=10)
    parser.add_argument("--max-tickers", type=int, default=10)
    parser.add_argument("--max-samples", type=int, default=25)
    args = parser.parse_args()

    load_default_env()

    preflight_self_heal = args.preflight_self_heal and not args.dry_run
    if preflight_self_heal:
        from tools.index_engine import update_trading_days

        update_trading_days.update_trading_days(auto_extend=True)

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

    pre_diag_output = ""
    if args.diagnose_missing and not args.dry_run and args.diagnose_missing_sql:
        pre_diag_output = _print_missing_diagnostics(
            start_date=start_date,
            end_date=end_date,
            max_dates=args.max_dates,
            max_tickers=args.max_tickers,
            max_samples=args.max_samples,
        )

    if args.dry_run:
        if args.diagnose_missing:
            _print_missing_diagnostics(
                start_date=start_date,
                end_date=end_date,
                max_dates=args.max_dates,
                max_tickers=args.max_tickers,
                max_samples=args.max_samples,
            )
        return 0

    if preflight_self_heal:
        _run_self_heal(
            start_date=start_date,
            end_date=end_date,
            allow_close=args.allow_close,
            debug=args.debug,
        )

        from tools.index_engine import check_price_completeness

        completeness = check_price_completeness.run_check(
            start_date=start_date,
            end_date=end_date,
            min_daily_coverage=1.0,
            max_bad_days=0,
            provider="CANON",
            allow_canon_close=args.allow_close,
            allow_imputation=True,
            email_on_fail=False,
        )
        if str(completeness.get("status")) in {"FAIL", "ERROR"}:
            summary = pre_diag_output or ""
            if args.diagnose_missing:
                if not summary:
                    summary = _print_missing_diagnostics(
                        start_date=start_date,
                        end_date=end_date,
                        max_dates=args.max_dates,
                        max_tickers=args.max_tickers,
                        max_samples=args.max_samples,
                    )
            if args.email_on_fail and should_send_alert_once_per_day(
                "sc_idx_index_calc_fail", detail=summary, status="FAIL"
            ):
                send_email(
                    "SC_IDX index calc missing prices",
                    summary,
                )
            return 2

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
        summary = pre_diag_output or ""
        if args.diagnose_missing:
            if not summary:
                summary = _print_missing_diagnostics(
                    start_date=start_date,
                    end_date=end_date,
                    max_dates=args.max_dates,
                    max_tickers=args.max_tickers,
                    max_samples=args.max_samples,
                )
        if args.email_on_fail and should_send_alert_once_per_day(
            "sc_idx_index_calc_fail", detail=summary, status="FAIL"
        ):
            send_email("SC_IDX index calc missing prices", summary)
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
