"""Check TECH100 price history completeness using canonical prices."""
from __future__ import annotations

import argparse
import datetime as _dt
import pathlib
import sys
from collections import defaultdict
from typing import Iterable

ROOT_PATH = pathlib.Path(__file__).resolve().parents[2]
APP_PATH = ROOT_PATH / "app"
for path in (ROOT_PATH, APP_PATH):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from tools.index_engine.env_loader import load_default_env

from index_engine.alerts import send_email
from index_engine.alert_state import should_send_alert_once_per_day
from index_engine.data_quality import evaluate_completeness
from index_engine.db import (
    fetch_impacted_tickers_for_trade_date,
    fetch_trading_days,
)
from index_engine.run_log import finish_run, start_run
from db_helper import get_connection

BASE_DATE = _dt.date(2025, 1, 2)
def _parse_date(value: str) -> _dt.date:
    text = value.strip().lower()
    if text == "today":
        return _dt.date.today()
    return _dt.date.fromisoformat(text)


def _build_in_clause(values: Iterable[str], prefix: str) -> tuple[str, dict[str, object]]:
    binds: dict[str, object] = {}
    keys: list[str] = []
    for idx, value in enumerate(values):
        key = f"{prefix}_{idx}"
        keys.append(f":{key}")
        binds[key] = value
    if not keys:
        return "(NULL)", binds
    return "(" + ", ".join(keys) + ")", binds


def _fetch_ok_price_rows(
    *,
    start: _dt.date,
    end: _dt.date,
    tickers: list[str],
    allow_canon_close: bool,
) -> dict[_dt.date, set[str]]:
    if not tickers:
        return {}
    in_clause, binds = _build_in_clause(tickers, "ticker")
    sql = (
        "SELECT trade_date, ticker, canon_adj_close_px, canon_close_px "
        "FROM SC_IDX_PRICES_CANON "
        "WHERE trade_date >= :start_date "
        "AND trade_date <= :end_date "
        f"AND ticker IN {in_clause}"
    )
    binds.update({"start_date": start, "end_date": end})

    ok_by_date: dict[_dt.date, set[str]] = defaultdict(set)
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, binds)
        for trade_date, ticker, adj_close, close_px in cur.fetchall():
            if trade_date is None or ticker is None:
                continue
            has_adj = adj_close is not None
            has_close = allow_canon_close and close_px is not None
            if not (has_adj or has_close):
                continue
            if isinstance(trade_date, _dt.datetime):
                trade_date = trade_date.date()
            ok_by_date[trade_date].add(str(ticker).strip().upper())
    return ok_by_date


def _build_coverage(
    weekdays: list[_dt.date],
    expected_tickers: list[str],
    ok_by_date: dict[_dt.date, set[str]],
) -> dict[_dt.date, float]:
    coverage_by_date: dict[_dt.date, float] = {}
    expected = len(expected_tickers)
    for trade_date in weekdays:
        ok = len(ok_by_date.get(trade_date, set()))
        ratio = ok / expected if expected else 0.0
        coverage_by_date[trade_date] = ratio
    return coverage_by_date


def _collect_missing_by_ticker(
    weekdays: list[_dt.date],
    expected_tickers: list[str],
    ok_by_date: dict[_dt.date, set[str]],
    trading_days: set[_dt.date],
) -> dict[str, int]:
    missing_by_ticker = {ticker: 0 for ticker in expected_tickers}
    for trade_date in weekdays:
        if trade_date not in trading_days:
            continue
        ok_tickers = ok_by_date.get(trade_date, set())
        for ticker in expected_tickers:
            if ticker not in ok_tickers:
                missing_by_ticker[ticker] += 1
    return missing_by_ticker


def _format_top_dates(
    coverage_by_date: dict[_dt.date, float],
    trading_days: set[_dt.date],
    expected_by_date: dict[_dt.date, int],
    limit: int = 10,
) -> list[str]:
    entries = []
    for trade_date, ratio in coverage_by_date.items():
        if trade_date not in trading_days:
            continue
        expected = expected_by_date.get(trade_date, 0)
        ok = int(round(ratio * expected)) if expected else 0
        missing = expected - ok
        entries.append((ratio, trade_date, missing))
    entries.sort(key=lambda item: (item[0], item[1]))
    return [
        f"{trade_date.isoformat()} coverage={ratio:.3f} missing={missing}"
        for ratio, trade_date, missing in entries[:limit]
    ]


def _format_top_tickers(missing_by_ticker: dict[str, int], limit: int = 10) -> list[str]:
    entries = sorted(missing_by_ticker.items(), key=lambda item: (-item[1], item[0]))
    return [f"{ticker} missing_days={count}" for ticker, count in entries[:limit]]


def _total_gaps(
    weekdays: list[_dt.date],
    expected_count: int,
    ok_by_date: dict[_dt.date, set[str]],
    trading_days: set[_dt.date],
) -> int:
    total = 0
    for trade_date in weekdays:
        if trade_date not in trading_days:
            continue
        total += max(expected_count - len(ok_by_date.get(trade_date, set())), 0)
    return total


def _summary_line(
    expected_trading_days: int,
    actual_trading_days: int,
    total_gaps: int,
    worst_dates: list[_dt.date],
    worst_tickers: list[str],
) -> str:
    worst_dates_text = ",".join(date.isoformat() for date in worst_dates) if worst_dates else "none"
    worst_tickers_text = ",".join(worst_tickers) if worst_tickers else "none"
    return (
        "expected_trading_days={expected} actual_trading_days={actual} total_gaps={gaps} "
        "worst_dates={dates} worst_tickers={tickers}"
    ).format(
        expected=expected_trading_days,
        actual=actual_trading_days,
        gaps=total_gaps,
        dates=worst_dates_text,
        tickers=worst_tickers_text,
    )


def _fetch_ok_for_date(
    trade_date: _dt.date,
    tickers: list[str],
    *,
    allow_canon_close: bool,
) -> set[str]:
    if not tickers:
        return set()
    in_clause, binds = _build_in_clause(tickers, "ticker")
    sql = (
        "SELECT ticker, canon_adj_close_px, canon_close_px "
        "FROM SC_IDX_PRICES_CANON "
        "WHERE trade_date = :trade_date "
        f"AND ticker IN {in_clause}"
    )
    binds.update({"trade_date": trade_date})
    ok_tickers: set[str] = set()
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, binds)
        for ticker, adj_close, close_px in cur.fetchall():
            if ticker is None:
                continue
            has_adj = adj_close is not None
            has_close = allow_canon_close and close_px is not None
            if not (has_adj or has_close):
                continue
            ok_tickers.add(str(ticker).strip().upper())
    return ok_tickers


def run_check(
    *,
    start_date: _dt.date,
    end_date: _dt.date,
    min_daily_coverage: float = 1.0,
    max_bad_days: int = 0,
    provider: str = "CANON",
    allow_canon_close: bool = False,
    allow_imputation: bool = False,
    email_on_fail: bool = False,
) -> dict[str, object]:
    load_default_env()

    run_id = start_run(
        "completeness_check",
        end_date=end_date,
        provider=provider,
        max_provider_calls=None,
        meta={"start_date": start_date},
    )

    status = "ERROR"
    summary_text = ""
    details: dict[str, object] = {}
    try:
        trading_days = fetch_trading_days(start_date, end_date)
        if not trading_days:
            raise RuntimeError("no_trading_days_found")
        trading_days_set = set(trading_days)
        impacted_cache: dict[_dt.date, list[str]] = {}
        ok_by_date: dict[_dt.date, set[str]] = {}
        expected_by_date: dict[_dt.date, int] = {}

        for trade_date in trading_days:
            tickers = fetch_impacted_tickers_for_trade_date(trade_date, cache=impacted_cache)
            expected_by_date[trade_date] = len(tickers)
            ok_by_date[trade_date] = _fetch_ok_for_date(
                trade_date,
                tickers,
                allow_canon_close=allow_canon_close,
            )

        coverage_by_date = {
            trade_date: (len(ok_by_date.get(trade_date, set())) / expected_by_date[trade_date])
            if expected_by_date[trade_date]
            else 0.0
            for trade_date in trading_days
        }
        evaluation = evaluate_completeness(
            coverage_by_date,
            holidays=[],
            min_daily_coverage=min_daily_coverage,
            max_bad_days=max_bad_days,
        )
        status = str(evaluation["status"])
        if status == "FAIL" and allow_imputation:
            status = "PASS_WITH_IMPUTATION"

        missing_by_ticker: dict[str, int] = {}
        total_gaps = 0
        for trade_date in trading_days:
            tickers = fetch_impacted_tickers_for_trade_date(trade_date, cache=impacted_cache)
            ok_tickers = ok_by_date.get(trade_date, set())
            total_gaps += max(len(tickers) - len(ok_tickers), 0)
            for ticker in tickers:
                if ticker not in ok_tickers:
                    missing_by_ticker[ticker] = missing_by_ticker.get(ticker, 0) + 1

        worst_dates = [
            item[0]
            for item in sorted(
                ((d, coverage_by_date[d]) for d in trading_days),
                key=lambda item: (item[1], item[0]),
            )[:3]
        ]
        worst_tickers = [
            ticker
            for ticker, _ in sorted(
                missing_by_ticker.items(),
                key=lambda item: (-item[1], item[0]),
            )[:3]
        ]

        summary_text = _summary_line(
            expected_trading_days=len(trading_days),
            actual_trading_days=len(trading_days),
            total_gaps=total_gaps,
            worst_dates=worst_dates,
            worst_tickers=worst_tickers,
        )

        details = {
            "summary": summary_text,
            "coverage_by_date": coverage_by_date,
            "missing_by_ticker": missing_by_ticker,
            "expected_tickers": [],
            "trading_days": trading_days,
            "expected_by_date": expected_by_date,
        }

        if status == "FAIL" and email_on_fail:
            body = summary_text + "\n\n"
            body += "dates_with_lowest_coverage:\n"
            body += "\n".join(_format_top_dates(coverage_by_date, trading_days_set, expected_by_date))
            body += "\n\ntickers_with_most_missing_days:\n"
            body += "\n".join(_format_top_tickers(missing_by_ticker))
            if should_send_alert_once_per_day("sc_idx_completeness_fail", detail=body, status="FAIL"):
                send_email(
                    f"SC_IDX completeness FAIL (start={start_date.isoformat()}, end={end_date.isoformat()})",
                    body,
                )
    except Exception as exc:  # pragma: no cover - env specific
        summary_text = f"error={exc}"
        status = "ERROR"

    finish_run(
        run_id,
        status="OK" if status != "ERROR" else "ERROR",
        error=summary_text,
    )

    return {"status": status, "summary": summary_text, **details}


def main() -> int:
    parser = argparse.ArgumentParser(description="Check TECH100 price completeness")
    parser.add_argument("--start", help="Start date YYYY-MM-DD")
    parser.add_argument("--end", help="End date YYYY-MM-DD or 'today' (default: today)")
    parser.add_argument("--since-base", action="store_true", help="Use base date 2025-01-02 as start")
    parser.add_argument("--min-daily-coverage", type=float, default=1.00)
    parser.add_argument("--max-bad-days", type=int, default=0)
    parser.add_argument("--provider", default="CANON")
    parser.add_argument("--allow-canon-close", action="store_true")
    parser.add_argument("--allow-imputation", action="store_true")
    parser.add_argument("--email-on-fail", action="store_true")
    parser.add_argument("--email", action="store_true", help="Alias for --email-on-fail")
    parser.add_argument("--strict", action="store_true", help="Require 100% coverage (default)")
    args = parser.parse_args()

    if args.since_base:
        start_date = BASE_DATE
    elif args.start:
        start_date = _parse_date(args.start)
    else:
        raise ValueError("Provide --start or --since-base")

    end_value = args.end or "today"
    end_date = _parse_date(end_value)
    if end_date < start_date:
        raise ValueError("end date must be on or after start date")

    email_on_fail = args.email_on_fail or args.email
    min_daily_coverage = 1.0 if args.strict else args.min_daily_coverage

    result = run_check(
        start_date=start_date,
        end_date=end_date,
        min_daily_coverage=min_daily_coverage,
        max_bad_days=args.max_bad_days,
        provider=args.provider,
        allow_canon_close=args.allow_canon_close,
        allow_imputation=args.allow_imputation,
        email_on_fail=email_on_fail,
    )

    status = str(result.get("status"))
    summary_text = str(result.get("summary") or "")
    if status == "ERROR":
        print(f"ERROR {summary_text}")
        return 1

    print(summary_text)
    coverage_by_date = result.get("coverage_by_date") or {}
    trading_days = set(result.get("trading_days") or [])
    missing_by_ticker = result.get("missing_by_ticker") or {}
    expected_by_date = result.get("expected_by_date") or {}

    print("dates_with_lowest_coverage:")
    for line in _format_top_dates(
        coverage_by_date,
        trading_days,
        expected_by_date,
    ):
        print(f"  {line}")

    print("tickers_with_most_missing_days:")
    for line in _format_top_tickers(missing_by_ticker):
        print(f"  {line}")

    if status in {"PASS", "PASS_WITH_IMPUTATION"}:
        return 0
    if status == "FAIL":
        return 2
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
