"""Check TECH100 price history completeness using canonical prices."""
from __future__ import annotations

import argparse
import datetime as _dt
import os
import pathlib
import sys
from collections import defaultdict
from typing import Iterable

ROOT_PATH = pathlib.Path(__file__).resolve().parents[2]
APP_PATH = ROOT_PATH / "app"
for path in (ROOT_PATH, APP_PATH):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from tools.oracle.env_bootstrap import load_env_files

from index_engine.alerts import send_email
from index_engine.data_quality import evaluate_completeness, generate_weekdays, infer_holidays
from index_engine.db import fetch_constituent_tickers, fetch_distinct_tech100_tickers
from index_engine.run_log import finish_run, start_run
from db_helper import get_connection

BASE_DATE = _dt.date(2025, 1, 2)
SECRETS_ENV = "/etc/sustainacore-ai/secrets.env"


def _parse_date(value: str) -> _dt.date:
    text = value.strip().lower()
    if text == "today":
        return _dt.date.today()
    return _dt.date.fromisoformat(text)


def _load_extra_env(path: str) -> None:
    if not path or not os.path.isfile(path):
        return
    try:
        with open(path, "r", encoding="utf-8") as handle:
            for line in handle:
                text = line.strip()
                if not text or text.startswith("#"):
                    continue
                if text.startswith("export "):
                    text = text[len("export ") :].strip()
                name, sep, value = text.partition("=")
                if sep != "=":
                    continue
                key = name.strip()
                if not key or key in os.environ:
                    continue
                os.environ[key] = value.strip().strip("\"'")
    except (FileNotFoundError, PermissionError):
        return
    except Exception:
        return


def _load_env() -> None:
    load_env_files()
    _load_extra_env(SECRETS_ENV)


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
    holidays: set[_dt.date],
) -> dict[str, int]:
    missing_by_ticker = {ticker: 0 for ticker in expected_tickers}
    for trade_date in weekdays:
        if trade_date in holidays:
            continue
        ok_tickers = ok_by_date.get(trade_date, set())
        for ticker in expected_tickers:
            if ticker not in ok_tickers:
                missing_by_ticker[ticker] += 1
    return missing_by_ticker


def _format_top_dates(
    coverage_by_date: dict[_dt.date, float],
    holidays: set[_dt.date],
    expected_count: int,
    limit: int = 10,
) -> list[str]:
    entries = []
    for trade_date, ratio in coverage_by_date.items():
        if trade_date in holidays:
            continue
        ok = int(round(ratio * expected_count)) if expected_count else 0
        missing = expected_count - ok
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
    holidays: set[_dt.date],
) -> int:
    total = 0
    for trade_date in weekdays:
        if trade_date in holidays:
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


def _select_expected_tickers(start: _dt.date) -> list[str]:
    tickers = fetch_constituent_tickers(start)
    if tickers:
        return tickers
    return fetch_distinct_tech100_tickers()


def main() -> int:
    parser = argparse.ArgumentParser(description="Check TECH100 price completeness")
    parser.add_argument("--start", help="Start date YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="End date YYYY-MM-DD or 'today'")
    parser.add_argument("--since-base", action="store_true", help="Use base date 2025-01-02 as start")
    parser.add_argument("--min-daily-coverage", type=float, default=0.90)
    parser.add_argument("--holiday-coverage-threshold", type=float, default=0.10)
    parser.add_argument("--max-bad-days", type=int, default=0)
    parser.add_argument("--provider", default="CANON")
    parser.add_argument("--allow-canon-close", action="store_true")
    parser.add_argument("--email-on-fail", action="store_true")
    args = parser.parse_args()

    if args.since_base:
        start_date = BASE_DATE
    elif args.start:
        start_date = _parse_date(args.start)
    else:
        raise ValueError("Provide --start or --since-base")

    end_date = _parse_date(args.end)
    if end_date < start_date:
        raise ValueError("end date must be on or after start date")

    _load_env()

    run_id = start_run(
        "completeness_check",
        end_date=end_date,
        provider=args.provider,
        max_provider_calls=None,
        meta={"start_date": start_date},
    )

    status = "ERROR"
    summary_text = ""
    try:
        expected_tickers = _select_expected_tickers(start_date)
        if not expected_tickers:
            raise RuntimeError("no_tech100_tickers_found")

        weekdays = generate_weekdays(start_date, end_date)
        ok_by_date = _fetch_ok_price_rows(
            start=start_date,
            end=end_date,
            tickers=expected_tickers,
            allow_canon_close=args.allow_canon_close,
        )

        coverage_by_date = _build_coverage(weekdays, expected_tickers, ok_by_date)
        holidays = infer_holidays(
            coverage_by_date, threshold=args.holiday_coverage_threshold
        )

        evaluation = evaluate_completeness(
            coverage_by_date,
            holidays=sorted(holidays),
            min_daily_coverage=args.min_daily_coverage,
            max_bad_days=args.max_bad_days,
        )
        status = str(evaluation["status"])

        missing_by_ticker = _collect_missing_by_ticker(
            weekdays,
            expected_tickers,
            ok_by_date,
            holidays,
        )
        total_gaps = _total_gaps(
            weekdays,
            len(expected_tickers),
            ok_by_date,
            holidays,
        )

        non_holiday_weekdays = [d for d in weekdays if d not in holidays]
        worst_dates = [
            item[0]
            for item in sorted(
                ((d, coverage_by_date[d]) for d in non_holiday_weekdays),
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
            expected_trading_days=len(weekdays),
            actual_trading_days=len(non_holiday_weekdays),
            total_gaps=total_gaps,
            worst_dates=worst_dates,
            worst_tickers=worst_tickers,
        )

        print(summary_text)
        print("dates_with_lowest_coverage:")
        for line in _format_top_dates(
            coverage_by_date,
            holidays,
            len(expected_tickers),
        ):
            print(f"  {line}")

        print("tickers_with_most_missing_days:")
        for line in _format_top_tickers(missing_by_ticker):
            print(f"  {line}")

        if status == "FAIL" and args.email_on_fail:
            body = summary_text + "\n\n"
            body += "dates_with_lowest_coverage:\n"
            body += "\n".join(_format_top_dates(coverage_by_date, holidays, len(expected_tickers)))
            body += "\n\ntickers_with_most_missing_days:\n"
            body += "\n".join(_format_top_tickers(missing_by_ticker))
            send_email(
                f"SC_IDX completeness FAIL (start={start_date.isoformat()}, end={end_date.isoformat()})",
                body,
            )
    except Exception as exc:  # pragma: no cover - env specific
        summary_text = f"error={exc}"
        print(f"ERROR {summary_text}")
        status = "ERROR"

    finish_run(
        run_id,
        status="OK" if status != "ERROR" else "ERROR",
        error=summary_text,
    )

    if status == "PASS":
        return 0
    if status == "FAIL":
        return 2
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
