"""Alpha Vantage TECH100 ingest (backfill + incremental)."""
from __future__ import annotations

import argparse
import datetime as _dt
import http.client  # preload stdlib http to avoid app.http shadowing
import os
import sys
import uuid
from pathlib import Path
from typing import Iterable

APP_PATH = Path(__file__).resolve().parents[2] / "app"
if str(APP_PATH) not in sys.path:
    sys.path.insert(0, str(APP_PATH))

from index_engine.db import (
    fetch_distinct_tech100_tickers,
    fetch_max_ok_trade_date,
    fetch_raw_ok_rows,
    upsert_prices_canon,
    upsert_prices_raw,
)
from index_engine.run_log import fetch_calls_used_today, finish_run, start_run
from tools.index_engine.ingest_prices import compute_canonical_rows
from providers.alphavantage import fetch_daily_adjusted

PROVIDER = "ALPHAVANTAGE"
JOB_NAME = "sc_idx_av_price_ingest"
DEFAULT_START = _dt.date(2025, 1, 2)
MODEL_DAILY_LIMIT = 25
MODEL_DAILY_BUFFER = 3
DAILY_LIMIT_ENV = "SC_IDX_ALPHAVANTAGE_DAILY_LIMIT"
DAILY_BUFFER_ENV = "SC_IDX_ALPHAVANTAGE_DAILY_BUFFER"


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _parse_date(value: str) -> _dt.date:
    return _dt.date.fromisoformat(value)


def _split_tickers(raw: str | None) -> list[str]:
    if not raw:
        return []
    tickers: list[str] = []
    for part in raw.split(","):
        cleaned = part.strip().upper()
        if cleaned:
            tickers.append(cleaned)
    return tickers


def _coerce_date(raw: str | None) -> _dt.date | None:
    if raw is None:
        return None
    try:
        return _dt.date.fromisoformat(str(raw))
    except Exception:
        return None


def _build_raw_rows(ticker: str, payload: list[dict], start: _dt.date, end: _dt.date) -> list[dict]:
    rows: list[dict] = []
    for item in payload:
        trade_date = _coerce_date(item.get("trade_date"))
        if trade_date is None or trade_date < start or trade_date > end:
            continue
        close_px = item.get("close")
        adj_close_px = item.get("adj_close") if item.get("adj_close") is not None else close_px
        rows.append(
            {
                "ticker": ticker,
                "trade_date": trade_date,
                "provider": PROVIDER,
                "close_px": close_px,
                "adj_close_px": adj_close_px,
                "volume": item.get("volume"),
                "currency": None,
                "status": "OK",
                "error_msg": None,
            }
        )
    return rows


def _reconcile_canonical(tickers: Iterable[str], start: _dt.date, end: _dt.date) -> int:
    raw_rows = fetch_raw_ok_rows(sorted(set(tickers)), start, end)
    if not raw_rows:
        return 0
    canon_rows = compute_canonical_rows(raw_rows)
    if not canon_rows:
        return 0
    return upsert_prices_canon(canon_rows)


def _compute_budget() -> tuple[int, int, int, int]:
    calls_used_today = fetch_calls_used_today(PROVIDER)
    daily_limit = _env_int(DAILY_LIMIT_ENV, MODEL_DAILY_LIMIT)
    daily_buffer = _env_int(DAILY_BUFFER_ENV, MODEL_DAILY_BUFFER)
    max_provider_calls = max(0, max(0, daily_limit - calls_used_today) - daily_buffer)
    return calls_used_today, daily_limit, daily_buffer, max_provider_calls


def _prepare_ticker_list(raw: str | None) -> list[str]:
    tickers = sorted(set(_split_tickers(raw)))
    if tickers:
        return tickers
    return sorted(fetch_distinct_tech100_tickers())


def _print_summary(summary: dict) -> None:
    print(
        "summary: tickers_processed={tickers} provider_calls_used={calls} raw_ok_rows_written={ok} "
        "raw_error_rows_written={error} canon_rows_written={canon} budget_stop={budget}".format(
            tickers=summary.get("tickers_processed", 0),
            calls=summary.get("provider_calls_used", 0),
            ok=summary.get("raw_ok_rows_written", 0),
            error=summary.get("raw_error_rows_written", 0),
            canon=summary.get("canon_rows_written", 0),
            budget=summary.get("budget_stop", False),
        )
    )


def _run_backfill(
    tickers: list[str],
    start_date: _dt.date,
    end_date: _dt.date,
    max_provider_calls: int | None,
) -> dict:
    provider_calls_used = 0
    raw_ok = 0
    raw_error = 0
    tickers_called: list[str] = []
    tickers_with_updates: list[str] = []
    earliest_ok: _dt.date | None = None
    budget_stop = False

    for ticker in tickers:
        if max_provider_calls is not None and provider_calls_used >= max_provider_calls:
            budget_stop = True
            break

        try:
            max_ok = fetch_max_ok_trade_date(ticker, PROVIDER)
        except Exception as exc:
            print(f"warning: failed to probe {ticker} max date: {exc}", file=sys.stderr)
            continue

        ticker_start = start_date
        if max_ok:
            if max_ok >= end_date:
                continue
            ticker_start = max(max_ok + _dt.timedelta(days=1), start_date)

        provider_calls_used += 1
        tickers_called.append(ticker)

        try:
            payload = fetch_daily_adjusted(ticker, outputsize="full")
            rows = _build_raw_rows(ticker, payload, ticker_start, end_date)
            if rows:
                upsert_prices_raw(rows)
                raw_ok += len(rows)
                tickers_with_updates.append(ticker)
                row_dates = [row["trade_date"] for row in rows if row.get("trade_date")]
                if row_dates:
                    min_date = min(row_dates)
                    if earliest_ok is None or min_date < earliest_ok:
                        earliest_ok = min_date
        except Exception as exc:
            error_date = ticker_start if ticker_start <= end_date else end_date
            error_row = {
                "ticker": ticker,
                "trade_date": error_date,
                "provider": PROVIDER,
                "close_px": None,
                "adj_close_px": None,
                "volume": None,
                "currency": None,
                "status": "ERROR",
                "error_msg": str(exc),
            }
            upsert_prices_raw([error_row])
            raw_error += 1

    canon_rows = 0
    if tickers_with_updates and earliest_ok:
        canon_rows = _reconcile_canonical(tickers_with_updates, earliest_ok, end_date)

    return {
        "provider_calls_used": provider_calls_used,
        "raw_ok_rows_written": raw_ok,
        "raw_error_rows_written": raw_error,
        "canon_rows_written": canon_rows,
        "tickers_processed": len(tickers_called),
        "budget_stop": budget_stop,
        "earliest_date": earliest_ok,
        "tickers_with_updates": sorted(set(tickers_with_updates)),
    }


def _run_incremental(
    tickers: list[str],
    end_date: _dt.date,
    max_provider_calls: int | None,
) -> dict:
    provider_calls_used = 0
    raw_ok = 0
    raw_error = 0
    tickers_called: list[str] = []
    tickers_with_updates: list[str] = []
    earliest_ok: _dt.date | None = None
    budget_stop = False

    for ticker in tickers:
        if max_provider_calls is not None and provider_calls_used >= max_provider_calls:
            budget_stop = True
            break

        try:
            max_ok = fetch_max_ok_trade_date(ticker, PROVIDER)
        except Exception as exc:
            print(f"warning: failed to probe {ticker} max date: {exc}", file=sys.stderr)
            continue

        ticker_start = DEFAULT_START if max_ok is None else max_ok + _dt.timedelta(days=1)
        if ticker_start > end_date:
            continue

        provider_calls_used += 1
        tickers_called.append(ticker)

        try:
            payload = fetch_daily_adjusted(ticker, outputsize="compact")
            rows = _build_raw_rows(ticker, payload, ticker_start, end_date)
            if rows:
                upsert_prices_raw(rows)
                raw_ok += len(rows)
                tickers_with_updates.append(ticker)
                row_dates = [row["trade_date"] for row in rows if row.get("trade_date")]
                if row_dates:
                    min_date = min(row_dates)
                    if earliest_ok is None or min_date < earliest_ok:
                        earliest_ok = min_date
        except Exception as exc:
            error_date = ticker_start if ticker_start <= end_date else end_date
            error_row = {
                "ticker": ticker,
                "trade_date": error_date,
                "provider": PROVIDER,
                "close_px": None,
                "adj_close_px": None,
                "volume": None,
                "currency": None,
                "status": "ERROR",
                "error_msg": str(exc),
            }
            upsert_prices_raw([error_row])
            raw_error += 1

    canon_rows = 0
    if tickers_with_updates and earliest_ok:
        canon_rows = _reconcile_canonical(tickers_with_updates, earliest_ok, end_date)

    return {
        "provider_calls_used": provider_calls_used,
        "raw_ok_rows_written": raw_ok,
        "raw_error_rows_written": raw_error,
        "canon_rows_written": canon_rows,
        "tickers_processed": len(tickers_called),
        "budget_stop": budget_stop,
        "earliest_date": earliest_ok,
        "tickers_with_updates": sorted(set(tickers_with_updates)),
    }


def _determine_incremental_end_date(explicit_end: str | None, today: _dt.date) -> _dt.date:
    if explicit_end:
        return _parse_date(explicit_end)
    try:
        probe_rows = fetch_daily_adjusted("AAPL", outputsize="compact")
        dates = [_coerce_date(row.get("trade_date")) for row in probe_rows]
        dates = [d for d in dates if d is not None]
        if dates:
            latest = max(dates)
            return today if latest >= today else today - _dt.timedelta(days=1)
    except Exception as exc:
        print(f"warning: failed to probe Alpha Vantage end date: {exc}", file=sys.stderr)
    return today - _dt.timedelta(days=1)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Ingest Alpha Vantage TECH100 prices")
    parser.add_argument("--backfill", action="store_true", help="run log for historic range")
    parser.add_argument("--incremental", action="store_true", help="run in incremental mode")
    parser.add_argument("--start", help="backfill start date (YYYY-MM-DD)")
    parser.add_argument("--end", help="end date (YYYY-MM-DD)")
    parser.add_argument("--tickers", help="comma separated tickers override")
    parser.add_argument("--debug", action="store_true", help="print diagnostics")

    args = parser.parse_args(argv)
    mode = "backfill" if args.backfill else "incremental"
    if args.backfill and args.incremental:
        print("Specify only one of --backfill or --incremental")
        return 1

    try:
        tickers = _prepare_ticker_list(args.tickers)
    except Exception as exc:
        print(f"Failed to load tickers: {exc}")
        return 1

    today_utc = _dt.datetime.now(_dt.timezone.utc).date()
    calls_used_today, daily_limit, daily_buffer, max_provider_calls = _compute_budget()
    remaining_daily = max(0, daily_limit - calls_used_today)
    budget_stop = remaining_daily <= daily_buffer
    end_date: _dt.date | None = None
    start_date: _dt.date | None = None

    if mode == "backfill":
        if not args.start or not args.end:
            print("--backfill requires --start and --end")
            return 1
        start_date = _parse_date(args.start)
        end_date = _parse_date(args.end)
        if end_date < start_date:
            print("Invalid backfill range: end must be on or after start")
            return 1
    else:
        if today_utc.weekday() >= 5:
            print("weekend_skip: Alpha Vantage ingest runs Mon-Fri only")
            return 0
        if budget_stop:
            end_date = today_utc
        else:
            end_date = _determine_incremental_end_date(args.end, today_utc)

    run_id = str(uuid.uuid4())
    start_run(
        JOB_NAME,
        end_date=end_date,
        provider=PROVIDER,
        max_provider_calls=max_provider_calls,
        meta={
            "run_id": run_id,
            "start_date": start_date,
            "usage_current": None,
            "usage_limit": daily_limit,
            "usage_remaining": remaining_daily,
            "credit_buffer": daily_buffer,
        },
    )

    summary: dict = {}
    status = "COMPLETED"
    try:
        if budget_stop:
            print(
                f"budget_stop_av: limit={daily_limit} buffer={daily_buffer} remaining={remaining_daily}"
            )
            summary = {
                "provider_calls_used": 0,
                "raw_ok_rows_written": 0,
                "raw_error_rows_written": 0,
                "canon_rows_written": 0,
                "tickers_processed": 0,
                "budget_stop": True,
            }
        elif mode == "backfill":
            summary = _run_backfill(tickers, start_date, end_date, max_provider_calls)
        else:
            summary = _run_incremental(tickers, end_date, max_provider_calls)

        _print_summary(summary)
        if summary.get("budget_stop"):
            status = "BUDGET_STOP"
    except Exception as exc:
        status = "ERROR"
        finish_run(
            run_id,
            status=status,
            provider_calls_used=summary.get("provider_calls_used", 0),
            raw_upserts=summary.get("raw_ok_rows_written", 0),
            canon_upserts=summary.get("canon_rows_written", 0),
            raw_ok=summary.get("raw_ok_rows_written", 0),
            raw_missing=0,
            raw_error=summary.get("raw_error_rows_written", 0),
            max_provider_calls=max_provider_calls,
            usage_current=None,
            usage_limit=daily_limit,
            usage_remaining=remaining_daily,
            oracle_user=None,
            error=str(exc),
        )
        print(f"error: {exc}", file=sys.stderr)
        return 1
    finish_run(
        run_id,
        status=status,
        provider_calls_used=summary.get("provider_calls_used", 0),
        raw_upserts=summary.get("raw_ok_rows_written", 0),
        canon_upserts=summary.get("canon_rows_written", 0),
        raw_ok=summary.get("raw_ok_rows_written", 0),
        raw_missing=0,
        raw_error=summary.get("raw_error_rows_written", 0),
        max_provider_calls=max_provider_calls,
        usage_current=None,
        usage_limit=daily_limit,
        usage_remaining=remaining_daily,
        oracle_user=None,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
