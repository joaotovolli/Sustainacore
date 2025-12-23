"""Ingest EOD prices for TECH100 constituents."""
from __future__ import annotations

import argparse
import datetime as _dt
import http.client  # preload stdlib http to avoid app.http shadowing
import pathlib
import sys
from collections import defaultdict
from typing import Iterable, List, Optional

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.index_engine.env_loader import load_default_env

load_default_env()

APP_PATH = pathlib.Path(__file__).resolve().parents[2] / "app"
if str(APP_PATH) not in sys.path:
    sys.path.insert(0, str(APP_PATH))

from index_engine.db import (
    fetch_constituent_tickers,
    fetch_distinct_tech100_tickers,
    fetch_max_ok_trade_date,
    fetch_missing_canon_pairs,
    fetch_trading_days,
    get_current_user,
    upsert_prices_canon,
    upsert_prices_raw,
)
from index_engine.reconcile import reconcile_canonical
from providers.twelvedata import fetch_eod_prices

PROVIDER = "TWELVEDATA"


def _parse_date(value: str) -> _dt.date:
    return _dt.date.fromisoformat(value)


def _parse_dates(args: argparse.Namespace) -> list[_dt.date]:
    if args.date:
        return [_parse_date(args.date)]
    if args.start and args.end:
        start = _parse_date(args.start)
        end = _parse_date(args.end)
        if end < start:
            raise ValueError("end date must be on or after start date")
        days = (end - start).days + 1
        return [start + _dt.timedelta(days=i) for i in range(days)]
    raise ValueError("Provide --date or both --start and --end")


def _split_tickers(raw: Optional[str]) -> list[str]:
    if not raw:
        return []
    tickers: List[str] = []
    for part in raw.split(","):
        cleaned = part.strip().upper()
        if cleaned:
            tickers.append(cleaned)
    return tickers


def _collect_constituents(dates: Iterable[_dt.date], override: list[str] | None = None) -> dict[_dt.date, list[str]]:
    tickers_by_date: dict[_dt.date, list[str]] = {}
    for trade_date in dates:
        tickers_by_date[trade_date] = override or fetch_constituent_tickers(trade_date)
    return tickers_by_date


def _normalize_ticker(value: object) -> str:
    return str(value or "").strip().upper()


def _is_bad_price(value: object) -> bool:
    if value is None:
        return True
    try:
        return float(value) <= 0
    except (TypeError, ValueError):
        return True


def _build_raw_rows_from_provider_with_stats(provider_rows: list[dict]) -> tuple[list[dict], dict]:
    raw_rows: list[dict] = []
    stats = {"skipped": 0, "errors": 0}
    for row in provider_rows:
        try:
            trade_date = _dt.date.fromisoformat(str(row.get("trade_date")))
        except Exception:
            stats["skipped"] += 1
            continue
        ticker = _normalize_ticker(row.get("ticker"))
        if not ticker:
            stats["skipped"] += 1
            continue
        close_px = row.get("close")
        if _is_bad_price(close_px):
            stats["errors"] += 1
            raw_rows.append(
                {
                    "ticker": ticker,
                    "trade_date": trade_date,
                    "provider": PROVIDER,
                    "close_px": None,
                    "adj_close_px": None,
                    "volume": row.get("volume"),
                    "currency": row.get("currency"),
                    "status": "ERROR",
                    "error_msg": "invalid_close",
                }
            )
            continue
        adj_close = row.get("adj_close")
        if _is_bad_price(adj_close):
            adj_close = close_px
        raw_rows.append(
            {
                "ticker": ticker,
                "trade_date": trade_date,
                "provider": PROVIDER,
                "close_px": close_px,
                "adj_close_px": adj_close,
                "volume": row.get("volume"),
                "currency": row.get("currency"),
                "status": "OK",
                "error_msg": None,
            }
        )
    return raw_rows, stats


def _build_raw_rows_from_provider(provider_rows: list[dict]) -> list[dict]:
    raw_rows, _stats = _build_raw_rows_from_provider_with_stats(provider_rows)
    return raw_rows


def _build_missing_rows(expected_pairs: Iterable[tuple[str, _dt.date]], present_pairs: set[tuple[str, _dt.date]]) -> list[dict]:
    missing_rows: list[dict] = []
    for ticker, trade_date in expected_pairs:
        if (ticker, trade_date) in present_pairs:
            continue
        missing_rows.append(
            {
                "ticker": ticker,
                "trade_date": trade_date,
                "provider": PROVIDER,
                "close_px": None,
                "adj_close_px": None,
                "volume": None,
                "currency": None,
                "status": "MISSING",
                "error_msg": "no_data",
            }
        )
    return missing_rows


def compute_canonical_rows(raw_rows: list[dict]) -> list[dict]:
    grouped: dict[tuple[str, _dt.date], list[dict]] = defaultdict(list)
    for row in raw_rows:
        ticker = row.get("ticker")
        trade_date = row.get("trade_date")
        if ticker and trade_date:
            grouped[(ticker, trade_date)].append(row)

    canon_rows: list[dict] = []
    for (ticker, trade_date), entries in grouped.items():
        provider_adj_closes = {}
        provider_closes = {}
        for entry in entries:
            if entry.get("status") != "OK":
                continue
            provider = entry.get("provider")
            if not provider:
                continue
            adj_value = entry.get("adj_close_px")
            close_value = entry.get("close_px")
            if adj_value is None:
                adj_value = close_value
            if adj_value is None or close_value is None:
                continue
            provider_adj_closes[provider] = adj_value
            provider_closes[provider] = close_value

        recon = reconcile_canonical(provider_adj_closes, provider_closes)
        if recon.get("providers_ok", 0) == 0:
            continue
        canon_rows.append(
            {
                "ticker": ticker,
                "trade_date": trade_date,
                "canon_close_px": recon.get("canon_close"),
                "canon_adj_close_px": recon.get("canon_adj_close"),
                "chosen_provider": recon.get("chosen_provider") or PROVIDER,
                "providers_ok": recon.get("providers_ok", 0),
                "divergence_pct": recon.get("divergence_pct"),
                "quality": recon.get("quality", "LOW"),
            }
        )
    return canon_rows


def _print_summary(
    raw_rows: list[dict],
    canon_rows: list[dict],
    *,
    provider_calls_used: int | None = None,
    tickers_requested: int | None = None,
    tickers_returned: int | None = None,
    rows_skipped: int | None = None,
    raw_upserts: int | None = None,
    canon_upserts: int | None = None,
) -> None:
    status_counts = {"OK": 0, "MISSING": 0, "ERROR": 0}
    for row in raw_rows:
        status = row.get("status")
        if status in status_counts:
            status_counts[status] += 1
    parts = [
        f"tickers_requested={tickers_requested}" if tickers_requested is not None else None,
        f"tickers_returned={tickers_returned}" if tickers_returned is not None else None,
        f"rows_ok={status_counts['OK']}",
        f"rows_missing={status_counts['MISSING']}",
        f"rows_error={status_counts['ERROR']}",
        f"rows_skipped={rows_skipped}" if rows_skipped is not None else None,
        f"raw_upserts={raw_upserts}" if raw_upserts is not None else None,
        f"canon_upserts={canon_upserts}" if canon_upserts is not None else None,
    ]
    if provider_calls_used is not None:
        parts.append(f"provider_calls_used={provider_calls_used}")
    summary = "ingest_summary: " + " ".join([p for p in parts if p is not None])
    print(summary)


def _print_debug(
    *,
    dates: list[_dt.date],
    tickers_by_date: dict[_dt.date, list[str]] | None,
    all_tickers: list[str],
    provider_called: bool,
    provider_rows: list[dict],
    raw_rows: list[dict],
    canon_rows: list[dict],
    provider_error: str | None,
    oracle_user: str | None,
    oracle_user_error: str | None,
    backfill: bool = False,
    provider_calls_used: int | None = None,
) -> None:
    print(f"debug: dates={len(dates)} total_unique_tickers={len(all_tickers)} backfill={backfill}")
    if tickers_by_date:
        for trade_date in sorted(tickers_by_date):
            tickers = tickers_by_date.get(trade_date) or []
            preview = ", ".join(tickers[:5])
            suffix = f" preview=[{preview}]" if preview else ""
            print(f"debug: {trade_date.isoformat()} tickers={len(tickers)}{suffix}")
    if oracle_user:
        print(f"debug: oracle_user={oracle_user}")
    elif oracle_user_error:
        print(f"debug: oracle_user_lookup_failed={oracle_user_error}")

    if provider_error:
        print(f"debug: provider_error={provider_error}")

    budget_note = ""
    if provider_calls_used is not None:
        budget_note = f" provider_calls_used={provider_calls_used}"

    print(
        "debug: provider_called={called} provider_rows={provider_rows} raw_rows={raw_rows} canon_rows={canon_rows}{budget}".format(
            called=provider_called,
            provider_rows=len(provider_rows),
            raw_rows=len(raw_rows),
            canon_rows=len(canon_rows),
            budget=budget_note,
        )
    )


def _run_single(args: argparse.Namespace) -> tuple[int, dict]:
    summary: dict[str, int | _dt.date | None] = {
        "provider_calls_used": 0,
        "raw_upserts": 0,
        "canon_upserts": 0,
        "raw_ok": 0,
        "raw_missing": 0,
        "raw_error": 0,
        "max_ok_trade_date": None,
    }
    try:
        dates = _parse_dates(args)
    except Exception as exc:
        print(f"Invalid date arguments: {exc}")
        return 1, summary

    tickers_by_date: dict[_dt.date, list[str]] = {}
    manual_tickers = _split_tickers(args.tickers)
    try:
        tickers_by_date = _collect_constituents(dates, manual_tickers)
    except Exception as exc:
        print(f"Failed to load constituents: {exc}")
        return 1, summary

    oracle_user = None
    oracle_user_error = None
    if args.debug:
        try:
            oracle_user = get_current_user()
        except Exception as exc:  # pragma: no cover - defensive diagnostics
            oracle_user_error = str(exc)

    all_tickers = sorted({t for vals in tickers_by_date.values() for t in vals})
    provider_rows: list[dict] = []
    provider_error: str | None = None
    provider_called = False
    provider_stats = {"skipped": 0, "errors": 0}

    if all_tickers:
        provider_called = True
        try:
            provider_rows = fetch_eod_prices(
                all_tickers,
                dates[0].isoformat(),
                dates[-1].isoformat(),
            )
        except Exception as exc:
            provider_error = str(exc)
            print(f"Provider fetch failed: {exc}")

    raw_rows, provider_stats = _build_raw_rows_from_provider_with_stats(provider_rows)
    expected_pairs = [
        (ticker, trade_date)
        for trade_date, tickers in tickers_by_date.items()
        for ticker in tickers
    ]
    present_pairs = {(row.get("ticker"), row.get("trade_date")) for row in raw_rows if row.get("ticker") and row.get("trade_date")}
    missing_rows = _build_missing_rows(expected_pairs, present_pairs)
    if missing_rows:
        raw_rows.extend(missing_rows)
    status_counts = {"OK": 0, "MISSING": 0, "ERROR": 0}
    for row in raw_rows:
        status = row.get("status")
        if status in status_counts:
            status_counts[status] += 1

    raw_written = upsert_prices_raw(raw_rows) if raw_rows else 0

    canon_rows = compute_canonical_rows(raw_rows)
    canon_written = upsert_prices_canon(canon_rows) if canon_rows else 0

    max_ok = None
    ok_dates = [row.get("trade_date") for row in raw_rows if row.get("status") == "OK" and row.get("trade_date")]
    if ok_dates:
        max_ok = max(ok_dates)

    if args.debug:
        _print_debug(
            dates=dates,
            tickers_by_date=tickers_by_date,
            all_tickers=all_tickers,
            provider_called=provider_called,
            provider_rows=provider_rows,
            raw_rows=raw_rows,
            canon_rows=canon_rows,
            provider_error=provider_error,
            oracle_user=oracle_user,
            oracle_user_error=oracle_user_error,
        )

    tickers_returned = len({(_normalize_ticker(row.get("ticker"))) for row in provider_rows if row.get("ticker")})
    _print_summary(
        raw_rows,
        canon_rows,
        provider_calls_used=1 if provider_called else 0,
        tickers_requested=len(all_tickers),
        tickers_returned=tickers_returned,
        rows_skipped=provider_stats.get("skipped"),
        raw_upserts=raw_written,
        canon_upserts=canon_written,
    )

    summary.update(
        {
            "provider_calls_used": 1 if provider_called else 0,
            "raw_upserts": raw_written,
            "canon_upserts": canon_written,
            "raw_ok": status_counts["OK"],
            "raw_missing": status_counts["MISSING"],
            "raw_error": status_counts["ERROR"],
            "max_ok_trade_date": max_ok,
        }
    )
    return 0, summary


def _run_backfill(args: argparse.Namespace) -> tuple[int, dict]:
    empty_summary = {
        "provider_calls_used": 0,
        "raw_upserts": 0,
        "canon_upserts": 0,
        "raw_ok": 0,
        "raw_missing": 0,
        "raw_error": 0,
        "max_ok_trade_date": None,
    }
    if not args.start or not args.end:
        print("Backfill mode requires --start and --end")
        return 1, empty_summary

    start_date = _parse_date(args.start)
    end_date = _parse_date(args.end)
    if end_date < start_date:
        print("Invalid date range: end must be on or after start")
        return 1, empty_summary

    trading_days = fetch_trading_days(start_date, end_date)
    if not trading_days:
        print("No trading days found for requested range")
        return 1, empty_summary
    last_trading_day = trading_days[-1]

    max_provider_calls = args.max_provider_calls
    provider_calls_used = 0

    oracle_user = None
    oracle_user_error = None
    if args.debug:
        try:
            oracle_user = get_current_user()
        except Exception as exc:  # pragma: no cover - defensive diagnostics
            oracle_user_error = str(exc)

    tickers = _split_tickers(args.tickers)
    if not tickers:
        try:
            tickers = fetch_distinct_tech100_tickers()
        except Exception as exc:
            print(f"Failed to load tickers: {exc}")
            return 1, empty_summary

    total_raw = 0
    total_canon = 0
    total_status = {"OK": 0, "MISSING": 0, "ERROR": 0}
    provider_error: str | None = None
    max_ok_trade_date = None
    total_skipped = 0

    for ticker in tickers:
        if max_provider_calls is not None and provider_calls_used >= max_provider_calls:
            print(
                f"budget_stop: provider_calls_used={provider_calls_used} "
                f"max_provider_calls={max_provider_calls}"
            )
            break

        provider_error = None
        try:
            max_ok = fetch_max_ok_trade_date(ticker, PROVIDER)
        except Exception as exc:
            print(f"Failed to probe max date for {ticker}: {exc}")
            continue

        ticker_start = start_date
        if max_ok:
            if max_ok >= last_trading_day:
                continue
            ticker_start = max(max_ok + _dt.timedelta(days=1), start_date)
        if ticker_start > last_trading_day:
            continue

        provider_calls_used += 1
        try:
            provider_rows = fetch_eod_prices(
                [ticker],
                ticker_start.isoformat(),
                last_trading_day.isoformat(),
            )
        except Exception as exc:
            provider_error = str(exc)
            print(f"Provider fetch failed for {ticker}: {exc}")
            continue

        raw_rows, stats = _build_raw_rows_from_provider_with_stats(provider_rows)
        total_skipped += stats.get("skipped", 0)
        expected_days = [d for d in trading_days if ticker_start <= d <= last_trading_day]
        expected_pairs = [(ticker, trade_date) for trade_date in expected_days]
        present_pairs = {(row.get("ticker"), row.get("trade_date")) for row in raw_rows if row.get("ticker") and row.get("trade_date")}
        missing_rows = _build_missing_rows(expected_pairs, present_pairs)
        if missing_rows:
            raw_rows.extend(missing_rows)

        if raw_rows:
            total_raw += upsert_prices_raw(raw_rows)
        for row in raw_rows:
            status = row.get("status")
            if status in total_status:
                total_status[status] += 1
            if status == "OK" and row.get("trade_date"):
                date_value = row.get("trade_date")
                if max_ok_trade_date is None or date_value > max_ok_trade_date:
                    max_ok_trade_date = date_value
        canon_rows = compute_canonical_rows(raw_rows)
        if canon_rows:
            total_canon += upsert_prices_canon(canon_rows)

        if args.debug:
            _print_debug(
                dates=[ticker_start, end_date],
                tickers_by_date=None,
                all_tickers=[ticker],
                provider_called=True,
                provider_rows=provider_rows,
                raw_rows=raw_rows,
                canon_rows=canon_rows,
                provider_error=provider_error,
                oracle_user=oracle_user,
                oracle_user_error=oracle_user_error,
                backfill=True,
                provider_calls_used=provider_calls_used,
            )

    print(
        "backfill_summary: raw_upserts={raw} canon_upserts={canon} provider_calls_used={calls} rows_skipped={skipped}".format(
            raw=total_raw, canon=total_canon, calls=provider_calls_used, skipped=total_skipped
        )
    )
    summary = {
        "provider_calls_used": provider_calls_used,
        "raw_upserts": total_raw,
        "canon_upserts": total_canon,
        "raw_ok": total_status["OK"],
        "raw_missing": total_status["MISSING"],
        "raw_error": total_status["ERROR"],
        "max_ok_trade_date": max_ok_trade_date,
    }
    return 0, summary


def _run_backfill_missing(args: argparse.Namespace) -> tuple[int, dict]:
    empty_summary = {
        "provider_calls_used": 0,
        "raw_upserts": 0,
        "canon_upserts": 0,
        "raw_ok": 0,
        "raw_missing": 0,
        "raw_error": 0,
        "max_ok_trade_date": None,
    }
    if not args.start or not args.end:
        print("Backfill-missing mode requires --start and --end")
        return 1, empty_summary

    start_date = _parse_date(args.start)
    end_date = _parse_date(args.end)
    if end_date < start_date:
        print("Invalid date range: end must be on or after start")
        return 1, empty_summary

    tickers = _split_tickers(args.tickers)
    if not tickers:
        try:
            tickers = fetch_distinct_tech100_tickers()
        except Exception as exc:
            print(f"Failed to load tickers: {exc}")
            return 1, empty_summary

    missing_pairs = fetch_missing_canon_pairs(start_date, end_date, tickers, allow_imputed=False)
    if not missing_pairs:
        print("backfill_missing_summary: missing_pairs=0")
        return 0, empty_summary

    missing_by_ticker: dict[str, list[_dt.date]] = defaultdict(list)
    for ticker, trade_date in missing_pairs:
        missing_by_ticker[ticker].append(trade_date)

    max_provider_calls = args.max_provider_calls
    provider_calls_used = 0
    total_raw = 0
    total_canon = 0
    total_status = {"OK": 0, "MISSING": 0, "ERROR": 0}
    total_skipped = 0
    max_ok_trade_date = None

    for ticker, dates in sorted(missing_by_ticker.items()):
        if max_provider_calls is not None and provider_calls_used >= max_provider_calls:
            print(
                f"budget_stop: provider_calls_used={provider_calls_used} "
                f"max_provider_calls={max_provider_calls}"
            )
            break

        missing_set = {(ticker, d) for d in dates}
        date_start = min(dates)
        date_end = max(dates)

        provider_calls_used += 1
        try:
            provider_rows = fetch_eod_prices(
                [ticker],
                date_start.isoformat(),
                date_end.isoformat(),
            )
        except Exception as exc:
            print(f"Provider fetch failed for {ticker}: {exc}")
            continue

        raw_rows, stats = _build_raw_rows_from_provider_with_stats(provider_rows)
        total_skipped += stats.get("skipped", 0)
        raw_rows = [row for row in raw_rows if (row.get("ticker"), row.get("trade_date")) in missing_set]

        present_pairs = {(row.get("ticker"), row.get("trade_date")) for row in raw_rows if row.get("ticker") and row.get("trade_date")}
        missing_rows = _build_missing_rows(missing_set, present_pairs)
        if missing_rows:
            raw_rows.extend(missing_rows)

        if raw_rows:
            total_raw += upsert_prices_raw(raw_rows)
        for row in raw_rows:
            status = row.get("status")
            if status in total_status:
                total_status[status] += 1
            if status == "OK" and row.get("trade_date"):
                date_value = row.get("trade_date")
                if max_ok_trade_date is None or date_value > max_ok_trade_date:
                    max_ok_trade_date = date_value

        canon_rows = compute_canonical_rows(raw_rows)
        if canon_rows:
            total_canon += upsert_prices_canon(canon_rows)

    print(
        "backfill_missing_summary: raw_upserts={raw} canon_upserts={canon} "
        "provider_calls_used={calls} rows_skipped={skipped} missing_pairs={missing}".format(
            raw=total_raw,
            canon=total_canon,
            calls=provider_calls_used,
            skipped=total_skipped,
            missing=len(missing_pairs),
        )
    )

    summary = {
        "provider_calls_used": provider_calls_used,
        "raw_upserts": total_raw,
        "canon_upserts": total_canon,
        "raw_ok": total_status["OK"],
        "raw_missing": total_status["MISSING"],
        "raw_error": total_status["ERROR"],
        "max_ok_trade_date": max_ok_trade_date,
    }
    return 0, summary


def run_ingest(argv: list[str] | None = None) -> tuple[int, dict]:
    parser = argparse.ArgumentParser(description="Ingest TECH100 EOD prices")
    parser.add_argument("--date", help="single trade date (YYYY-MM-DD)")
    parser.add_argument("--start", help="start date inclusive (YYYY-MM-DD)")
    parser.add_argument("--end", help="end date inclusive (YYYY-MM-DD)")
    parser.add_argument("--backfill", action="store_true", help="run in backfill mode")
    parser.add_argument(
        "--backfill-missing",
        action="store_true",
        help="backfill only missing (ticker, date) pairs in the requested range",
    )
    parser.add_argument("--tickers", help="comma-separated ticker list override")
    parser.add_argument("--debug", action="store_true", help="print ingest diagnostics")
    parser.add_argument(
        "--max-provider-calls",
        type=int,
        default=None,
        help="Maximum Twelve Data requests to issue in backfill mode.",
    )

    args = parser.parse_args(argv)

    if args.backfill_missing:
        return _run_backfill_missing(args)
    if args.backfill:
        return _run_backfill(args)
    return _run_single(args)


def main(argv: list[str] | None = None) -> int:
    exit_code, _summary = run_ingest(argv)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
