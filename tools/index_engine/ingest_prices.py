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
APP_PATH = REPO_ROOT / "app"
for path in (REPO_ROOT, APP_PATH):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from tools.oracle.env_bootstrap import load_env_files

from index_engine.db import (
    fetch_constituent_tickers,
    fetch_distinct_tech100_tickers,
    fetch_impacted_tickers_for_trade_date,
    fetch_max_ok_trade_date,
    fetch_trading_days,
    get_current_user,
    upsert_prices_canon,
    upsert_prices_raw,
)
from index_engine.reconcile import reconcile_canonical
from providers.market_data_provider import fetch_eod_prices, fetch_single_day_bar

PROVIDER = "MARKET_DATA"


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


def _build_raw_rows_from_provider(provider_rows: list[dict]) -> list[dict]:
    raw_rows: list[dict] = []
    for row in provider_rows:
        try:
            trade_date = _dt.date.fromisoformat(str(row.get("trade_date")))
        except Exception:
            continue
        ticker = str(row.get("ticker") or "").upper()
        if not ticker:
            continue
        close_px = row.get("close")
        adj_close = row.get("adj_close") if row.get("adj_close") is not None else row.get("close")
        if close_px is None or close_px <= 0 or adj_close is None or adj_close <= 0:
            continue

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
    return raw_rows


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
            if adj_value is None:
                adj_value = entry.get("close_px")
            provider_adj_closes[provider] = adj_value
            provider_closes[provider] = entry.get("close_px")

        recon = reconcile_canonical(provider_adj_closes, provider_closes)
        if recon.get("providers_ok", 0) == 0:
            continue
        canon_close = recon.get("canon_close")
        canon_adj_close = recon.get("canon_adj_close")
        if canon_close is None or canon_adj_close is None:
            continue
        if canon_close <= 0 or canon_adj_close <= 0:
            continue
        canon_rows.append(
            {
                "ticker": ticker,
                "trade_date": trade_date,
                "canon_close_px": canon_close,
                "canon_adj_close_px": canon_adj_close,
                "chosen_provider": recon.get("chosen_provider") or PROVIDER,
                "providers_ok": recon.get("providers_ok", 0),
                "divergence_pct": recon.get("divergence_pct"),
                "quality": recon.get("quality", "LOW"),
            }
        )
    return canon_rows


def _print_summary(raw_rows: list[dict], canon_rows: list[dict], provider_calls_used: int | None = None) -> None:
    status_counts = {"OK": 0, "MISSING": 0, "ERROR": 0}
    for row in raw_rows:
        status = row.get("status")
        if status in status_counts:
            status_counts[status] += 1
    summary = (
        "rows_ok={ok} rows_missing={missing} rows_error={error} canon_written={canon}".format(
            ok=status_counts["OK"],
            missing=status_counts["MISSING"],
            error=status_counts["ERROR"],
            canon=len(canon_rows),
        )
    )
    if provider_calls_used is not None:
        summary = f"{summary} provider_calls_used={provider_calls_used}"
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


def _next_trading_day(trading_days: list[_dt.date], start: _dt.date) -> _dt.date | None:
    for day in trading_days:
        if day >= start:
            return day
    return None


def _build_missing_rows(
    *,
    ticker: str,
    trading_days: list[_dt.date],
    start_date: _dt.date,
    end_date: _dt.date,
    reason: str,
) -> list[dict]:
    missing_rows: list[dict] = []
    for trade_date in trading_days:
        if trade_date < start_date or trade_date > end_date:
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
                "error_msg": reason,
            }
        )
    return missing_rows


def _fetch_provider_rows(ticker: str, start_date: _dt.date, end_date: _dt.date) -> list[dict]:
    try:
        return fetch_eod_prices([ticker], start_date.isoformat(), end_date.isoformat())
    except Exception as exc:
        if start_date == end_date:
            try:
                return fetch_single_day_bar(ticker, start_date)
            except Exception:
                raise exc
        raise


def _run_single(args: argparse.Namespace) -> int:
    try:
        dates = _parse_dates(args)
    except Exception as exc:
        print(f"Invalid date arguments: {exc}")
        return 1

    tickers_by_date: dict[_dt.date, list[str]] = {}
    manual_tickers = _split_tickers(args.tickers)
    try:
        tickers_by_date = _collect_constituents(dates, manual_tickers)
    except Exception as exc:
        print(f"Failed to load constituents: {exc}")
        return 1

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

    if provider_called and not provider_rows and provider_error is None:
        print("Provider returned empty payload; aborting ingest.")
        provider_error = "empty_payload"

    raw_rows = _build_raw_rows_from_provider(provider_rows)
    if raw_rows:
        upsert_prices_raw(raw_rows)

    canon_rows = compute_canonical_rows(raw_rows)
    if canon_rows:
        upsert_prices_canon(canon_rows)

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

    _print_summary(raw_rows, canon_rows)
    return 1 if provider_error else 0


def _run_backfill(args: argparse.Namespace) -> tuple[int, dict]:
    if not args.start or not args.end:
        print("Backfill mode requires --start and --end")
        return 1, {}

    start_date = _parse_date(args.start)
    end_date = _parse_date(args.end)
    if end_date < start_date:
        print("Invalid date range: end must be on or after start")
        return 1, {}

    trading_days = fetch_trading_days(start_date, end_date)
    if not trading_days:
        print("No trading days found for requested range")
        return 1, {}
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
        impacted: set[str] = set()
        for day in trading_days:
            impacted.update(fetch_impacted_tickers_for_trade_date(day))
        tickers = sorted(impacted)
        if not tickers:
            try:
                tickers = fetch_distinct_tech100_tickers()
            except Exception as exc:
                print(f"Failed to load tickers: {exc}")
                return 1, {}
    print(
        "backfill_window: start={start} end={end} trading_days={days} tickers={tickers}".format(
            start=start_date.isoformat(),
            end=last_trading_day.isoformat(),
            days=len(trading_days),
            tickers=len(tickers),
        )
    )

    total_raw = 0
    total_canon = 0
    total_status = {"OK": 0, "MISSING": 0, "ERROR": 0}
    provider_error: str | None = None
    max_ok_trade_date: _dt.date | None = None

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
        ticker_start = _next_trading_day(trading_days, ticker_start) or ticker_start
        if ticker_start > last_trading_day:
            continue

        provider_calls_used += 1
        try:
            provider_rows = _fetch_provider_rows(ticker, ticker_start, last_trading_day)
        except Exception as exc:
            provider_error = str(exc)
            print(
                "Provider fetch failed for {ticker} range={start}..{end}: {exc}".format(
                    ticker=ticker,
                    start=ticker_start.isoformat(),
                    end=last_trading_day.isoformat(),
                    exc=exc,
                )
            )
            missing_rows = _build_missing_rows(
                ticker=ticker,
                trading_days=trading_days,
                start_date=ticker_start,
                end_date=last_trading_day,
                reason="provider_error",
            )
            if missing_rows:
                total_raw += upsert_prices_raw(missing_rows)
                total_status["MISSING"] += len(missing_rows)
            continue

        if not provider_rows:
            print(
                "Provider returned empty payload for {ticker} range={start}..{end}".format(
                    ticker=ticker,
                    start=ticker_start.isoformat(),
                    end=last_trading_day.isoformat(),
                )
            )
            missing_rows = _build_missing_rows(
                ticker=ticker,
                trading_days=trading_days,
                start_date=ticker_start,
                end_date=last_trading_day,
                reason="empty_payload",
            )
            if missing_rows:
                total_raw += upsert_prices_raw(missing_rows)
                total_status["MISSING"] += len(missing_rows)
            continue

        raw_rows = _build_raw_rows_from_provider(provider_rows)
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
        "backfill_summary: raw_upserts={raw} canon_upserts={canon} provider_calls_used={calls}".format(
            raw=total_raw, canon=total_canon, calls=provider_calls_used
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


def _ranges_from_missing(trading_days: list[_dt.date], missing: set[_dt.date]) -> list[tuple[_dt.date, _dt.date]]:
    ranges: list[tuple[_dt.date, _dt.date]] = []
    start: _dt.date | None = None
    last: _dt.date | None = None
    for day in trading_days:
        if day in missing:
            if start is None:
                start = day
            last = day
            continue
        if start is not None and last is not None:
            ranges.append((start, last))
        start = None
        last = None
    if start is not None and last is not None:
        ranges.append((start, last))
    return ranges


def _fetch_existing_ok(
    *,
    start_date: _dt.date,
    end_date: _dt.date,
    tickers: list[str],
) -> set[tuple[str, _dt.date]]:
    if not tickers:
        return set()
    placeholders = ",".join(f":t{i}" for i in range(len(tickers)))
    sql = (
        "SELECT ticker, trade_date "
        "FROM sc_idx_prices_raw "
        f"WHERE provider = '{PROVIDER}' AND status = 'OK' "
        "AND trade_date BETWEEN :start_date AND :end_date "
        f"AND ticker IN ({placeholders})"
    )
    binds = {"start_date": start_date, "end_date": end_date}
    binds.update({f"t{i}": t for i, t in enumerate(tickers)})
    existing: set[tuple[str, _dt.date]] = set()
    from db_helper import get_connection
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, binds)
        for ticker, trade_date in cur.fetchall():
            if not ticker or trade_date is None:
                continue
            if isinstance(trade_date, _dt.datetime):
                trade_date = trade_date.date()
            existing.add((str(ticker).strip().upper(), trade_date))
    return existing


def _run_backfill_missing(args: argparse.Namespace) -> tuple[int, dict]:
    if not args.start or not args.end:
        print("Backfill-missing mode requires --start and --end")
        return 1, {}

    start_date = _parse_date(args.start)
    end_date = _parse_date(args.end)
    if end_date < start_date:
        print("Invalid date range: end must be on or after start")
        return 1, {}

    trading_days = fetch_trading_days(start_date, end_date)
    if not trading_days:
        print("No trading days found for requested range")
        return 1, {}

    tickers = _split_tickers(args.tickers)
    impacted_by_date: dict[_dt.date, list[str]] = {}
    if not tickers:
        impacted: set[str] = set()
        for day in trading_days:
            impacted_list = fetch_impacted_tickers_for_trade_date(day)
            impacted_by_date[day] = impacted_list
            impacted.update(impacted_list)
        tickers = sorted(impacted)
        if not tickers:
            try:
                tickers = fetch_distinct_tech100_tickers()
            except Exception as exc:
                print(f"Failed to load tickers: {exc}")
                return 1, {}
    else:
        for day in trading_days:
            impacted_by_date[day] = tickers

    print(
        "backfill_missing_window: start={start} end={end} trading_days={days} tickers={tickers}".format(
            start=start_date.isoformat(),
            end=end_date.isoformat(),
            days=len(trading_days),
            tickers=len(tickers),
        )
    )

    existing_ok = _fetch_existing_ok(
        start_date=start_date,
        end_date=end_date,
        tickers=tickers,
    )

    max_provider_calls = args.max_provider_calls
    provider_calls_used = 0
    total_raw = 0
    total_canon = 0
    total_status = {"OK": 0, "MISSING": 0, "ERROR": 0}
    max_ok_trade_date: _dt.date | None = None

    missing_by_ticker: dict[str, set[_dt.date]] = {}
    for trade_date, impacted in impacted_by_date.items():
        for ticker in impacted:
            if (ticker, trade_date) in existing_ok:
                continue
            missing_by_ticker.setdefault(ticker, set()).add(trade_date)

    for ticker, missing_dates in missing_by_ticker.items():
        if not missing_dates:
            continue
        for start, end in _ranges_from_missing(trading_days, missing_dates):
            if max_provider_calls is not None and provider_calls_used >= max_provider_calls:
                print(
                    f"budget_stop: provider_calls_used={provider_calls_used} "
                    f"max_provider_calls={max_provider_calls}"
                )
                return 0, {
                    "provider_calls_used": provider_calls_used,
                    "raw_upserts": total_raw,
                    "canon_upserts": total_canon,
                    "raw_ok": total_status["OK"],
                    "raw_missing": total_status["MISSING"],
                    "raw_error": total_status["ERROR"],
                    "max_ok_trade_date": max_ok_trade_date,
                }

            provider_calls_used += 1
            try:
                provider_rows = _fetch_provider_rows(ticker, start, end)
            except Exception as exc:
                print(
                    "Provider fetch failed for {ticker} range={start}..{end}: {exc}".format(
                        ticker=ticker,
                        start=start.isoformat(),
                        end=end.isoformat(),
                        exc=exc,
                    )
                )
                missing_rows = _build_missing_rows(
                    ticker=ticker,
                    trading_days=trading_days,
                    start_date=start,
                    end_date=end,
                    reason="provider_error",
                )
                if missing_rows:
                    total_raw += upsert_prices_raw(missing_rows)
                    total_status["MISSING"] += len(missing_rows)
                continue

            if not provider_rows:
                print(
                    "Provider returned empty payload for {ticker} range={start}..{end}".format(
                        ticker=ticker,
                        start=start.isoformat(),
                        end=end.isoformat(),
                    )
                )
                missing_rows = _build_missing_rows(
                    ticker=ticker,
                    trading_days=trading_days,
                    start_date=start,
                    end_date=end,
                    reason="empty_payload",
                )
                if missing_rows:
                    total_raw += upsert_prices_raw(missing_rows)
                    total_status["MISSING"] += len(missing_rows)
                continue

            raw_rows = _build_raw_rows_from_provider(provider_rows)
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
        "backfill_missing_summary: raw_upserts={raw} canon_upserts={canon} provider_calls_used={calls}".format(
            raw=total_raw, canon=total_canon, calls=provider_calls_used
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


def main(argv: list[str] | None = None) -> int:
    load_env_files(
        paths=(
            "/etc/sustainacore/db.env",
            "/etc/sustainacore-ai/app.env",
            "/etc/sustainacore-ai/secrets.env",
        )
    )
    parser = argparse.ArgumentParser(description="Ingest TECH100 EOD prices")
    parser.add_argument("--date", help="single trade date (YYYY-MM-DD)")
    parser.add_argument("--start", help="start date inclusive (YYYY-MM-DD)")
    parser.add_argument("--end", help="end date inclusive (YYYY-MM-DD)")
    parser.add_argument("--backfill", action="store_true", help="run in backfill mode")
    parser.add_argument("--backfill-missing", action="store_true", help="backfill only missing ticker/dates")
    parser.add_argument("--tickers", help="comma-separated ticker list override")
    parser.add_argument("--debug", action="store_true", help="print ingest diagnostics")
    parser.add_argument(
        "--max-provider-calls",
        type=int,
        default=None,
        help="Maximum provider requests to issue in backfill mode.",
    )

    args = parser.parse_args(argv)

    if args.backfill:
        code, _summary = _run_backfill(args)
        return code
    if args.backfill_missing:
        code, _summary = _run_backfill_missing(args)
        return code
    return _run_single(args)


if __name__ == "__main__":
    sys.exit(main())
