import argparse
import datetime as _dt
import importlib.util
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple


_FETCH_TIME_SERIES: Optional[Callable[[str, _dt.date, _dt.date], List[Dict[str, Any]]]] = None


DEFAULT_TICKERS = ("AAPL",)
DEFAULT_CALL_INTERVAL = float(os.getenv("SC_IDX_PROVIDER_INTERVAL_SECONDS", "7.5"))
DEFAULT_DATA_DIR = Path(__file__).resolve().parent / "data"


def _load_provider_module():
    module_path = Path(__file__).resolve().parents[2] / "app" / "providers" / "twelvedata.py"
    spec = importlib.util.spec_from_file_location("twelvedata_provider", module_path)
    if spec is None or spec.loader is None:
        raise ImportError("Unable to load Twelve Data provider module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[arg-type]
    return module


def _default_fetcher() -> Callable[[str, _dt.date, _dt.date], List[Dict[str, Any]]]:
    global _FETCH_TIME_SERIES
    if _FETCH_TIME_SERIES is not None:
        return _FETCH_TIME_SERIES

    try:
        from app.providers import twelvedata as _provider
    except Exception:
        _provider = _load_provider_module()

    _FETCH_TIME_SERIES = _provider.fetch_time_series
    return _FETCH_TIME_SERIES


def _parse_date(raw: str) -> _dt.date:
    try:
        return _dt.date.fromisoformat(raw)
    except Exception as exc:
        raise argparse.ArgumentTypeError(f"Invalid date {raw}") from exc


def _parse_tickers(raw: Optional[str]) -> List[str]:
    if not raw:
        return list(DEFAULT_TICKERS)
    return [item.strip().upper() for item in raw.split(",") if item.strip()]


def _record_trade_date(entry: Dict[str, Any]) -> Optional[_dt.date]:
    for key in ("trade_date", "datetime", "date"):
        raw = entry.get(key)
        if raw is None:
            continue
        text = str(raw)
        try:
            return _dt.date.fromisoformat(text)
        except ValueError:
            try:
                return _dt.datetime.fromisoformat(text.replace("Z", "+00:00")).date()
            except ValueError:
                continue
    return None


def _load_existing(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    records: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return records


def _write_records(path: Path, records: Sequence[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, separators=(",", ":")) + "\n")


def _latest_date(records: Iterable[Dict[str, Any]]) -> Optional[_dt.date]:
    latest: Optional[_dt.date] = None
    for record in records:
        trade_date = _record_trade_date(record)
        if trade_date and (latest is None or trade_date > latest):
            latest = trade_date
    return latest


def _merge_records(
    ticker: str,
    existing: List[Dict[str, Any]],
    new_values: List[Dict[str, Any]],
    start_date: _dt.date,
    end_date: _dt.date,
) -> Tuple[List[Dict[str, Any]], int]:
    merged: Dict[_dt.date, Dict[str, Any]] = {}
    for record in existing:
        trade_date = _record_trade_date(record)
        if trade_date:
            merged[trade_date] = record

    new_count = 0
    for entry in new_values:
        trade_date = _record_trade_date(entry)
        if trade_date is None:
            continue
        if trade_date < start_date or trade_date > end_date:
            continue

        record = dict(entry)
        record["ticker"] = ticker
        record["trade_date"] = trade_date.isoformat()
        if trade_date not in merged:
            new_count += 1
        merged[trade_date] = record

    ordered = [merged[date] for date in sorted(merged)]
    return ordered, new_count


def backfill_prices(
    tickers: Iterable[str],
    start_date: _dt.date,
    end_date: _dt.date,
    *,
    max_provider_calls: Optional[int] = None,
    fetcher: Optional[Callable[[str, _dt.date, _dt.date], List[Dict[str, Any]]]] = None,
    data_dir: Path = DEFAULT_DATA_DIR,
    sleep_seconds: Optional[float] = None,
) -> Dict[str, Any]:
    fetch_fn = fetcher or _default_fetcher()
    tickers_list = [ticker.strip().upper() for ticker in tickers if ticker.strip()]
    call_interval = DEFAULT_CALL_INTERVAL if sleep_seconds is None else sleep_seconds

    provider_calls_used = 0
    tickers_completed = 0
    records_written = 0

    for index, ticker in enumerate(tickers_list):
        if max_provider_calls is not None and provider_calls_used >= max_provider_calls:
            print(
                f"budget_stop: provider_calls_used={provider_calls_used} "
                f"max_provider_calls={max_provider_calls}"
            )
            break

        existing = _load_existing(data_dir / f"{ticker}.jsonl")
        latest_existing = _latest_date(existing)
        effective_start = start_date
        if latest_existing and latest_existing >= start_date:
            effective_start = latest_existing + _dt.timedelta(days=1)

        if effective_start > end_date:
            continue

        provider_calls_used += 1
        try:
            values = fetch_fn(ticker, effective_start, end_date)
        except Exception as exc:  # pragma: no cover - transports provider errors
            print(f"error fetching {ticker}: {exc}", file=sys.stderr)
            continue

        merged, new_count = _merge_records(ticker, existing, values, effective_start, end_date)
        if new_count:
            _write_records(data_dir / f"{ticker}.jsonl", merged)
            records_written += new_count

        tickers_completed += 1

        if call_interval > 0 and index < len(tickers_list) - 1:
            time.sleep(call_interval)

    return {
        "provider_calls_used": provider_calls_used,
        "tickers_requested": len(tickers_list),
        "tickers_completed": tickers_completed,
        "records_written": records_written,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backfill prices from Twelve Data.")
    parser.add_argument("--backfill", action="store_true", help="Run a backfill between start/end dates.")
    parser.add_argument("--start", type=_parse_date, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=_parse_date, help="End date (YYYY-MM-DD)")
    parser.add_argument(
        "--tickers",
        type=str,
        help="Comma-separated list of tickers. Defaults to SC_IDX_TICKERS or a minimal built-in list.",
    )
    parser.add_argument(
        "--max-provider-calls",
        type=int,
        default=None,
        help="Maximum Twelve Data requests to issue this run.",
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default=str(DEFAULT_DATA_DIR),
        help="Directory for cached price files.",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if not args.backfill:
        parser.error("Only backfill mode is supported at this time.")

    if not args.start or not args.end:
        parser.error("--start and --end are required for backfill")

    tickers = _parse_tickers(args.tickers or os.getenv("SC_IDX_TICKERS"))
    summary = backfill_prices(
        tickers,
        args.start,
        args.end,
        max_provider_calls=args.max_provider_calls,
        data_dir=Path(args.data_dir),
    )

    print(
        f"ingest_complete: tickers={summary['tickers_completed']}/"
        f"{summary['tickers_requested']} provider_calls_used={summary['provider_calls_used']} "
        f"records_written={summary['records_written']}"
    )
    return 0
"""Ingest EOD prices for TECH100 constituents."""
from __future__ import annotations

import argparse
import datetime as _dt
import http.client  # preload stdlib http to avoid app.http shadowing
import pathlib
import sys
from collections import defaultdict
from typing import Iterable, List, Optional
from typing import Optional

APP_PATH = pathlib.Path(__file__).resolve().parents[2] / "app"
if str(APP_PATH) not in sys.path:
    sys.path.insert(0, str(APP_PATH))

from index_engine.db import (
    fetch_distinct_tech100_tickers,
    fetch_max_ok_trade_date,
    fetch_constituent_tickers,
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
    tickers: list[str] = []
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
        raw_rows.append(
            {
                "ticker": ticker,
                "trade_date": trade_date,
                "provider": PROVIDER,
                "close_px": row.get("close"),
                "adj_close_px": row.get("adj_close") if row.get("adj_close") is not None else row.get("close"),
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


def _print_summary(raw_rows: list[dict], canon_rows: list[dict]) -> None:
    status_counts = {"OK": 0, "MISSING": 0, "ERROR": 0}
    for row in raw_rows:
        status = row.get("status")
        if status in status_counts:
            status_counts[status] += 1
    print(
        "rows_ok={ok} rows_missing={missing} rows_error={error} canon_written={canon}".format(
            ok=status_counts["OK"],
            missing=status_counts["MISSING"],
            error=status_counts["ERROR"],
            canon=len(canon_rows),
        )
    )


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

    print(
        "debug: provider_called={called} provider_rows={provider_rows} raw_rows={raw_rows} canon_rows={canon_rows}".format(
            called=provider_called,
            provider_rows=len(provider_rows),
            raw_rows=len(raw_rows),
            canon_rows=len(canon_rows),
        )
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Ingest TECH100 EOD prices")
    parser.add_argument("--date", help="single trade date (YYYY-MM-DD)")
    parser.add_argument("--start", help="start date inclusive (YYYY-MM-DD)")
    parser.add_argument("--end", help="end date inclusive (YYYY-MM-DD)")
    parser.add_argument("--tickers", help="comma-separated ticker override")
    parser.add_argument("--debug", action="store_true", help="print ingest diagnostics")

    args = parser.parse_args(argv)

    manual_tickers = _split_tickers(args.tickers)

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
        for trade_date in dates:
            tickers_by_date[trade_date] = manual_tickers or fetch_constituent_tickers(trade_date)
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
    return 0


def _run_backfill(args: argparse.Namespace) -> int:
    if not args.start or not args.end:
        print("Backfill mode requires --start and --end")
        return 1

    start_date = _parse_date(args.start)
    end_date = _parse_date(args.end)
    if end_date < start_date:
        print("Invalid date range: end must be on or after start")
        return 1

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
            return 1

    total_raw = 0
    total_canon = 0
    provider_error: str | None = None

    for ticker in tickers:
        provider_error = None
        try:
            max_ok = fetch_max_ok_trade_date(ticker, PROVIDER)
        except Exception as exc:
            print(f"Failed to probe max date for {ticker}: {exc}")
            continue

        ticker_start = start_date
        if max_ok:
            if max_ok >= end_date:
                continue
            ticker_start = max(max_ok + _dt.timedelta(days=1), start_date)

        try:
            provider_rows = fetch_eod_prices(
                [ticker],
                ticker_start.isoformat(),
                end_date.isoformat(),
            )
        except Exception as exc:
            provider_error = str(exc)
            print(f"Provider fetch failed for {ticker}: {exc}")
            continue

        raw_rows = _build_raw_rows_from_provider(provider_rows)
        if raw_rows:
            total_raw += upsert_prices_raw(raw_rows)
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
            )

    print(
        "backfill_summary: raw_upserts={raw} canon_upserts={canon}".format(
            raw=total_raw, canon=total_canon
        )
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Ingest TECH100 EOD prices")
    parser.add_argument("--date", help="single trade date (YYYY-MM-DD)")
    parser.add_argument("--start", help="start date inclusive (YYYY-MM-DD)")
    parser.add_argument("--end", help="end date inclusive (YYYY-MM-DD)")
    parser.add_argument("--backfill", action="store_true", help="run in backfill mode")
    parser.add_argument("--tickers", help="comma-separated ticker list override")
    parser.add_argument("--debug", action="store_true", help="print ingest diagnostics")

    args = parser.parse_args(argv)

    if args.backfill:
        return _run_backfill(args)
    return _run_single(args)


if __name__ == "__main__":
    sys.exit(main())
