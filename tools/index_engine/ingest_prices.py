"""Ingest EOD prices for TECH100 constituents."""
from __future__ import annotations

import argparse
import datetime as _dt
import http.client  # preload stdlib http to avoid app.http shadowing
import pathlib
import sys
from collections import defaultdict
from typing import Iterable

APP_PATH = pathlib.Path(__file__).resolve().parents[2] / "app"
if str(APP_PATH) not in sys.path:
    sys.path.insert(0, str(APP_PATH))

from index_engine.db import fetch_constituent_tickers, upsert_prices_canon, upsert_prices_raw
from index_engine.reconcile import reconcile_canonical
from providers.twelvedata import fetch_eod_prices

PROVIDER = "TWELVEDATA"


def _parse_dates(args: argparse.Namespace) -> list[_dt.date]:
    if args.date:
        return [_dt.date.fromisoformat(args.date)]
    if args.start and args.end:
        start = _dt.date.fromisoformat(args.start)
        end = _dt.date.fromisoformat(args.end)
        if end < start:
            raise ValueError("end date must be on or after start date")
        days = (end - start).days + 1
        return [start + _dt.timedelta(days=i) for i in range(days)]
    raise ValueError("Provide --date or both --start and --end")


def _collect_constituents(dates: Iterable[_dt.date]) -> dict[_dt.date, list[str]]:
    tickers_by_date: dict[_dt.date, list[str]] = {}
    for trade_date in dates:
        tickers_by_date[trade_date] = fetch_constituent_tickers(trade_date)
    return tickers_by_date


def _build_raw_rows(
    tickers_by_date: dict[_dt.date, list[str]],
    provider_rows: list[dict],
    provider_error: str | None = None,
) -> list[dict]:
    provider_map = {}
    for row in provider_rows:
        try:
            dt = _dt.date.fromisoformat(str(row.get("trade_date")))
        except Exception:
            continue
        ticker_key = str(row.get("ticker") or "").upper()
        provider_map[(ticker_key, dt)] = row

    raw_rows: list[dict] = []
    for trade_date, tickers in tickers_by_date.items():
        for ticker in tickers:
            upper = ticker.upper()
            row = provider_map.get((upper, trade_date))
            if provider_error:
                status = "ERROR"
                error_msg = provider_error
                close_px = None
                adj_close_px = None
                volume = None
                currency = None
            elif row:
                status = "OK"
                error_msg = None
                close_px = row.get("close")
                adj_close_px = row.get("adj_close")
                volume = row.get("volume")
                currency = row.get("currency")
            else:
                status = "MISSING"
                error_msg = "not returned by provider"
                close_px = None
                adj_close_px = None
                volume = None
                currency = None

            raw_rows.append(
                {
                    "ticker": upper,
                    "trade_date": trade_date,
                    "provider": PROVIDER,
                    "close_px": close_px,
                    "adj_close_px": adj_close_px,
                    "volume": volume,
                    "currency": currency,
                    "status": status,
                    "error_msg": error_msg,
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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Ingest TECH100 EOD prices")
    parser.add_argument("--date", help="single trade date (YYYY-MM-DD)")
    parser.add_argument("--start", help="start date inclusive (YYYY-MM-DD)")
    parser.add_argument("--end", help="end date inclusive (YYYY-MM-DD)")

    args = parser.parse_args(argv)

    try:
        dates = _parse_dates(args)
    except Exception as exc:
        print(f"Invalid date arguments: {exc}")
        return 1

    try:
        tickers_by_date = _collect_constituents(dates)
    except Exception as exc:
        print(f"Failed to load constituents: {exc}")
        return 1

    all_tickers = sorted({t for vals in tickers_by_date.values() for t in vals})
    provider_rows: list[dict] = []
    provider_error: str | None = None

    if all_tickers:
        try:
            provider_rows = fetch_eod_prices(
                all_tickers,
                dates[0].isoformat(),
                dates[-1].isoformat(),
            )
        except Exception as exc:
            provider_error = str(exc)
            print(f"Provider fetch failed: {exc}")

    raw_rows = _build_raw_rows(tickers_by_date, provider_rows, provider_error)
    if raw_rows:
        upsert_prices_raw(raw_rows)

    canon_rows = compute_canonical_rows(raw_rows)
    if canon_rows:
        upsert_prices_canon(canon_rows)

    _print_summary(raw_rows, canon_rows)
    return 0


if __name__ == "__main__":
    sys.exit(main())
