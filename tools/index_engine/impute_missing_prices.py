from __future__ import annotations

import argparse
import datetime as _dt
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Iterable

REPO_ROOT = Path(__file__).resolve().parents[2]
APP_ROOT = REPO_ROOT / "app"
for path in (REPO_ROOT, APP_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from tools.index_engine.env_loader import load_default_env

from index_engine.alerts import send_email
from index_engine.alert_state import should_send_alert_once_per_day
from index_engine.data_quality import find_previous_available, format_imputation_alert
from index_engine.db import fetch_impacted_tickers_for_trade_date, fetch_trading_days
from db_helper import get_connection

INDEX_CODE = "TECH100"
REASON = "MISSING_PROVIDER_DATA"


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


def select_replacement_tickers(
    imputed_rows: list[tuple[str, _dt.date]],
    impacted_by_date: dict[_dt.date, list[str]],
    limit: int,
) -> list[str]:
    """Select unique tickers to refresh for replacing imputed rows."""
    tickers: list[str] = []
    seen = set()
    for ticker, trade_date in imputed_rows:
        impacted = impacted_by_date.get(trade_date, [])
        if ticker not in impacted:
            continue
        if ticker in seen:
            continue
        seen.add(ticker)
        tickers.append(ticker)
        if len(tickers) >= limit:
            break
    return tickers


def _fetch_canon_prices(
    start: _dt.date,
    end: _dt.date,
    *,
    allow_canon_close: bool,
) -> tuple[dict[str, set[_dt.date]], dict[tuple[str, _dt.date], float]]:
    sql = (
        "SELECT ticker, trade_date, canon_adj_close_px, canon_close_px "
        "FROM SC_IDX_PRICES_CANON "
        "WHERE trade_date BETWEEN :start_date AND :end_date"
    )
    binds = {"start_date": start, "end_date": end}

    available_by_ticker: dict[str, set[_dt.date]] = defaultdict(set)
    price_by_key: dict[tuple[str, _dt.date], float] = {}
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, binds)
        for ticker, trade_date, adj_close, close_px in cur.fetchall():
            if ticker is None or trade_date is None:
                continue
            if isinstance(trade_date, _dt.datetime):
                trade_date = trade_date.date()
            price = adj_close
            if price is None and allow_canon_close:
                price = close_px
            if price is None:
                continue
            key = (str(ticker).strip().upper(), trade_date)
            available_by_ticker[key[0]].add(trade_date)
            price_by_key[key] = float(price)
    return available_by_ticker, price_by_key


def _upsert_imputations(rows: list[dict]) -> int:
    if not rows:
        return 0
    sql = (
        "MERGE INTO SC_IDX_IMPUTATIONS dst "
        "USING (SELECT :index_code AS index_code, :trade_date AS trade_date, :ticker AS ticker FROM dual) src "
        "ON (dst.index_code = src.index_code AND dst.trade_date = src.trade_date AND dst.ticker = src.ticker) "
        "WHEN MATCHED THEN UPDATE SET "
        "  imputed_from_date = :imputed_from_date, "
        "  imputed_price = :imputed_price, "
        "  reason = :reason "
        "WHEN NOT MATCHED THEN INSERT "
        "  (index_code, trade_date, ticker, imputed_from_date, imputed_price, reason) "
        "VALUES (:index_code, :trade_date, :ticker, :imputed_from_date, :imputed_price, :reason)"
    )
    with get_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute("ALTER SESSION DISABLE PARALLEL DML")
        except Exception:
            pass
        cur.executemany(sql, rows)
        conn.commit()
        return len(rows)


def _upsert_canon_imputed(rows: list[dict]) -> int:
    if not rows:
        return 0
    sql = (
        "MERGE INTO SC_IDX_PRICES_CANON dst "
        "USING (SELECT :ticker AS ticker, :trade_date AS trade_date FROM dual) src "
        "ON (dst.ticker = src.ticker AND dst.trade_date = src.trade_date) "
        "WHEN MATCHED THEN UPDATE SET "
        "  canon_close_px = :price, "
        "  canon_adj_close_px = :price, "
        "  chosen_provider = :provider, "
        "  providers_ok = :providers_ok, "
        "  quality = :quality, "
        "  computed_at = SYSTIMESTAMP "
        "  WHERE dst.canon_close_px IS NULL AND dst.canon_adj_close_px IS NULL "
        "WHEN NOT MATCHED THEN INSERT "
        "  (ticker, trade_date, canon_close_px, canon_adj_close_px, chosen_provider, providers_ok, quality) "
        "VALUES (:ticker, :trade_date, :price, :price, :provider, :providers_ok, :quality)"
    )
    with get_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute("ALTER SESSION DISABLE PARALLEL DML")
        except Exception:
            pass
        cur.executemany(sql, rows)
        conn.commit()
        return len(rows)


def impute_missing_prices(
    *,
    start_date: _dt.date,
    end_date: _dt.date,
    allow_canon_close: bool = True,
    email_on_impute: bool = False,
) -> dict[str, object]:
    load_default_env()

    trading_days = fetch_trading_days(start_date, end_date)
    if not trading_days:
        raise RuntimeError("no_trading_days_found")

    available_by_ticker, price_by_key = _fetch_canon_prices(
        start_date,
        end_date,
        allow_canon_close=allow_canon_close,
    )

    imputations: list[dict] = []
    canon_rows: list[dict] = []
    missing_without_prior: list[tuple[str, _dt.date]] = []
    per_date_counts: Counter[_dt.date] = Counter()
    per_ticker_counts: Counter[str] = Counter()

    impacted_cache: dict[_dt.date, list[str]] = {}
    for trade_date in trading_days:
        tickers = fetch_impacted_tickers_for_trade_date(trade_date, cache=impacted_cache)
        for ticker in tickers:
            available_dates = available_by_ticker.get(ticker, set())
            if trade_date in available_dates:
                continue
            prior_date = find_previous_available(trading_days, trade_date, available_dates)
            if prior_date is None:
                missing_without_prior.append((ticker, trade_date))
                continue
            price = price_by_key.get((ticker, prior_date))
            if price is None:
                missing_without_prior.append((ticker, trade_date))
                continue
            imputations.append(
                {
                    "index_code": INDEX_CODE,
                    "trade_date": trade_date,
                    "ticker": ticker,
                    "imputed_from_date": prior_date,
                    "imputed_price": price,
                    "reason": REASON,
                }
            )
            canon_rows.append(
                {
                    "ticker": ticker,
                    "trade_date": trade_date,
                    "price": price,
                    "provider": "IMPUTED",
                    "providers_ok": 0,
                    "quality": "IMPUTED",
                }
            )
            per_date_counts[trade_date] += 1
            per_ticker_counts[ticker] += 1

    imputed_count = _upsert_imputations(imputations)
    canon_imputed = _upsert_canon_imputed(canon_rows)

    per_date = sorted(per_date_counts.items(), key=lambda item: (-item[1], item[0]))[:10]
    top_tickers = sorted(per_ticker_counts.items(), key=lambda item: (-item[1], item[0]))[:10]

    summary = {
        "total_imputed": imputed_count,
        "canon_imputed": canon_imputed,
        "missing_without_prior": len(missing_without_prior),
        "per_date": per_date,
        "top_tickers": top_tickers,
        "missing_without_prior_rows": missing_without_prior,
    }

    if (imputed_count > 0 or missing_without_prior) and email_on_impute:
        body = format_imputation_alert(
            date_range=(start_date, end_date),
            total_imputed=imputed_count,
            total_missing_without_prior=len(missing_without_prior),
            per_date_counts=per_date,
            top_tickers=top_tickers,
        )
        if should_send_alert_once_per_day("sc_idx_imputation", detail=body, status="OK"):
            send_email(
                f"SC_IDX imputations detected (date range {start_date.isoformat()}..{end_date.isoformat()})",
                body,
            )

    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Impute missing TECH100 prices using carry-forward.")
    parser.add_argument("--start", help="Start date YYYY-MM-DD")
    parser.add_argument("--end", help="End date YYYY-MM-DD or 'today' (default: today)")
    parser.add_argument("--since-base", action="store_true", help="Use base date 2025-01-02 as start")
    parser.add_argument("--allow-canon-close", action="store_true")
    parser.add_argument("--email-on-impute", action="store_true")
    parser.add_argument("--email", action="store_true", help="Alias for --email-on-impute")
    args = parser.parse_args()

    if args.since_base:
        start_date = _dt.date(2025, 1, 2)
    elif args.start:
        start_date = _parse_date(args.start)
    else:
        raise ValueError("Provide --start or --since-base")

    end_value = args.end or "today"
    end_date = _parse_date(end_value)
    if end_date < start_date:
        raise ValueError("end date must be on or after start date")

    email_on_impute = args.email_on_impute or args.email
    summary = impute_missing_prices(
        start_date=start_date,
        end_date=end_date,
        allow_canon_close=args.allow_canon_close,
        email_on_impute=email_on_impute,
    )

    print(
        "imputed={imputed} canon_imputed={canon} missing_without_prior={missing}".format(
            imputed=summary.get("total_imputed"),
            canon=summary.get("canon_imputed"),
            missing=summary.get("missing_without_prior"),
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
