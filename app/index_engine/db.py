"""Database helpers for SC_IDX price ingest."""
from __future__ import annotations

import datetime as _dt
from typing import Iterable, Mapping

from db_helper import get_connection


def fetch_constituent_tickers(trade_date: _dt.date) -> list[str]:
    """Return TECH100 tickers for the most recent rebalance on or before the date."""

    sql = (
        "SELECT DISTINCT ticker "
        "FROM tech11_ai_gov_eth_index "
        "WHERE ticker IS NOT NULL "
        "AND port_date = ("
        "  SELECT MAX(port_date) FROM tech11_ai_gov_eth_index WHERE port_date <= :trade_date"
        ")"
    )

    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, {"trade_date": trade_date})
        rows = cur.fetchall()
        return [str(row[0]).strip() for row in rows if row and row[0] is not None]


def upsert_prices_raw(rows: Iterable[Mapping]) -> int:
    """Upsert rows into SC_IDX_PRICES_RAW. Returns affected row count."""

    payload = list(rows)
    if not payload:
        return 0

    sql = (
        "MERGE INTO SC_IDX_PRICES_RAW dst "
        "USING (SELECT :ticker AS ticker, :trade_date AS trade_date, :provider AS provider FROM dual) src "
        "ON (dst.ticker = src.ticker AND dst.trade_date = src.trade_date AND dst.provider = src.provider) "
        "WHEN MATCHED THEN UPDATE SET "
        "  close_px = :close_px, "
        "  adj_close_px = :adj_close_px, "
        "  volume = :volume, "
        "  currency = :currency, "
        "  status = :status, "
        "  error_msg = :error_msg, "
        "  ingested_at = SYSTIMESTAMP "
        "WHEN NOT MATCHED THEN INSERT (ticker, trade_date, provider, close_px, adj_close_px, volume, currency, status, error_msg) "
        "VALUES (:ticker, :trade_date, :provider, :close_px, :adj_close_px, :volume, :currency, :status, :error_msg)"
    )

    binds = [
        {
            "ticker": row.get("ticker"),
            "trade_date": row.get("trade_date"),
            "provider": row.get("provider"),
            "close_px": row.get("close_px"),
            "adj_close_px": row.get("adj_close_px"),
            "volume": row.get("volume"),
            "currency": row.get("currency"),
            "status": row.get("status", "OK"),
            "error_msg": row.get("error_msg"),
        }
        for row in payload
    ]

    with get_connection() as conn:
        cur = conn.cursor()
        cur.executemany(sql, binds)
        conn.commit()
        return len(binds)


def upsert_prices_canon(rows: Iterable[Mapping]) -> int:
    """Upsert rows into SC_IDX_PRICES_CANON. Returns affected row count."""

    payload = list(rows)
    if not payload:
        return 0

    sql = (
        "MERGE INTO SC_IDX_PRICES_CANON dst "
        "USING (SELECT :ticker AS ticker, :trade_date AS trade_date FROM dual) src "
        "ON (dst.ticker = src.ticker AND dst.trade_date = src.trade_date) "
        "WHEN MATCHED THEN UPDATE SET "
        "  canon_close_px = :canon_close_px, "
        "  canon_adj_close_px = :canon_adj_close_px, "
        "  chosen_provider = :chosen_provider, "
        "  providers_ok = :providers_ok, "
        "  divergence_pct = :divergence_pct, "
        "  quality = :quality, "
        "  computed_at = SYSTIMESTAMP "
        "WHEN NOT MATCHED THEN INSERT (ticker, trade_date, canon_close_px, canon_adj_close_px, chosen_provider, providers_ok, divergence_pct, quality) "
        "VALUES (:ticker, :trade_date, :canon_close_px, :canon_adj_close_px, :chosen_provider, :providers_ok, :divergence_pct, :quality)"
    )

    binds = [
        {
            "ticker": row.get("ticker"),
            "trade_date": row.get("trade_date"),
            "canon_close_px": row.get("canon_close_px"),
            "canon_adj_close_px": row.get("canon_adj_close_px"),
            "chosen_provider": row.get("chosen_provider"),
            "providers_ok": row.get("providers_ok", 0),
            "divergence_pct": row.get("divergence_pct"),
            "quality": row.get("quality", "LOW"),
        }
        for row in payload
    ]

    with get_connection() as conn:
        cur = conn.cursor()
        cur.executemany(sql, binds)
        conn.commit()
        return len(binds)
