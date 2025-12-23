"""Database helpers for SC_IDX price ingest."""
from __future__ import annotations

import datetime as _dt
from typing import Iterable, Mapping, Optional, Sequence

from db_helper import get_connection


def fetch_constituent_tickers(trade_date: _dt.date) -> list[str]:
    """
    Return TECH100 tickers from the latest rebalance (matches /api/tech100 source).

    The trade_date argument is currently ignored; we always pull the most recent
    port_date that contains tickers and use the ranked basket (top 25).
    """

    sql = (
        "WITH latest AS ("
        "  SELECT MAX(port_date) AS port_date "
        "  FROM tech11_ai_gov_eth_index "
        "  WHERE ticker IS NOT NULL"
        ") "
        "SELECT ticker "
        "FROM tech11_ai_gov_eth_index "
        "WHERE ticker IS NOT NULL "
        "AND port_date = (SELECT port_date FROM latest) "
        "AND rank_index <= 25 "
        "GROUP BY ticker, rank_index "
        "ORDER BY rank_index"
    )

    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        tickers: list[str] = []
        for row in rows:
            if not row or row[0] is None:
                continue
            cleaned = str(row[0]).strip().upper()
            if cleaned:
                tickers.append(cleaned)
        return tickers


def get_current_user() -> Optional[str]:
    """Return the current Oracle user for verification."""

    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT USER FROM dual")
        row = cur.fetchone()
        if not row or row[0] is None:
            return None
        return str(row[0]).strip()


def fetch_distinct_tech100_tickers() -> list[str]:
    """Return all distinct TECH100 tickers (ignores date)."""

    sql = "SELECT DISTINCT ticker FROM tech11_ai_gov_eth_index WHERE ticker IS NOT NULL ORDER BY ticker"
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        tickers: list[str] = []
        for row in rows:
            if not row or row[0] is None:
                continue
            cleaned = str(row[0]).strip().upper()
            if cleaned:
                tickers.append(cleaned)
        return tickers


def fetch_port_date_for_trade_date(trade_date: _dt.date) -> Optional[_dt.date]:
    """Return the latest port_date <= trade_date."""

    sql = "SELECT MAX(port_date) FROM tech11_ai_gov_eth_index WHERE port_date <= :trade_date"
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, {"trade_date": trade_date})
        row = cur.fetchone()
        value = row[0] if row else None
        if value is None:
            return None
        if isinstance(value, _dt.datetime):
            return value.date()
        return value


def fetch_impacted_tickers_for_port_date(port_date: _dt.date) -> list[str]:
    """Return top 25 TECH100 tickers with PORT_WEIGHT > 0 for a port_date."""

    sql = (
        "SELECT ticker "
        "FROM tech11_ai_gov_eth_index "
        "WHERE port_date = :port_date "
        "AND port_weight > 0 "
        "AND ticker IS NOT NULL "
        "ORDER BY port_weight DESC, rank_index "
        "FETCH FIRST 25 ROWS ONLY"
    )
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, {"port_date": port_date})
        rows = cur.fetchall()
        tickers: list[str] = []
        for row in rows:
            if not row or row[0] is None:
                continue
            cleaned = str(row[0]).strip().upper()
            if cleaned:
                tickers.append(cleaned)
        return tickers


def fetch_impacted_tickers_for_trade_date(
    trade_date: _dt.date,
    *,
    cache: Optional[dict[_dt.date, list[str]]] = None,
) -> list[str]:
    """Return impacted tickers for a trade_date (top 25, PORT_WEIGHT > 0)."""

    port_date = fetch_port_date_for_trade_date(trade_date)
    if port_date is None:
        return []
    if cache is not None and port_date in cache:
        return cache[port_date]
    tickers = fetch_impacted_tickers_for_port_date(port_date)
    if cache is not None:
        cache[port_date] = tickers
    return tickers


def fetch_missing_real_for_trade_date(trade_date: _dt.date, impacted: Optional[list[str]] = None) -> list[str]:
    """
    Return impacted tickers lacking REAL canonical price (quality <> 'IMPUTED') on the given trade_date.
    """

    if impacted is None:
        impacted = fetch_impacted_tickers_for_trade_date(trade_date)
    if not impacted:
        return []

    values_clause = " UNION ALL ".join([f"SELECT :t{i} AS ticker FROM dual" for i in range(len(impacted))])
    sql = (
        f"WITH impacted AS ({values_clause}) "
        "SELECT i.ticker "
        "FROM impacted i "
        "WHERE NOT EXISTS ("
        "  SELECT 1 FROM sc_idx_prices_canon c "
        "  WHERE c.trade_date = :trade_date "
        "    AND c.ticker = i.ticker "
        "    AND NVL(c.canon_adj_close_px, c.canon_close_px) IS NOT NULL "
        "    AND c.quality <> 'IMPUTED'"
        ") "
        "ORDER BY i.ticker"
    )
    binds: dict[str, object] = {"trade_date": trade_date}
    for idx, ticker in enumerate(impacted):
        binds[f"t{idx}"] = ticker

    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, binds)
        rows = cur.fetchall()
        missing: list[str] = []
        for row in rows:
            if not row or row[0] is None:
                continue
            cleaned = str(row[0]).strip().upper()
            if cleaned:
                missing.append(cleaned)
    return missing


def fetch_missing_canon_pairs(
    start_date: _dt.date,
    end_date: _dt.date,
    tickers: Sequence[str],
    *,
    allow_imputed: bool = False,
) -> list[tuple[str, _dt.date]]:
    """Return (ticker, trade_date) pairs missing canonical prices in the window."""

    if not tickers:
        return []

    values_clause = " UNION ALL ".join([f"SELECT :t{i} AS ticker FROM dual" for i in range(len(tickers))])
    quality_clause = "" if allow_imputed else " OR c.quality = 'IMPUTED'"
    sql = (
        f"WITH tickers AS ({values_clause}) "
        "SELECT t.ticker, d.trade_date "
        "FROM tickers t "
        "CROSS JOIN ("
        "  SELECT trade_date FROM SC_IDX_TRADING_DAYS "
        "  WHERE trade_date BETWEEN :start_date AND :end_date"
        ") d "
        "LEFT JOIN SC_IDX_PRICES_CANON c "
        "  ON c.ticker = t.ticker AND c.trade_date = d.trade_date "
        "WHERE c.ticker IS NULL "
        "   OR NVL(c.canon_adj_close_px, c.canon_close_px) IS NULL "
        "   OR NVL(c.canon_adj_close_px, c.canon_close_px) <= 0"
        f"{quality_clause} "
        "ORDER BY t.ticker, d.trade_date"
    )
    binds: dict[str, object] = {"start_date": start_date, "end_date": end_date}
    for idx, ticker in enumerate(tickers):
        binds[f"t{idx}"] = str(ticker).strip().upper()

    missing: list[tuple[str, _dt.date]] = []
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, binds)
        rows = cur.fetchall()
        for row in rows:
            if not row:
                continue
            ticker = str(row[0]).strip().upper() if row[0] else None
            trade_date = row[1]
            if isinstance(trade_date, _dt.datetime):
                trade_date = trade_date.date()
            if ticker and isinstance(trade_date, _dt.date):
                missing.append((ticker, trade_date))
    return missing


def fetch_max_ok_trade_date(ticker: str, provider: str) -> Optional[_dt.date]:
    """Return the latest trade_date with status OK for the ticker/provider."""

    sql = (
        "SELECT MAX(trade_date) "
        "FROM SC_IDX_PRICES_RAW "
        "WHERE ticker = :ticker AND provider = :provider AND status = 'OK'"
    )
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, {"ticker": ticker, "provider": provider})
        row = cur.fetchone()
        value = row[0] if row else None
        if value is None:
            return None
        if isinstance(value, _dt.datetime):
            return value.date()
        return value


def fetch_trading_days(start: _dt.date, end: _dt.date) -> list[_dt.date]:
    """Return trading days between start and end (inclusive)."""

    sql = (
        "SELECT trade_date "
        "FROM SC_IDX_TRADING_DAYS "
        "WHERE trade_date BETWEEN :start_date AND :end_date "
        "ORDER BY trade_date"
    )
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, {"start_date": start, "end_date": end})
        rows = cur.fetchall()
        trading_days: list[_dt.date] = []
        for (trade_date,) in rows:
            if isinstance(trade_date, _dt.datetime):
                trade_date = trade_date.date()
            if isinstance(trade_date, _dt.date):
                trading_days.append(trade_date)
        return trading_days


def fetch_latest_trading_day() -> Optional[_dt.date]:
    """Return the latest trading day in the calendar."""

    sql = "SELECT MAX(trade_date) FROM SC_IDX_TRADING_DAYS"
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        value = row[0] if row else None
        if value is None:
            return None
        if isinstance(value, _dt.datetime):
            return value.date()
        return value


def fetch_latest_trading_day_on_or_before(target_date: _dt.date) -> Optional[_dt.date]:
    """Return the latest trading day on or before target_date."""

    sql = "SELECT MAX(trade_date) FROM SC_IDX_TRADING_DAYS WHERE trade_date <= :target_date"
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, {"target_date": target_date})
        row = cur.fetchone()
        value = row[0] if row else None
        if value is None:
            return None
        if isinstance(value, _dt.datetime):
            return value.date()
        return value


def upsert_trading_days(trading_days: Sequence[_dt.date], source: str) -> int:
    """Upsert trading days into SC_IDX_TRADING_DAYS. Returns affected count."""

    if not trading_days:
        return 0

    sql = (
        "MERGE INTO SC_IDX_TRADING_DAYS dst "
        "USING (SELECT :trade_date AS trade_date FROM dual) src "
        "ON (dst.trade_date = src.trade_date) "
        "WHEN MATCHED THEN UPDATE SET "
        "  source = :source "
        "WHEN NOT MATCHED THEN INSERT (trade_date, source) "
        "VALUES (:trade_date, :source)"
    )
    binds = [{"trade_date": day, "source": source} for day in trading_days]

    with get_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute("ALTER SESSION DISABLE PARALLEL DML")
        except Exception:
            pass
        cur.executemany(sql, binds)
        conn.commit()
        return len(binds)


def fetch_imputed_rows(start: _dt.date, end: _dt.date) -> list[tuple[str, _dt.date]]:
    """Return imputed canonical rows within the date range."""

    sql = (
        "SELECT ticker, trade_date "
        "FROM SC_IDX_PRICES_CANON "
        "WHERE quality = 'IMPUTED' "
        "AND trade_date BETWEEN :start_date AND :end_date"
    )
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, {"start_date": start, "end_date": end})
        rows = []
        for ticker, trade_date in cur.fetchall():
            if ticker is None or trade_date is None:
                continue
            if isinstance(trade_date, _dt.datetime):
                trade_date = trade_date.date()
            rows.append((str(ticker).strip().upper(), trade_date))
        return rows


def fetch_imputations(start: _dt.date, end: _dt.date) -> list[tuple[str, _dt.date]]:
    """Return imputation audit rows within the date range."""

    sql = (
        "SELECT ticker, trade_date "
        "FROM SC_IDX_IMPUTATIONS "
        "WHERE trade_date BETWEEN :start_date AND :end_date"
    )
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, {"start_date": start, "end_date": end})
        rows = []
        for ticker, trade_date in cur.fetchall():
            if ticker is None or trade_date is None:
                continue
            if isinstance(trade_date, _dt.datetime):
                trade_date = trade_date.date()
            rows.append((str(ticker).strip().upper(), trade_date))
        return rows


def fetch_raw_ok_rows(tickers: list[str], start: _dt.date, end: _dt.date) -> list[dict]:
    """Return OK rows for the given tickers/date range (inclusive)."""

    cleaned = [t.strip().upper() for t in tickers if t and str(t).strip()]
    if not cleaned:
        return []

    placeholders = ",".join(f":ticker_{i}" for i in range(len(cleaned)))
    sql = (
        "SELECT ticker, trade_date, provider, close_px, adj_close_px, status "
        "FROM SC_IDX_PRICES_RAW "
        f"WHERE ticker IN ({placeholders}) "
        "AND trade_date BETWEEN :start AND :end "
        "AND status = 'OK'"
    )
    binds = {f"ticker_{i}": ticker for i, ticker in enumerate(cleaned)}
    binds.update({"start": start, "end": end})

    rows: list[dict] = []
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, binds)
        for row in cur.fetchall():
            if not row:
                continue
            trade_date = row[1]
            if isinstance(trade_date, _dt.datetime):
                trade_date = trade_date.date()
            rows.append(
                {
                    "ticker": str(row[0]).strip().upper() if row[0] else None,
                    "trade_date": trade_date,
                    "provider": row[2],
                    "close_px": row[3],
                    "adj_close_px": row[4],
                    "status": row[5],
                }
            )
    return rows


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
        try:
            cur.execute("ALTER SESSION DISABLE PARALLEL DML")
        except Exception:
            pass
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
        try:
            cur.execute("ALTER SESSION DISABLE PARALLEL DML")
        except Exception:
            pass
        cur.executemany(sql, binds)
        conn.commit()
        return len(binds)
