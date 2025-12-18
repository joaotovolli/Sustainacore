"""Oracle helpers for TECH100 index calculation."""
from __future__ import annotations

import datetime as _dt
from typing import Iterable, Optional

from db_helper import get_connection

INDEX_CODE = "TECH100"


def _disable_parallel_dml(conn) -> None:
    try:
        cur = conn.cursor()
        cur.execute("ALTER SESSION DISABLE PARALLEL DML")
    except Exception:
        return


def fetch_trading_days(start: _dt.date, end: _dt.date) -> list[_dt.date]:
    sql = (
        "SELECT trade_date "
        "FROM SC_IDX_TRADING_DAYS "
        "WHERE trade_date BETWEEN :start_date AND :end_date "
        "ORDER BY trade_date"
    )
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, {"start_date": start, "end_date": end})
        rows = []
        for (trade_date,) in cur.fetchall():
            if isinstance(trade_date, _dt.datetime):
                trade_date = trade_date.date()
            if isinstance(trade_date, _dt.date):
                rows.append(trade_date)
        return rows


def fetch_max_trading_day() -> Optional[_dt.date]:
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


def fetch_port_dates(end: _dt.date) -> list[_dt.date]:
    sql = "SELECT DISTINCT port_date FROM tech11_ai_gov_eth_index WHERE port_date <= :end_date"
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, {"end_date": end})
        rows = []
        for (port_date,) in cur.fetchall():
            if isinstance(port_date, _dt.datetime):
                port_date = port_date.date()
            if isinstance(port_date, _dt.date):
                rows.append(port_date)
        return sorted(rows)


def fetch_universe(trade_date: _dt.date) -> tuple[_dt.date | None, list[str]]:
    sql = (
        "SELECT port_date, ticker "
        "FROM tech11_ai_gov_eth_index "
        "WHERE port_date = (SELECT MAX(port_date) FROM tech11_ai_gov_eth_index WHERE port_date <= :trade_date) "
        "AND port_weight > 0 "
        "AND ticker IS NOT NULL "
        "ORDER BY port_weight DESC, rank_index "
        "FETCH FIRST 25 ROWS ONLY"
    )
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, {"trade_date": trade_date})
        rows = cur.fetchall()
        if not rows:
            return None, []
        port_date = rows[0][0]
        if isinstance(port_date, _dt.datetime):
            port_date = port_date.date()
        tickers: list[str] = []
        for _, ticker in rows:
            if ticker is None:
                continue
            cleaned = str(ticker).strip().upper()
            if cleaned:
                tickers.append(cleaned)
        return port_date, tickers


def fetch_prices(
    trade_date: _dt.date,
    tickers: Iterable[str],
    *,
    allow_close: bool,
) -> dict[str, dict[str, object]]:
    tickers_list = [t for t in tickers if t]
    if not tickers_list:
        return {}
    placeholders = ",".join(f":t{i}" for i in range(len(tickers_list)))
    sql = (
        "SELECT ticker, canon_adj_close_px, canon_close_px, quality "
        "FROM SC_IDX_PRICES_CANON "
        f"WHERE trade_date = :trade_date AND ticker IN ({placeholders})"
    )
    binds = {"trade_date": trade_date}
    binds.update({f"t{i}": t for i, t in enumerate(tickers_list)})
    result: dict[str, dict[str, object]] = {}
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, binds)
        for ticker, adj_close, close_px, quality in cur.fetchall():
            if ticker is None:
                continue
            price = adj_close if adj_close is not None else (close_px if allow_close else None)
            result[str(ticker).strip().upper()] = {
                "price": price,
                "quality": str(quality or "").upper() if quality else None,
            }
    return result


def fetch_existing_levels(start: _dt.date, end: _dt.date) -> dict[_dt.date, float]:
    sql = (
        "SELECT trade_date, level_tr "
        "FROM SC_IDX_LEVELS "
        "WHERE index_code = :index_code "
        "AND trade_date BETWEEN :start_date AND :end_date"
    )
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, {"index_code": INDEX_CODE, "start_date": start, "end_date": end})
        rows = {}
        for trade_date, level in cur.fetchall():
            if isinstance(trade_date, _dt.datetime):
                trade_date = trade_date.date()
            if isinstance(trade_date, _dt.date):
                rows[trade_date] = float(level)
        return rows


def fetch_last_level_before(date: _dt.date) -> tuple[_dt.date | None, float | None]:
    sql = (
        "SELECT trade_date, level_tr "
        "FROM SC_IDX_LEVELS "
        "WHERE index_code = :index_code "
        "AND trade_date < :trade_date "
        "ORDER BY trade_date DESC FETCH FIRST 1 ROWS ONLY"
    )
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, {"index_code": INDEX_CODE, "trade_date": date})
        row = cur.fetchone()
        if not row:
            return None, None
        trade_date, level = row
        if isinstance(trade_date, _dt.datetime):
            trade_date = trade_date.date()
        return trade_date, float(level)


def fetch_divisor_for_date(date: _dt.date) -> float | None:
    sql = (
        "SELECT divisor "
        "FROM SC_IDX_DIVISOR "
        "WHERE index_code = :index_code "
        "AND effective_date = :effective_date"
    )
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, {"index_code": INDEX_CODE, "effective_date": date})
        row = cur.fetchone()
        if not row or row[0] is None:
            return None
        return float(row[0])


def upsert_holdings(rebalance_date: _dt.date, rows: list[dict]) -> None:
    if not rows:
        return
    sql = (
        "MERGE INTO SC_IDX_HOLDINGS dst "
        "USING (SELECT :index_code AS index_code, :rebalance_date AS rebalance_date, :ticker AS ticker FROM dual) src "
        "ON (dst.index_code = src.index_code AND dst.rebalance_date = src.rebalance_date AND dst.ticker = src.ticker) "
        "WHEN MATCHED THEN UPDATE SET "
        "  target_weight = :target_weight, "
        "  shares = :shares "
        "WHEN NOT MATCHED THEN INSERT "
        "  (index_code, rebalance_date, ticker, target_weight, shares) "
        "VALUES (:index_code, :rebalance_date, :ticker, :target_weight, :shares)"
    )
    binds = [
        {
            "index_code": INDEX_CODE,
            "rebalance_date": rebalance_date,
            "ticker": row["ticker"],
            "target_weight": row["target_weight"],
            "shares": row["shares"],
        }
        for row in rows
    ]
    with get_connection() as conn:
        cur = conn.cursor()
        _disable_parallel_dml(conn)
        cur.executemany(sql, binds)
        conn.commit()


def upsert_divisor(effective_date: _dt.date, divisor: float, reason: str | None = None) -> None:
    sql = (
        "MERGE INTO SC_IDX_DIVISOR dst "
        "USING (SELECT :index_code AS index_code, :effective_date AS effective_date FROM dual) src "
        "ON (dst.index_code = src.index_code AND dst.effective_date = src.effective_date) "
        "WHEN MATCHED THEN UPDATE SET "
        "  divisor = :divisor, "
        "  reason = :reason "
        "WHEN NOT MATCHED THEN INSERT "
        "  (index_code, effective_date, divisor, reason) "
        "VALUES (:index_code, :effective_date, :divisor, :reason)"
    )
    with get_connection() as conn:
        cur = conn.cursor()
        _disable_parallel_dml(conn)
        cur.execute(
            sql,
            {
                "index_code": INDEX_CODE,
                "effective_date": effective_date,
                "divisor": divisor,
                "reason": reason,
            },
        )
        conn.commit()


def upsert_levels(rows: list[dict]) -> None:
    if not rows:
        return
    sql = (
        "MERGE INTO SC_IDX_LEVELS dst "
        "USING (SELECT :index_code AS index_code, :trade_date AS trade_date FROM dual) src "
        "ON (dst.index_code = src.index_code AND dst.trade_date = src.trade_date) "
        "WHEN MATCHED THEN UPDATE SET "
        "  level_tr = :level_tr, "
        "  computed_at = SYSTIMESTAMP "
        "WHEN NOT MATCHED THEN INSERT "
        "  (index_code, trade_date, level_tr) "
        "VALUES (:index_code, :trade_date, :level_tr)"
    )
    binds = [
        {
            "index_code": INDEX_CODE,
            "trade_date": row["trade_date"],
            "level_tr": row["level_tr"],
        }
        for row in rows
    ]
    with get_connection() as conn:
        cur = conn.cursor()
        _disable_parallel_dml(conn)
        cur.executemany(sql, binds)
        conn.commit()


def upsert_constituent_daily(rows: list[dict]) -> None:
    if not rows:
        return
    sql = (
        "MERGE INTO SC_IDX_CONSTITUENT_DAILY dst "
        "USING (SELECT :trade_date AS trade_date, :ticker AS ticker FROM dual) src "
        "ON (dst.trade_date = src.trade_date AND dst.ticker = src.ticker) "
        "WHEN MATCHED THEN UPDATE SET "
        "  rebalance_date = :rebalance_date, "
        "  shares = :shares, "
        "  price_used = :price_used, "
        "  market_value = :market_value, "
        "  weight = :weight, "
        "  price_quality = :price_quality, "
        "  computed_at = SYSTIMESTAMP "
        "WHEN NOT MATCHED THEN INSERT "
        "  (trade_date, ticker, rebalance_date, shares, price_used, market_value, weight, price_quality) "
        "VALUES (:trade_date, :ticker, :rebalance_date, :shares, :price_used, :market_value, :weight, :price_quality)"
    )
    binds = [
        {
            "trade_date": row["trade_date"],
            "ticker": row["ticker"],
            "rebalance_date": row["rebalance_date"],
            "shares": row["shares"],
            "price_used": row["price_used"],
            "market_value": row["market_value"],
            "weight": row["weight"],
            "price_quality": row["price_quality"],
        }
        for row in rows
    ]
    with get_connection() as conn:
        cur = conn.cursor()
        _disable_parallel_dml(conn)
        cur.executemany(sql, binds)
        conn.commit()


def upsert_contribution_daily(rows: list[dict]) -> None:
    if not rows:
        return
    sql = (
        "MERGE INTO SC_IDX_CONTRIBUTION_DAILY dst "
        "USING (SELECT :trade_date AS trade_date, :ticker AS ticker FROM dual) src "
        "ON (dst.trade_date = src.trade_date AND dst.ticker = src.ticker) "
        "WHEN MATCHED THEN UPDATE SET "
        "  weight_prev = :weight_prev, "
        "  ret_1d = :ret_1d, "
        "  contribution = :contribution, "
        "  computed_at = SYSTIMESTAMP "
        "WHEN NOT MATCHED THEN INSERT "
        "  (trade_date, ticker, weight_prev, ret_1d, contribution) "
        "VALUES (:trade_date, :ticker, :weight_prev, :ret_1d, :contribution)"
    )
    binds = [
        {
            "trade_date": row["trade_date"],
            "ticker": row["ticker"],
            "weight_prev": row["weight_prev"],
            "ret_1d": row["ret_1d"],
            "contribution": row["contribution"],
        }
        for row in rows
    ]
    with get_connection() as conn:
        cur = conn.cursor()
        _disable_parallel_dml(conn)
        cur.executemany(sql, binds)
        conn.commit()


def upsert_stats_daily(rows: list[dict]) -> None:
    if not rows:
        return
    sql = (
        "MERGE INTO SC_IDX_STATS_DAILY dst "
        "USING (SELECT :trade_date AS trade_date FROM dual) src "
        "ON (dst.trade_date = src.trade_date) "
        "WHEN MATCHED THEN UPDATE SET "
        "  level_tr = :level_tr, "
        "  ret_1d = :ret_1d, "
        "  ret_5d = :ret_5d, "
        "  ret_20d = :ret_20d, "
        "  vol_20d = :vol_20d, "
        "  max_drawdown_252d = :max_drawdown_252d, "
        "  n_constituents = :n_constituents, "
        "  n_imputed = :n_imputed, "
        "  top5_weight = :top5_weight, "
        "  herfindahl = :herfindahl, "
        "  computed_at = SYSTIMESTAMP "
        "WHEN NOT MATCHED THEN INSERT "
        "  (trade_date, level_tr, ret_1d, ret_5d, ret_20d, vol_20d, max_drawdown_252d, "
        "   n_constituents, n_imputed, top5_weight, herfindahl) "
        "VALUES (:trade_date, :level_tr, :ret_1d, :ret_5d, :ret_20d, :vol_20d, :max_drawdown_252d, "
        "   :n_constituents, :n_imputed, :top5_weight, :herfindahl)"
    )
    binds = [
        {
            "trade_date": row["trade_date"],
            "level_tr": row.get("level_tr"),
            "ret_1d": row.get("ret_1d"),
            "ret_5d": row.get("ret_5d"),
            "ret_20d": row.get("ret_20d"),
            "vol_20d": row.get("vol_20d"),
            "max_drawdown_252d": row.get("max_drawdown_252d"),
            "n_constituents": row.get("n_constituents"),
            "n_imputed": row.get("n_imputed"),
            "top5_weight": row.get("top5_weight"),
            "herfindahl": row.get("herfindahl"),
        }
        for row in rows
    ]
    with get_connection() as conn:
        cur = conn.cursor()
        _disable_parallel_dml(conn)
        cur.executemany(sql, binds)
        conn.commit()


def delete_index_range(start: _dt.date, end: _dt.date) -> None:
    tables = [
        "SC_IDX_LEVELS",
        "SC_IDX_CONSTITUENT_DAILY",
        "SC_IDX_CONTRIBUTION_DAILY",
        "SC_IDX_STATS_DAILY",
    ]
    with get_connection() as conn:
        cur = conn.cursor()
        _disable_parallel_dml(conn)
        for table in tables:
            cur.execute(
                f"DELETE FROM {table} WHERE trade_date BETWEEN :start_date AND :end_date",
                {"start_date": start, "end_date": end},
            )
        conn.commit()


def delete_holdings_divisor(rebalance_dates: Iterable[_dt.date]) -> None:
    dates = list(rebalance_dates)
    if not dates:
        return
    placeholders = ",".join(f":d{i}" for i in range(len(dates)))
    binds = {f"d{i}": d for i, d in enumerate(dates)}
    binds["index_code"] = INDEX_CODE
    with get_connection() as conn:
        cur = conn.cursor()
        _disable_parallel_dml(conn)
        cur.execute(
            f"DELETE FROM SC_IDX_HOLDINGS WHERE index_code = :index_code AND rebalance_date IN ({placeholders})",
            binds,
        )
        cur.execute(
            f"DELETE FROM SC_IDX_DIVISOR WHERE index_code = :index_code AND effective_date IN ({placeholders})",
            binds,
        )
        conn.commit()


__all__ = [
    "INDEX_CODE",
    "fetch_trading_days",
    "fetch_max_trading_day",
    "fetch_port_dates",
    "fetch_universe",
    "fetch_prices",
    "fetch_existing_levels",
    "fetch_last_level_before",
    "fetch_divisor_for_date",
    "upsert_holdings",
    "upsert_divisor",
    "upsert_levels",
    "upsert_constituent_daily",
    "upsert_contribution_daily",
    "upsert_stats_daily",
    "delete_index_range",
    "delete_holdings_divisor",
]
