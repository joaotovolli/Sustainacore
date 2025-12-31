from __future__ import annotations

from dataclasses import dataclass
import datetime as dt
import math
import statistics
import os
from typing import Iterable, Optional

from django.core.cache import cache

from core.oracle_db import get_connection

INDEX_CODE = "TECH100"

CACHE_TTLS = {
    "levels": 600,
    "returns": 600,
    "stats": 600,
    "constituents": 300,
    "contribution": 300,
    "latest_date": 300,
    "date_bounds": 600,
    "quality_counts": 300,
    "imputed_overview": 300,
    "imputation_history": 300,
    "performance_attribution": 300,
    "attribution_table": 300,
    "holdings": 300,
    "rebalance_date": 600,
}

FIXTURE_LATEST_DATE = dt.date(2025, 1, 31)


def _data_mode() -> str:
    return os.getenv("TECH100_UI_DATA_MODE", "oracle").lower()


def get_data_mode() -> str:
    return _data_mode()


@dataclass(frozen=True)
class DrawdownResult:
    drawdown: float
    peak_date: Optional[dt.date]
    trough_date: Optional[dt.date]


def _cache_key(*parts: str) -> str:
    return "tech100_idx:" + ":".join(parts)


def _to_date(value) -> Optional[dt.date]:
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    return None


def _execute_rows(sql: str, params: dict) -> list[tuple]:
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        return cur.fetchall()


def _fixture_levels() -> list[tuple[dt.date, float]]:
    levels = []
    base = 1000.0
    for idx in range(60):
        levels.append((FIXTURE_LATEST_DATE - dt.timedelta(days=59 - idx), base + idx * 4.2))
    return levels


def _fixture_constituents() -> list[dict]:
    rows = []
    for idx in range(25):
        weight = 0.02 + (24 - idx) * 0.0008
        rows.append(
            {
                "ticker": f"TCH{idx + 1:02d}",
                "weight": weight,
                "price": 75.0 + idx * 1.4,
                "quality": "IMPUTED" if idx % 6 == 0 else "REAL",
                "ret_1d": 0.002 * (idx % 5 - 2),
                "contribution": 0.0002 * (idx % 7 - 3),
                "name": f"Tech Company {idx + 1}",
            }
        )
    return rows


def _fixture_stats() -> dict:
    return {
        "trade_date": FIXTURE_LATEST_DATE,
        "level_tr": 1250.4,
        "ret_1d": 0.0021,
        "ret_5d": 0.0112,
        "ret_20d": 0.0314,
        "vol_20d": 0.182,
        "max_drawdown_252d": -0.124,
        "n_constituents": 25,
        "n_imputed": 4,
        "top5_weight": 0.24,
        "herfindahl": 0.082,
    }


def _fixture_contribution() -> list[dict]:
    return [
        {
            "ticker": row["ticker"],
            "weight_prev": row["weight"],
            "ret_1d": row["ret_1d"],
            "contribution": row["contribution"],
            "quality": row["quality"],
            "weight": row["weight"],
        }
        for row in _fixture_constituents()
    ]


def _fixture_quality_counts() -> dict:
    return {"REAL": 21, "IMPUTED": 4}


def _fixture_sector_breakdown() -> list[dict]:
    return [
        {"sector": "Software", "weight": 0.34},
        {"sector": "Semiconductors", "weight": 0.22},
        {"sector": "Internet", "weight": 0.18},
        {"sector": "Hardware", "weight": 0.16},
        {"sector": "Services", "weight": 0.10},
    ]


def _fixture_imputed_overview() -> list[dict]:
    return [
        {"ticker": "TCH01", "imputed_days": 9},
        {"ticker": "TCH07", "imputed_days": 7},
        {"ticker": "TCH13", "imputed_days": 6},
        {"ticker": "TCH19", "imputed_days": 5},
        {"ticker": "TCH25", "imputed_days": 4},
    ]


def get_latest_rebalance_date() -> Optional[dt.date]:
    if _data_mode() == "fixture":
        return FIXTURE_LATEST_DATE - dt.timedelta(days=30)
    key = _cache_key("rebalance_date")

    def _load() -> Optional[dt.date]:
        rows = _execute_rows("SELECT MAX(port_date) FROM TECH11_AI_GOV_ETH_INDEX", {})
        if not rows:
            return None
        return _to_date(rows[0][0])

    return cache.get_or_set(key, _load, CACHE_TTLS["rebalance_date"])


def get_latest_trade_date() -> Optional[dt.date]:
    if _data_mode() == "fixture":
        return FIXTURE_LATEST_DATE
    key = _cache_key("latest_date")

    def _load() -> Optional[dt.date]:
        rows = _execute_rows(
            "SELECT MAX(trade_date) FROM SC_IDX_LEVELS WHERE index_code = :index_code",
            {"index_code": INDEX_CODE},
        )
        if not rows:
            return None
        return _to_date(rows[0][0])

    return cache.get_or_set(key, _load, CACHE_TTLS["latest_date"])


def get_trade_date_bounds() -> tuple[Optional[dt.date], Optional[dt.date]]:
    if _data_mode() == "fixture":
        levels = _fixture_levels()
        return levels[0][0], levels[-1][0]
    key = _cache_key("date_bounds")

    def _load() -> tuple[Optional[dt.date], Optional[dt.date]]:
        rows = _execute_rows(
            "SELECT MIN(trade_date), MAX(trade_date) "
            "FROM SC_IDX_LEVELS WHERE index_code = :index_code",
            {"index_code": INDEX_CODE},
        )
        if not rows:
            return None, None
        return _to_date(rows[0][0]), _to_date(rows[0][1])

    return cache.get_or_set(key, _load, CACHE_TTLS["date_bounds"])


def _levels_query(start_date: Optional[dt.date], end_date: Optional[dt.date]) -> tuple[str, dict]:
    conditions = ["index_code = :index_code"]
    params: dict = {"index_code": INDEX_CODE}
    if start_date:
        conditions.append("trade_date >= :start_date")
        params["start_date"] = start_date
    if end_date:
        conditions.append("trade_date <= :end_date")
        params["end_date"] = end_date
    sql = (
        "SELECT trade_date, level_tr "
        "FROM SC_IDX_LEVELS "
        f"WHERE {' AND '.join(conditions)} "
        "ORDER BY trade_date"
    )
    return sql, params


def get_index_levels(
    start_date: Optional[dt.date] = None, end_date: Optional[dt.date] = None
) -> list[tuple[dt.date, float]]:
    if _data_mode() == "fixture":
        levels = _fixture_levels()
        if start_date:
            levels = [row for row in levels if row[0] >= start_date]
        if end_date:
            levels = [row for row in levels if row[0] <= end_date]
        return levels
    key = _cache_key(
        "levels",
        (start_date.isoformat() if start_date else "none"),
        (end_date.isoformat() if end_date else "none"),
    )

    def _load() -> list[tuple[dt.date, float]]:
        sql, params = _levels_query(start_date, end_date)
        rows = _execute_rows(sql, params)
        levels: list[tuple[dt.date, float]] = []
        for row in rows:
            trade_date = _to_date(row[0])
            if not trade_date:
                continue
            try:
                level = float(row[1])
            except (TypeError, ValueError):
                continue
            levels.append((trade_date, level))
        return levels

    return cache.get_or_set(key, _load, CACHE_TTLS["levels"])


def _get_recent_levels(as_of_date: dt.date, limit: int) -> list[tuple[dt.date, float]]:
    if _data_mode() == "fixture":
        levels = [row for row in _fixture_levels() if row[0] <= as_of_date]
        return levels[-limit:] if limit else []
    key = _cache_key("recent_levels", as_of_date.isoformat(), str(limit))

    def _load() -> list[tuple[dt.date, float]]:
        rows = _execute_rows(
            "SELECT trade_date, level_tr "
            "FROM SC_IDX_LEVELS "
            "WHERE index_code = :index_code AND trade_date <= :as_of_date "
            "ORDER BY trade_date DESC FETCH FIRST :limit ROWS ONLY",
            {"index_code": INDEX_CODE, "as_of_date": as_of_date, "limit": limit},
        )
        levels: list[tuple[dt.date, float]] = []
        for row in rows:
            trade_date = _to_date(row[0])
            if not trade_date:
                continue
            try:
                level = float(row[1])
            except (TypeError, ValueError):
                continue
            levels.append((trade_date, level))
        return list(reversed(levels))

    return cache.get_or_set(key, _load, CACHE_TTLS["levels"])


def _return_from_levels(level_end: Optional[float], level_start: Optional[float]) -> Optional[float]:
    if level_end in (None, 0) or level_start in (None, 0):
        return None
    return (level_end / level_start) - 1.0


def _level_at_or_before(target_date: dt.date) -> Optional[tuple[dt.date, float]]:
    if _data_mode() == "fixture":
        levels = [row for row in _fixture_levels() if row[0] <= target_date]
        return levels[-1] if levels else None
    key = _cache_key("level_at_or_before", target_date.isoformat())

    def _load() -> Optional[tuple[dt.date, float]]:
        rows = _execute_rows(
            "SELECT trade_date, level_tr "
            "FROM SC_IDX_LEVELS "
            "WHERE index_code = :index_code AND trade_date <= :target_date "
            "ORDER BY trade_date DESC FETCH FIRST 1 ROWS ONLY",
            {"index_code": INDEX_CODE, "target_date": target_date},
        )
        if not rows:
            return None
        trade_date = _to_date(rows[0][0])
        if not trade_date:
            return None
        try:
            level = float(rows[0][1])
        except (TypeError, ValueError):
            return None
        return trade_date, level

    return cache.get_or_set(key, _load, CACHE_TTLS["levels"])


def get_return_between(as_of_date: dt.date, start_date: dt.date) -> Optional[float]:
    if _data_mode() == "fixture":
        latest = _level_at_or_before(as_of_date)
        start = _level_at_or_before(start_date)
        if not latest or not start:
            return None
        return _return_from_levels(latest[1], start[1])

    key = _cache_key("return_between", as_of_date.isoformat(), start_date.isoformat())

    def _load() -> Optional[float]:
        latest = _level_at_or_before(as_of_date)
        start = _level_at_or_before(start_date)
        if not latest or not start:
            return None
        return _return_from_levels(latest[1], start[1])

    return cache.get_or_set(key, _load, CACHE_TTLS["returns"])


def get_ytd_return(as_of_date: dt.date) -> tuple[Optional[float], Optional[dt.date]]:
    ytd_start = dt.date(as_of_date.year, 1, 1)
    key = _cache_key("ytd_return", as_of_date.isoformat())

    def _load() -> tuple[Optional[float], Optional[dt.date]]:
        levels = get_index_levels(ytd_start, as_of_date)
        if not levels:
            return None, None
        start_date, start_level = levels[0]
        end_level = levels[-1][1]
        return _return_from_levels(end_level, start_level), start_date

    return cache.get_or_set(key, _load, CACHE_TTLS["returns"])


def get_index_returns(
    start_date: Optional[dt.date] = None, end_date: Optional[dt.date] = None
) -> list[tuple[dt.date, float]]:
    if _data_mode() == "fixture":
        levels = get_index_levels(start_date, end_date)
        return _returns_from_levels(levels)
    key = _cache_key(
        "returns",
        (start_date.isoformat() if start_date else "none"),
        (end_date.isoformat() if end_date else "none"),
    )

    def _load() -> list[tuple[dt.date, float]]:
        conditions = []
        params: dict = {}
        if start_date:
            conditions.append("trade_date >= :start_date")
            params["start_date"] = start_date
        if end_date:
            conditions.append("trade_date <= :end_date")
            params["end_date"] = end_date
        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        rows = _execute_rows(
            "SELECT trade_date, ret_1d "
            f"FROM SC_IDX_STATS_DAILY {where_clause} "
            "ORDER BY trade_date",
            params,
        )
        returns: list[tuple[dt.date, float]] = []
        for row in rows:
            trade_date = _to_date(row[0])
            if not trade_date:
                continue
            try:
                ret_1d = float(row[1])
            except (TypeError, ValueError):
                continue
            returns.append((trade_date, ret_1d))

        if returns:
            return returns

        levels = get_index_levels(start_date, end_date)
        return _returns_from_levels(levels)

    return cache.get_or_set(key, _load, CACHE_TTLS["returns"])


def _returns_from_levels(levels: Iterable[tuple[dt.date, float]]) -> list[tuple[dt.date, float]]:
    levels_list = list(levels)
    returns: list[tuple[dt.date, float]] = []
    if len(levels_list) < 2:
        return returns
    for idx in range(1, len(levels_list)):
        prev_level = levels_list[idx - 1][1]
        curr_level = levels_list[idx][1]
        if prev_level in (None, 0) or curr_level is None:
            continue
        returns.append((levels_list[idx][0], (curr_level / prev_level) - 1.0))
    return returns


def get_stats(as_of_date: dt.date) -> dict:
    if _data_mode() == "fixture":
        return _fixture_stats()
    key = _cache_key("stats", as_of_date.isoformat())

    def _load() -> dict:
        rows = _execute_rows(
            "SELECT trade_date, level_tr, ret_1d, ret_5d, ret_20d, vol_20d, "
            "max_drawdown_252d, n_constituents, n_imputed, top5_weight, herfindahl "
            "FROM SC_IDX_STATS_DAILY WHERE trade_date = :trade_date",
            {"trade_date": as_of_date},
        )
        if not rows:
            return {}
        row = rows[0]
        return {
            "trade_date": _to_date(row[0]),
            "level_tr": row[1],
            "ret_1d": row[2],
            "ret_5d": row[3],
            "ret_20d": row[4],
            "vol_20d": row[5],
            "max_drawdown_252d": row[6],
            "n_constituents": row[7],
            "n_imputed": row[8],
            "top5_weight": row[9],
            "herfindahl": row[10],
        }

    return cache.get_or_set(key, _load, CACHE_TTLS["stats"])


def get_kpis(as_of_date: dt.date) -> dict:
    if _data_mode() == "fixture":
        levels = get_index_levels(as_of_date - dt.timedelta(days=30), as_of_date)
        latest = levels[-1] if levels else None
        inception = levels[0] if levels else None
        ret_1d = _return_from_levels(latest[1], levels[-2][1]) if len(levels) >= 2 else None
        ret_1w = _return_from_levels(latest[1], levels[-6][1]) if len(levels) >= 6 else None
        ret_1m = _return_from_levels(latest[1], levels[0][1]) if levels else None
        ret_ytd, _ = get_ytd_return(as_of_date)
        return {
            "level": latest[1] if latest else None,
            "ret_1d": ret_1d,
            "ret_1w": ret_1w,
            "ret_1m": ret_1m,
            "ret_ytd": ret_ytd,
            "ret_since_inception": _return_from_levels(latest[1], inception[1]) if inception and latest else None,
        }
    key = _cache_key("kpis", as_of_date.isoformat())

    def _load() -> dict:
        latest = _level_at_or_before(as_of_date)
        stats = get_stats(as_of_date)
        latest_level = latest[1] if latest else None

        def _return_for_delta(days: int) -> Optional[float]:
            target = as_of_date - dt.timedelta(days=days)
            prev = _level_at_or_before(target)
            if not prev or latest_level is None:
                return None
            return _return_from_levels(latest_level, prev[1])

        inception_min, _ = get_trade_date_bounds()
        inception_level = _level_at_or_before(inception_min) if inception_min else None
        ret_ytd, _ = get_ytd_return(as_of_date)

        return {
            "level": latest_level,
            "ret_1d": stats.get("ret_1d") if stats else _return_for_delta(1),
            "ret_1w": stats.get("ret_5d") if stats else _return_for_delta(7),
            "ret_1m": stats.get("ret_20d") if stats else _return_for_delta(30),
            "ret_ytd": ret_ytd,
            "ret_since_inception": _return_from_levels(
                latest_level, inception_level[1] if inception_level else None
            ),
        }

    return cache.get_or_set(key, _load, CACHE_TTLS["stats"])


def get_rolling_vol(as_of_date: dt.date, window: int = 20) -> Optional[float]:
    if _data_mode() == "fixture":
        return 0.182
    if window == 20:
        stats = get_stats(as_of_date)
        if stats and stats.get("vol_20d") is not None:
            try:
                return float(stats["vol_20d"])
            except (TypeError, ValueError):
                pass

    levels = _get_recent_levels(as_of_date, window + 1)
    returns = _returns_from_levels(levels)
    if len(returns) < window:
        return None
    recent = [r[1] for r in returns[-window:] if r[1] is not None]
    if len(recent) < 2:
        return None
    return statistics.pstdev(recent) * math.sqrt(252.0)


def get_rolling_vol_series(
    start_date: dt.date, end_date: dt.date, window: int = 30
) -> list[tuple[dt.date, float]]:
    levels = get_index_levels(start_date, end_date)
    if len(levels) < window + 1:
        return []
    vols: list[tuple[dt.date, float]] = []
    returns = _returns_from_levels(levels)
    for idx in range(window - 1, len(returns)):
        window_returns = [r[1] for r in returns[idx - window + 1 : idx + 1]]
        if len(window_returns) < 2:
            continue
        vol = statistics.pstdev(window_returns) * math.sqrt(252.0)
        vols.append((returns[idx][0], vol))
    return vols


def get_drawdown_series(start_date: dt.date, end_date: dt.date) -> list[tuple[dt.date, float]]:
    levels = get_index_levels(start_date, end_date)
    if not levels:
        return []
    peak = levels[0][1]
    drawdowns: list[tuple[dt.date, float]] = []
    for trade_date, level in levels:
        if level > peak:
            peak = level
        dd = (level / peak) - 1.0 if peak else 0.0
        drawdowns.append((trade_date, dd))
    return drawdowns


def get_max_drawdown(start_date: dt.date, end_date: dt.date) -> DrawdownResult:
    if _data_mode() == "fixture":
        return DrawdownResult(-0.12, start_date, end_date)
    levels = get_index_levels(start_date, end_date)
    if not levels:
        return DrawdownResult(0.0, None, None)
    peak = levels[0][1]
    peak_date = levels[0][0]
    max_dd = 0.0
    trough_date = levels[0][0]
    for trade_date, level in levels:
        if level > peak:
            peak = level
            peak_date = trade_date
        if peak:
            dd = (level / peak) - 1.0
            if dd < max_dd:
                max_dd = dd
                trough_date = trade_date
    return DrawdownResult(max_dd, peak_date, trough_date)


def get_constituents(as_of_date: dt.date) -> list[dict]:
    if _data_mode() == "fixture":
        return _fixture_constituents()
    key = _cache_key("constituents", as_of_date.isoformat())

    def _load() -> list[dict]:
        sql_with_names = (
            "SELECT c.ticker, c.weight, c.price_used, c.price_quality, "
            "a.ret_1d, a.contribution, "
            "(SELECT t.company_name "
            " FROM tech11_ai_gov_eth_index t "
            " WHERE t.ticker = c.ticker AND t.port_date <= :trade_date "
            " ORDER BY t.port_date DESC FETCH FIRST 1 ROWS ONLY) AS company_name "
            "FROM SC_IDX_CONSTITUENT_DAILY c "
            "LEFT JOIN SC_IDX_CONTRIBUTION_DAILY a "
            "ON a.trade_date = c.trade_date AND a.ticker = c.ticker "
            "WHERE c.trade_date = :trade_date "
            "ORDER BY c.weight DESC NULLS LAST"
        )
        sql_basic = (
            "SELECT c.ticker, c.weight, c.price_used, c.price_quality, "
            "a.ret_1d, a.contribution "
            "FROM SC_IDX_CONSTITUENT_DAILY c "
            "LEFT JOIN SC_IDX_CONTRIBUTION_DAILY a "
            "ON a.trade_date = c.trade_date AND a.ticker = c.ticker "
            "WHERE c.trade_date = :trade_date "
            "ORDER BY c.weight DESC NULLS LAST"
        )
        try:
            rows = _execute_rows(sql_with_names, {"trade_date": as_of_date})
            has_name = True
        except Exception:
            rows = _execute_rows(sql_basic, {"trade_date": as_of_date})
            has_name = False
        results = []
        for row in rows:
            results.append(
                {
                    "ticker": row[0],
                    "weight": row[1],
                    "price": row[2],
                    "quality": row[3],
                    "ret_1d": row[4],
                    "contribution": row[5] if len(row) > 5 else None,
                    "name": row[6] if has_name and len(row) > 6 else None,
                }
            )
        return results

    return cache.get_or_set(key, _load, CACHE_TTLS["imputed_overview"])


def get_quality_counts(as_of_date: dt.date) -> dict:
    if _data_mode() == "fixture":
        return _fixture_quality_counts()
    key = _cache_key("quality_counts", as_of_date.isoformat())

    def _load() -> dict:
        rows = _execute_rows(
            "SELECT price_quality, COUNT(*) FROM SC_IDX_CONSTITUENT_DAILY "
            "WHERE trade_date = :trade_date GROUP BY price_quality",
            {"trade_date": as_of_date},
        )
        return {row[0] or "UNKNOWN": row[1] for row in rows}

    return cache.get_or_set(key, _load, CACHE_TTLS["quality_counts"])


def get_contribution(as_of_date: dt.date) -> list[dict]:
    if _data_mode() == "fixture":
        return _fixture_contribution()
    key = _cache_key("contribution", as_of_date.isoformat())

    def _load() -> list[dict]:
        rows = _execute_rows(
            "SELECT d.ticker, d.weight_prev, d.ret_1d, d.contribution, "
            "c.price_quality, c.weight "
            "FROM SC_IDX_CONTRIBUTION_DAILY d "
            "LEFT JOIN SC_IDX_CONSTITUENT_DAILY c "
            "ON c.trade_date = d.trade_date AND c.ticker = d.ticker "
            "WHERE d.trade_date = :trade_date "
            "ORDER BY d.contribution DESC NULLS LAST",
            {"trade_date": as_of_date},
        )
        results = []
        for row in rows:
            results.append(
                {
                    "ticker": row[0],
                    "weight_prev": row[1],
                    "ret_1d": row[2],
                    "contribution": row[3],
                    "quality": row[4],
                    "weight": row[5],
                }
            )
        return results

    return cache.get_or_set(key, _load, CACHE_TTLS["contribution"])


def get_contribution_summary(
    as_of_date: dt.date, start_date: dt.date, limit: int = 10, direction: str = "desc"
) -> list[dict]:
    if _data_mode() == "fixture":
        rows = _fixture_contribution()
        reverse = direction.lower() != "asc"
        rows.sort(key=lambda item: item.get("contribution") or 0.0, reverse=reverse)
        return rows[:limit]
    key = _cache_key(
        "performance_attribution",
        as_of_date.isoformat(),
        start_date.isoformat(),
        str(limit),
        direction,
    )

    def _load() -> list[dict]:
        order = "ASC" if direction.lower() == "asc" else "DESC"
        rows = _execute_rows(
            "SELECT d.ticker, SUM(d.contribution) AS contrib_sum, "
            "(SELECT t.company_name "
            " FROM tech11_ai_gov_eth_index t "
            " WHERE t.ticker = d.ticker AND t.port_date <= :as_of_date "
            " ORDER BY t.port_date DESC FETCH FIRST 1 ROWS ONLY) AS company_name "
            "FROM SC_IDX_CONTRIBUTION_DAILY d "
            "WHERE d.trade_date BETWEEN :start_date AND :as_of_date "
            "GROUP BY d.ticker "
            f"ORDER BY contrib_sum {order} FETCH FIRST :limit ROWS ONLY",
            {"start_date": start_date, "as_of_date": as_of_date, "limit": limit},
        )
        return [
            {"ticker": row[0], "contribution": row[1], "name": row[2]}
            for row in rows
        ]

    return cache.get_or_set(key, _load, CACHE_TTLS["performance_attribution"])


def get_imputed_overview(as_of_date: dt.date, window_days: int = 30) -> list[dict]:
    if _data_mode() == "fixture":
        return _fixture_imputed_overview()
    key = _cache_key("imputed_overview", as_of_date.isoformat(), str(window_days))

    def _load() -> list[dict]:
        rows = _execute_rows(
            "WITH recent_dates AS ("
            "SELECT trade_date FROM ("
            "SELECT DISTINCT trade_date "
            "FROM SC_IDX_CONSTITUENT_DAILY "
            "WHERE trade_date <= :as_of_date "
            "ORDER BY trade_date DESC"
            ") WHERE ROWNUM <= :window_days"
            ") "
            "SELECT c.ticker, COUNT(*) "
            "FROM SC_IDX_CONSTITUENT_DAILY c "
            "JOIN recent_dates d ON d.trade_date = c.trade_date "
            "WHERE c.price_quality = 'IMPUTED' "
            "GROUP BY c.ticker "
            "ORDER BY COUNT(*) DESC FETCH FIRST 10 ROWS ONLY",
            {"as_of_date": as_of_date, "window_days": window_days},
        )
        return [{"ticker": row[0], "imputed_days": row[1]} for row in rows]

    return cache.get_or_set(key, _load, CACHE_TTLS["constituents"])


def get_imputation_history(as_of_date: dt.date, window_days: int = 30) -> list[dict]:
    if _data_mode() == "fixture":
        return _fixture_imputed_overview()
    key = _cache_key("imputation_history", as_of_date.isoformat(), str(window_days))

    def _load() -> list[dict]:
        rows = _execute_rows(
            "WITH recent_dates AS ("
            "SELECT trade_date FROM ("
            "SELECT DISTINCT trade_date FROM SC_IDX_CONSTITUENT_DAILY "
            "WHERE trade_date <= :as_of_date "
            "ORDER BY trade_date DESC"
            ") WHERE ROWNUM <= :window_days"
            ") "
            "SELECT c.ticker, COUNT(*) AS imputed_days, MAX(c.trade_date) "
            "FROM SC_IDX_CONSTITUENT_DAILY c "
            "JOIN recent_dates d ON d.trade_date = c.trade_date "
            "WHERE c.price_quality = 'IMPUTED' "
            "GROUP BY c.ticker "
            "ORDER BY COUNT(*) DESC FETCH FIRST 10 ROWS ONLY",
            {"as_of_date": as_of_date, "window_days": window_days},
        )
        return [
            {"ticker": row[0], "imputed_days": row[1], "last_imputed": _to_date(row[2])}
            for row in rows
        ]

    return cache.get_or_set(key, _load, CACHE_TTLS["imputation_history"])


def get_holdings_with_meta(as_of_date: dt.date) -> list[dict]:
    if _data_mode() == "fixture":
        rows = _fixture_constituents()
        for row in rows:
            row["sector"] = "Software"
        return rows
    key = _cache_key("holdings", as_of_date.isoformat())

    def _load() -> list[dict]:
        sql_with_sector = (
            "SELECT c.ticker, c.weight, c.price_used, c.price_quality, "
            "d.ret_1d, d.contribution, "
            "(SELECT t.company_name "
            " FROM tech11_ai_gov_eth_index t "
            " WHERE t.ticker = c.ticker AND t.port_date <= :trade_date "
            " ORDER BY t.port_date DESC FETCH FIRST 1 ROWS ONLY) AS company_name, "
            "(SELECT t.sector "
            " FROM tech11_ai_gov_eth_index t "
            " WHERE t.ticker = c.ticker AND t.port_date <= :trade_date "
            " ORDER BY t.port_date DESC FETCH FIRST 1 ROWS ONLY) AS sector "
            "FROM SC_IDX_CONSTITUENT_DAILY c "
            "LEFT JOIN SC_IDX_CONTRIBUTION_DAILY d "
            "ON d.trade_date = c.trade_date AND d.ticker = c.ticker "
            "WHERE c.trade_date = :trade_date "
            "ORDER BY c.weight DESC NULLS LAST"
        )
        sql_with_gics_sector = (
            "SELECT c.ticker, c.weight, c.price_used, c.price_quality, "
            "d.ret_1d, d.contribution, "
            "(SELECT t.company_name "
            " FROM tech11_ai_gov_eth_index t "
            " WHERE t.ticker = c.ticker AND t.port_date <= :trade_date "
            " ORDER BY t.port_date DESC FETCH FIRST 1 ROWS ONLY) AS company_name, "
            "(SELECT t.gics_sector "
            " FROM tech11_ai_gov_eth_index t "
            " WHERE t.ticker = c.ticker AND t.port_date <= :trade_date "
            " ORDER BY t.port_date DESC FETCH FIRST 1 ROWS ONLY) AS sector "
            "FROM SC_IDX_CONSTITUENT_DAILY c "
            "LEFT JOIN SC_IDX_CONTRIBUTION_DAILY d "
            "ON d.trade_date = c.trade_date AND d.ticker = c.ticker "
            "WHERE c.trade_date = :trade_date "
            "ORDER BY c.weight DESC NULLS LAST"
        )
        sql_with_sector_name = (
            "SELECT c.ticker, c.weight, c.price_used, c.price_quality, "
            "d.ret_1d, d.contribution, "
            "(SELECT t.company_name "
            " FROM tech11_ai_gov_eth_index t "
            " WHERE t.ticker = c.ticker AND t.port_date <= :trade_date "
            " ORDER BY t.port_date DESC FETCH FIRST 1 ROWS ONLY) AS company_name, "
            "(SELECT t.gics_sector_name "
            " FROM tech11_ai_gov_eth_index t "
            " WHERE t.ticker = c.ticker AND t.port_date <= :trade_date "
            " ORDER BY t.port_date DESC FETCH FIRST 1 ROWS ONLY) AS sector "
            "FROM SC_IDX_CONSTITUENT_DAILY c "
            "LEFT JOIN SC_IDX_CONTRIBUTION_DAILY d "
            "ON d.trade_date = c.trade_date AND d.ticker = c.ticker "
            "WHERE c.trade_date = :trade_date "
            "ORDER BY c.weight DESC NULLS LAST"
        )
        sql_without_sector = (
            "SELECT c.ticker, c.weight, c.price_used, c.price_quality, "
            "d.ret_1d, d.contribution, "
            "(SELECT t.company_name "
            " FROM tech11_ai_gov_eth_index t "
            " WHERE t.ticker = c.ticker AND t.port_date <= :trade_date "
            " ORDER BY t.port_date DESC FETCH FIRST 1 ROWS ONLY) AS company_name "
            "FROM SC_IDX_CONSTITUENT_DAILY c "
            "LEFT JOIN SC_IDX_CONTRIBUTION_DAILY d "
            "ON d.trade_date = c.trade_date AND d.ticker = c.ticker "
            "WHERE c.trade_date = :trade_date "
            "ORDER BY c.weight DESC NULLS LAST"
        )
        rows = None
        has_sector = False

        def _sector_blank(index: int) -> bool:
            return all(len(row) <= index or not (row[index] and str(row[index]).strip()) for row in rows or [])

        try:
            rows = _execute_rows(sql_with_sector, {"trade_date": as_of_date})
            has_sector = True
        except Exception:
            rows = None
        if rows is None or _sector_blank(7):
            try:
                rows = _execute_rows(sql_with_gics_sector, {"trade_date": as_of_date})
                has_sector = True
            except Exception:
                rows = None
        if rows is None or _sector_blank(7):
            try:
                rows = _execute_rows(sql_with_sector_name, {"trade_date": as_of_date})
                has_sector = True
            except Exception:
                rows = None
        if rows is None:
            rows = _execute_rows(sql_without_sector, {"trade_date": as_of_date})
            has_sector = False
        results = []
        for row in rows:
            results.append(
                {
                    "ticker": row[0],
                    "weight": row[1],
                    "price": row[2],
                    "quality": row[3],
                    "ret_1d": row[4],
                    "contribution": row[5],
                    "name": row[6],
                    "sector": row[7] if has_sector and len(row) > 7 else None,
                }
            )
        return results

    return cache.get_or_set(key, _load, CACHE_TTLS["holdings"])


def get_sector_breakdown(holdings: list[dict]) -> list[dict]:
    if _data_mode() == "fixture":
        return _fixture_sector_breakdown()
    totals: dict[str, float] = {}
    for row in holdings:
        sector = (row.get("sector") or "").strip() or "Unclassified"
        weight = float(row.get("weight") or 0.0)
        totals[sector] = totals.get(sector, 0.0) + weight
    return [
        {"sector": sector, "weight": weight}
        for sector, weight in sorted(totals.items(), key=lambda item: item[1], reverse=True)
    ]


def get_attribution_table(as_of_date: dt.date, mtd_start: dt.date, ytd_start: dt.date) -> list[dict]:
    if _data_mode() == "fixture":
        rows = _fixture_constituents()
        for row in rows:
            row["contrib_mtd"] = row.get("contribution")
            row["contrib_ytd"] = row.get("contribution")
            row["sector"] = "Software"
        return rows
    key = _cache_key("attribution_table", as_of_date.isoformat())

    def _load() -> list[dict]:
        sql_with_sector = (
            "SELECT c.ticker, c.weight, c.price_quality, "
            "d.ret_1d, d.contribution, "
            "(SELECT SUM(x.contribution) FROM SC_IDX_CONTRIBUTION_DAILY x "
            " WHERE x.ticker = c.ticker AND x.trade_date BETWEEN :mtd_start AND :as_of_date) AS contrib_mtd, "
            "(SELECT SUM(x.contribution) FROM SC_IDX_CONTRIBUTION_DAILY x "
            " WHERE x.ticker = c.ticker AND x.trade_date BETWEEN :ytd_start AND :as_of_date) AS contrib_ytd, "
            "(SELECT t.company_name FROM tech11_ai_gov_eth_index t "
            " WHERE t.ticker = c.ticker AND t.port_date <= :as_of_date "
            " ORDER BY t.port_date DESC FETCH FIRST 1 ROWS ONLY) AS company_name, "
            "(SELECT t.sector FROM tech11_ai_gov_eth_index t "
            " WHERE t.ticker = c.ticker AND t.port_date <= :as_of_date "
            " ORDER BY t.port_date DESC FETCH FIRST 1 ROWS ONLY) AS sector "
            "FROM SC_IDX_CONSTITUENT_DAILY c "
            "LEFT JOIN SC_IDX_CONTRIBUTION_DAILY d "
            "ON d.trade_date = c.trade_date AND d.ticker = c.ticker "
            "WHERE c.trade_date = :as_of_date "
            "ORDER BY c.weight DESC NULLS LAST"
        )
        sql_with_gics_sector = (
            "SELECT c.ticker, c.weight, c.price_quality, "
            "d.ret_1d, d.contribution, "
            "(SELECT SUM(x.contribution) FROM SC_IDX_CONTRIBUTION_DAILY x "
            " WHERE x.ticker = c.ticker AND x.trade_date BETWEEN :mtd_start AND :as_of_date) AS contrib_mtd, "
            "(SELECT SUM(x.contribution) FROM SC_IDX_CONTRIBUTION_DAILY x "
            " WHERE x.ticker = c.ticker AND x.trade_date BETWEEN :ytd_start AND :as_of_date) AS contrib_ytd, "
            "(SELECT t.company_name FROM tech11_ai_gov_eth_index t "
            " WHERE t.ticker = c.ticker AND t.port_date <= :as_of_date "
            " ORDER BY t.port_date DESC FETCH FIRST 1 ROWS ONLY) AS company_name, "
            "(SELECT t.gics_sector FROM tech11_ai_gov_eth_index t "
            " WHERE t.ticker = c.ticker AND t.port_date <= :as_of_date "
            " ORDER BY t.port_date DESC FETCH FIRST 1 ROWS ONLY) AS sector "
            "FROM SC_IDX_CONSTITUENT_DAILY c "
            "LEFT JOIN SC_IDX_CONTRIBUTION_DAILY d "
            "ON d.trade_date = c.trade_date AND d.ticker = c.ticker "
            "WHERE c.trade_date = :as_of_date "
            "ORDER BY c.weight DESC NULLS LAST"
        )
        sql_with_sector_name = (
            "SELECT c.ticker, c.weight, c.price_quality, "
            "d.ret_1d, d.contribution, "
            "(SELECT SUM(x.contribution) FROM SC_IDX_CONTRIBUTION_DAILY x "
            " WHERE x.ticker = c.ticker AND x.trade_date BETWEEN :mtd_start AND :as_of_date) AS contrib_mtd, "
            "(SELECT SUM(x.contribution) FROM SC_IDX_CONTRIBUTION_DAILY x "
            " WHERE x.ticker = c.ticker AND x.trade_date BETWEEN :ytd_start AND :as_of_date) AS contrib_ytd, "
            "(SELECT t.company_name FROM tech11_ai_gov_eth_index t "
            " WHERE t.ticker = c.ticker AND t.port_date <= :as_of_date "
            " ORDER BY t.port_date DESC FETCH FIRST 1 ROWS ONLY) AS company_name, "
            "(SELECT t.gics_sector_name FROM tech11_ai_gov_eth_index t "
            " WHERE t.ticker = c.ticker AND t.port_date <= :as_of_date "
            " ORDER BY t.port_date DESC FETCH FIRST 1 ROWS ONLY) AS sector "
            "FROM SC_IDX_CONSTITUENT_DAILY c "
            "LEFT JOIN SC_IDX_CONTRIBUTION_DAILY d "
            "ON d.trade_date = c.trade_date AND d.ticker = c.ticker "
            "WHERE c.trade_date = :as_of_date "
            "ORDER BY c.weight DESC NULLS LAST"
        )
        sql_without_sector = (
            "SELECT c.ticker, c.weight, c.price_quality, "
            "d.ret_1d, d.contribution, "
            "(SELECT SUM(x.contribution) FROM SC_IDX_CONTRIBUTION_DAILY x "
            " WHERE x.ticker = c.ticker AND x.trade_date BETWEEN :mtd_start AND :as_of_date) AS contrib_mtd, "
            "(SELECT SUM(x.contribution) FROM SC_IDX_CONTRIBUTION_DAILY x "
            " WHERE x.ticker = c.ticker AND x.trade_date BETWEEN :ytd_start AND :as_of_date) AS contrib_ytd, "
            "(SELECT t.company_name FROM tech11_ai_gov_eth_index t "
            " WHERE t.ticker = c.ticker AND t.port_date <= :as_of_date "
            " ORDER BY t.port_date DESC FETCH FIRST 1 ROWS ONLY) AS company_name "
            "FROM SC_IDX_CONSTITUENT_DAILY c "
            "LEFT JOIN SC_IDX_CONTRIBUTION_DAILY d "
            "ON d.trade_date = c.trade_date AND d.ticker = c.ticker "
            "WHERE c.trade_date = :as_of_date "
            "ORDER BY c.weight DESC NULLS LAST"
        )
        rows = None
        has_sector = False

        def _attr_sector_blank(index: int) -> bool:
            return all(len(row) <= index or not (row[index] and str(row[index]).strip()) for row in rows or [])

        try:
            rows = _execute_rows(
                sql_with_sector,
                {"as_of_date": as_of_date, "mtd_start": mtd_start, "ytd_start": ytd_start},
            )
            has_sector = True
        except Exception:
            rows = None
        if rows is None or _attr_sector_blank(8):
            try:
                rows = _execute_rows(
                    sql_with_gics_sector,
                    {"as_of_date": as_of_date, "mtd_start": mtd_start, "ytd_start": ytd_start},
                )
                has_sector = True
            except Exception:
                rows = None
        if rows is None or _attr_sector_blank(8):
            try:
                rows = _execute_rows(
                    sql_with_sector_name,
                    {"as_of_date": as_of_date, "mtd_start": mtd_start, "ytd_start": ytd_start},
                )
                has_sector = True
            except Exception:
                rows = None
        if rows is None:
            rows = _execute_rows(
                sql_without_sector,
                {"as_of_date": as_of_date, "mtd_start": mtd_start, "ytd_start": ytd_start},
            )
            has_sector = False
        results = []
        for row in rows:
            results.append(
                {
                    "ticker": row[0],
                    "weight": row[1],
                    "quality": row[2],
                    "ret_1d": row[3],
                    "contribution": row[4],
                    "contrib_mtd": row[5],
                    "contrib_ytd": row[6],
                    "name": row[7],
                    "sector": row[8] if has_sector and len(row) > 8 else None,
                }
            )
        return results

    return cache.get_or_set(key, _load, CACHE_TTLS["attribution_table"])
