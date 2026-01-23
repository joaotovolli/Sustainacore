from __future__ import annotations

import datetime as dt
from typing import Optional

from django.core.cache import cache

from core.oracle_db import get_connection

METRIC_COLUMNS = {
    "composite": "aiges_composite_average",
    "transparency": "transparency",
    "ethical_principles": "ethical_principles",
    "governance_structure": "governance_structure",
    "regulatory_alignment": "regulatory_alignment",
    "stakeholder_engagement": "stakeholder_engagement",
}

CACHE_TTLS = {
    "summary": 300,
    "history": 600,
    "series": 300,
    "latest_date": 600,
    "baseline": 900,
}


def _cache_key(*parts: str) -> str:
    return "tech100_company:" + ":".join(parts)


def _execute_rows(sql: str, params: dict) -> list[tuple]:
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        return cur.fetchall()


def _execute_rows_with_fallback(sql: str, params: dict, fallback_sql: str) -> list[tuple]:
    rows = _execute_rows(sql, params)
    if rows:
        return rows
    return _execute_rows(fallback_sql, params)


def _normalize_ticker(ticker: str) -> str:
    return (ticker or "").strip().upper()


def _to_date(value) -> Optional[dt.date]:
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    return None


def _float_or_none(value) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _resolve_range(range_key: str, latest_date: dt.date) -> tuple[Optional[dt.date], dt.date]:
    normalized = (range_key or "").lower()
    if normalized == "ytd":
        return dt.date(latest_date.year, 1, 1), latest_date
    if normalized == "3m":
        return latest_date - dt.timedelta(days=90), latest_date
    if normalized == "1y":
        return latest_date - dt.timedelta(days=365), latest_date
    if normalized == "max":
        return None, latest_date
    # Default to 6m window.
    return latest_date - dt.timedelta(days=182), latest_date


def get_company_summary(ticker: str) -> Optional[dict]:
    normalized = _normalize_ticker(ticker)
    if not normalized:
        return None
    key = _cache_key("summary", normalized)
    cached = cache.get(key)
    if isinstance(cached, dict):
        return cached

    sql = (
        "SELECT ticker, company_name, gics_sector, port_date, rank_index, port_weight, "
        "aiges_composite_average, transparency, ethical_principles, governance_structure, "
        "regulatory_alignment, stakeholder_engagement "
        "FROM tech11_ai_gov_eth_index "
        "WHERE ticker = :ticker "
        "ORDER BY port_date DESC FETCH FIRST 1 ROWS ONLY"
    )
    fallback_sql = sql.replace("ticker = :ticker", "UPPER(ticker) = :ticker")
    rows = _execute_rows_with_fallback(sql, {"ticker": normalized}, fallback_sql)
    if not rows:
        return None

    (
        ticker_val,
        company_name,
        sector,
        port_date,
        rank_index,
        port_weight,
        composite,
        transparency,
        ethical_principles,
        governance_structure,
        regulatory_alignment,
        stakeholder_engagement,
    ) = rows[0]

    latest_date = _to_date(port_date)
    payload = {
        "ticker": normalized,
        "company_name": company_name,
        "sector": sector,
        "latest_date": latest_date.isoformat() if latest_date else None,
        "latest_rank": _float_or_none(rank_index),
        "latest_weight": _float_or_none(port_weight),
        "latest_scores": {
            "composite": _float_or_none(composite),
            "transparency": _float_or_none(transparency),
            "ethical_principles": _float_or_none(ethical_principles),
            "governance_structure": _float_or_none(governance_structure),
            "regulatory_alignment": _float_or_none(regulatory_alignment),
            "stakeholder_engagement": _float_or_none(stakeholder_engagement),
        },
    }
    cache.set(key, payload, CACHE_TTLS["summary"])
    return payload


def get_company_latest_date(ticker: str) -> Optional[dt.date]:
    normalized = _normalize_ticker(ticker)
    if not normalized:
        return None
    key = _cache_key("latest_date", normalized)
    cached = cache.get(key)
    if isinstance(cached, str):
        try:
            return dt.date.fromisoformat(cached)
        except ValueError:
            cache.delete(key)

    sql = "SELECT MAX(port_date) FROM tech11_ai_gov_eth_index WHERE ticker = :ticker"
    fallback_sql = sql.replace("ticker = :ticker", "UPPER(ticker) = :ticker")
    rows = _execute_rows_with_fallback(sql, {"ticker": normalized}, fallback_sql)
    latest_date = _to_date(rows[0][0]) if rows else None
    cache.set(key, latest_date.isoformat() if latest_date else None, CACHE_TTLS["latest_date"])
    return latest_date


def get_company_history(ticker: str) -> Optional[list[dict]]:
    normalized = _normalize_ticker(ticker)
    if not normalized:
        return None
    key = _cache_key("history", normalized)
    cached = cache.get(key)
    if isinstance(cached, list):
        return cached

    sql = (
        "SELECT port_date, rank_index, port_weight, aiges_composite_average, "
        "transparency, ethical_principles, governance_structure, "
        "regulatory_alignment, stakeholder_engagement "
        "FROM tech11_ai_gov_eth_index "
        "WHERE ticker = :ticker "
        "ORDER BY port_date DESC"
    )
    fallback_sql = sql.replace("ticker = :ticker", "UPPER(ticker) = :ticker")
    rows = _execute_rows_with_fallback(sql, {"ticker": normalized}, fallback_sql)
    if not rows:
        return None

    history: list[dict] = []
    for (
        port_date,
        rank_index,
        port_weight,
        composite,
        transparency,
        ethical_principles,
        governance_structure,
        regulatory_alignment,
        stakeholder_engagement,
    ) in rows:
        date_val = _to_date(port_date)
        history.append(
            {
                "date": date_val.isoformat() if date_val else None,
                "rank": _float_or_none(rank_index),
                "weight": _float_or_none(port_weight),
                "composite": _float_or_none(composite),
                "transparency": _float_or_none(transparency),
                "ethical_principles": _float_or_none(ethical_principles),
                "governance_structure": _float_or_none(governance_structure),
                "regulatory_alignment": _float_or_none(regulatory_alignment),
                "stakeholder_engagement": _float_or_none(stakeholder_engagement),
            }
        )

    cache.set(key, history, CACHE_TTLS["history"])
    return history


def _build_date_binds(dates: list[dt.date]) -> tuple[str, dict]:
    bindings = {}
    placeholders = []
    for idx, value in enumerate(dates):
        key = f"d{idx}"
        placeholders.append(f":{key}")
        bindings[key] = value
    return ", ".join(placeholders), bindings


def _cached_baseline(metric_key: str, dates: list[dt.date]) -> Optional[dict]:
    if not dates:
        return None
    date_key = ",".join([d.isoformat() for d in dates])
    cache_key = _cache_key("baseline", metric_key, date_key)
    cached = cache.get(cache_key)
    if isinstance(cached, dict):
        return cached
    return None


def get_company_series(ticker: str, metric: str, range_key: str) -> Optional[list[dict]]:
    normalized = _normalize_ticker(ticker)
    if not normalized:
        return None

    metric_key = (metric or "").lower()
    metric_col = METRIC_COLUMNS.get(metric_key)
    if not metric_col:
        return None

    latest_date = get_company_latest_date(normalized)
    if not latest_date:
        return None
    start_date, end_date = _resolve_range(range_key, latest_date)

    series_key = _cache_key("series", normalized, metric_key, range_key or "")
    cached = cache.get(series_key)
    if isinstance(cached, list):
        return cached

    params: dict = {"ticker": normalized, "end_date": end_date}
    baseline_params: dict = {"end_date": end_date}
    date_filter = "port_date <= :end_date"
    if start_date:
        params["start_date"] = start_date
        baseline_params["start_date"] = start_date
        date_filter = "port_date >= :start_date AND port_date <= :end_date"

    company_sql = (
        f"SELECT port_date, {metric_col} "
        "FROM tech11_ai_gov_eth_index "
        "WHERE ticker = :ticker AND "
        f"{date_filter} AND {metric_col} IS NOT NULL "
        "ORDER BY port_date"
    )
    fallback_company_sql = company_sql.replace("ticker = :ticker", "UPPER(ticker) = :ticker")
    company_rows = _execute_rows_with_fallback(company_sql, params, fallback_company_sql)
    if not company_rows:
        return None

    date_list = sorted({_to_date(row[0]) for row in company_rows if _to_date(row[0]) is not None})
    baseline_map = _cached_baseline(metric_key, date_list) or {}
    if not baseline_map:
        baseline_rows = []
        if 0 < len(date_list) <= 120:
            placeholders, date_params = _build_date_binds(date_list)
            baseline_sql = (
                "WITH ranked AS ("
                "SELECT port_date, "
                f"{metric_col} AS metric, "
                "rank_index, port_weight, aiges_composite_average, "
                "ROW_NUMBER() OVER (PARTITION BY port_date ORDER BY "
                "CASE WHEN rank_index IS NULL THEN 999999 ELSE rank_index END, "
                "port_weight DESC NULLS LAST, "
                "aiges_composite_average DESC NULLS LAST) AS rn "
                "FROM tech11_ai_gov_eth_index "
                f"WHERE port_date IN ({placeholders})"
                ") "
                "SELECT port_date, AVG(metric) AS baseline "
                "FROM ranked "
                "WHERE rn <= 25 AND metric IS NOT NULL "
                "GROUP BY port_date "
                "ORDER BY port_date"
            )
            baseline_rows = _execute_rows(baseline_sql, date_params)
        else:
            baseline_sql = (
                "WITH ranked AS ("
                "SELECT port_date, "
                f"{metric_col} AS metric, "
                "rank_index, port_weight, aiges_composite_average, "
                "ROW_NUMBER() OVER (PARTITION BY port_date ORDER BY "
                "CASE WHEN rank_index IS NULL THEN 999999 ELSE rank_index END, "
                "port_weight DESC NULLS LAST, "
                "aiges_composite_average DESC NULLS LAST) AS rn "
                "FROM tech11_ai_gov_eth_index "
                f"WHERE {date_filter}"
                ") "
                "SELECT port_date, AVG(metric) AS baseline "
                "FROM ranked "
                "WHERE rn <= 25 AND metric IS NOT NULL "
                "GROUP BY port_date "
                "ORDER BY port_date"
            )
            baseline_rows = _execute_rows(baseline_sql, baseline_params)

        baseline_map = {
            _to_date(row[0]).isoformat(): _float_or_none(row[1])
            for row in baseline_rows
            if _to_date(row[0]) is not None
        }
        if date_list:
            cache.set(
                _cache_key("baseline", metric_key, ",".join([d.isoformat() for d in date_list])),
                baseline_map,
                CACHE_TTLS["baseline"],
            )

    series: list[dict] = []
    for port_date, metric_value in company_rows:
        date_val = _to_date(port_date)
        if not date_val:
            continue
        date_key = date_val.isoformat()
        company_val = _float_or_none(metric_value)
        baseline_val = baseline_map.get(date_key)
        delta_val = None
        if company_val is not None and baseline_val is not None:
            delta_val = company_val - baseline_val
        series.append(
            {
                "date": date_key,
                "company": company_val,
                "baseline": baseline_val,
                "delta": delta_val,
            }
        )

    cache.set(series_key, series, CACHE_TTLS["series"])
    return series


def get_company_list() -> list[dict]:
    key = _cache_key("companies")
    cached = cache.get(key)
    if isinstance(cached, list):
        return cached

    sql = (
        "SELECT ticker, company_name, gics_sector "
        "FROM ("
        "SELECT ticker, company_name, gics_sector, "
        "ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY port_date DESC) AS rn "
        "FROM tech11_ai_gov_eth_index "
        "WHERE ticker IS NOT NULL"
        ") "
        "WHERE rn = 1 "
        "ORDER BY ticker"
    )
    rows = _execute_rows(sql, {})
    companies = [
        {"ticker": row[0], "company_name": row[1], "sector": row[2]} for row in rows if row[0]
    ]
    cache.set(key, companies, 900)
    return companies
