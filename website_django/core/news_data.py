from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import oracledb

from core.oracle_db import get_connection

NEWS_LIMIT_DEFAULT = 20
NEWS_LIMIT_MAX = 50


def _to_plain(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, oracledb.LOB):
        return value.read()
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            return value.decode("latin-1", errors="ignore")
    return value


def _parse_list(value: object) -> List[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        candidates = value
    else:
        candidates = str(value).replace(";", ",").split(",")
    parsed: List[str] = []
    for candidate in candidates:
        text = str(candidate).strip()
        if text:
            parsed.append(text)
    return parsed


def _sanitize_limit(value: Optional[int]) -> int:
    if value is None:
        return NEWS_LIMIT_DEFAULT
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return NEWS_LIMIT_DEFAULT
    return max(1, min(parsed, NEWS_LIMIT_MAX))


def _format_timestamp(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    try:
        return datetime.fromisoformat(str(value)).isoformat()
    except (TypeError, ValueError):
        return None


def _build_filter_clauses(
    *, days: Optional[int], source: Optional[str], tag: Optional[str], ticker: Optional[str]
) -> Tuple[str, Dict[str, Any]]:
    clauses: List[str] = ["WHERE 1 = 1"]
    binds: Dict[str, Any] = {}

    if days is not None:
        clauses.append("AND r.dt_pub >= (SYSDATE - :days)")
        binds["days"] = days

    if source:
        clauses.append("AND UPPER(r.source_name) = UPPER(:source)")
        binds["source"] = source

    if tag:
        clauses.append("AND UPPER(r.tags) LIKE '%' || UPPER(:tag) || '%'")
        binds["tag"] = tag

    if ticker:
        clauses.append("AND UPPER(r.tickers) LIKE '%' || UPPER(:ticker) || '%'")
        binds["ticker"] = ticker

    return " ".join(clauses), binds


def fetch_news_list(
    *,
    limit: Optional[int] = None,
    offset: int = 0,
    days: Optional[int] = None,
    source: Optional[str] = None,
    tag: Optional[str] = None,
    ticker: Optional[str] = None,
) -> Dict[str, Any]:
    effective_limit = _sanitize_limit(limit)
    offset_value = max(0, int(offset or 0))
    limit_plus_one = effective_limit + 1

    where_clause, binds = _build_filter_clauses(
        days=days, source=source, tag=tag, ticker=ticker
    )
    binds.update({"offset": offset_value, "limit_plus_one": limit_plus_one})

    sql = (
        "SELECT r.item_table, r.item_id, r.dt_pub, r.ticker, r.title, r.url, "
        "r.source_name, r.body, r.pillar_tags, r.categories, r.tags, r.tickers, "
        "CASE WHEN a.full_text IS NOT NULL THEN 1 ELSE 0 END AS has_full_body "
        "FROM v_news_recent r "
        "LEFT JOIN v_news_all a ON a.item_table = r.item_table AND a.item_id = r.item_id "
        f"{where_clause} "
        "ORDER BY r.dt_pub DESC "
        "OFFSET :offset ROWS FETCH NEXT :limit_plus_one ROWS ONLY"
    )

    count_sql = "SELECT COUNT(*) FROM v_news_recent r " f"{where_clause}"

    items: List[Dict[str, Any]] = []
    total_count = 0
    try:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute(count_sql, binds)
            count_row = cur.fetchone()
            if count_row:
                total_count = int(_to_plain(count_row[0]) or 0)

            cur.execute(sql, binds)
            rows = cur.fetchall() or []
    except Exception:
        return {"items": [], "meta": {}, "error": "Unable to load news data."}

    has_more = len(rows) > effective_limit
    trimmed = rows[:effective_limit]

    for raw in trimmed:
        (
            item_table,
            item_id,
            dt_pub,
            ticker_single,
            title,
            url,
            source_name,
            body,
            pillar_tags,
            categories,
            tags_raw,
            tickers_raw,
            has_full_body,
        ) = [_to_plain(val) for val in raw]

        parsed_tags = _parse_list(tags_raw)
        parsed_categories = _parse_list(categories)
        parsed_pillar_tags = _parse_list(pillar_tags)
        parsed_tickers = _parse_list(tickers_raw)

        ticker_value = ", ".join(parsed_tickers) if parsed_tickers else (
            ticker_single or tickers_raw or ""
        )

        item_id_str = None
        if item_table and item_id is not None:
            item_id_str = f"{item_table}:{item_id}"
        elif item_id is not None:
            item_id_str = str(item_id)

        items.append(
            {
                "id": item_id_str,
                "title": title,
                "source": source_name,
                "url": url,
                "summary": body or "",
                "tags": parsed_tags,
                "categories": parsed_categories,
                "pillar_tags": parsed_pillar_tags,
                "ticker": ticker_value or None,
                "published_at": _format_timestamp(dt_pub),
                "has_full_body": bool(has_full_body),
            }
        )

    meta = {
        "count": offset_value + len(items),
        "total": total_count,
        "has_more": has_more,
        "limit": effective_limit,
        "offset": offset_value,
    }
    return {"items": items, "meta": meta, "error": None}


def _parse_item_key(item_key: str) -> Tuple[Optional[str], Optional[Any]]:
    if not item_key:
        return None, None
    if ":" in item_key:
        table, raw_id = item_key.split(":", 1)
        table = table.strip().upper() or None
    else:
        table = None
        raw_id = item_key
    raw_id = (raw_id or "").strip()
    if not raw_id:
        return table, None
    try:
        item_id: Any = int(raw_id)
    except ValueError:
        item_id = raw_id
    return table, item_id


def fetch_news_detail(*, news_id: str) -> Dict[str, Any]:
    table, item_id = _parse_item_key(news_id)
    if item_id is None:
        return {"item": None, "error": "Invalid news identifier."}

    sql_enriched = (
        "SELECT e.item_table, e.item_id, e.dt_pub, e.ticker, e.title, e.url, "
        "e.source_name, e.body, a.full_text, e.pillar_tags, e.categories, e.tags, e.tickers "
        "FROM v_news_enriched e "
        "LEFT JOIN v_news_all a ON a.item_table = e.item_table AND a.item_id = e.item_id "
        "WHERE e.item_id = :item_id "
    )
    sql_recent = (
        "SELECT r.item_table, r.item_id, r.dt_pub, r.ticker, r.title, r.url, "
        "r.source_name, r.body, a.full_text, r.pillar_tags, r.categories, r.tags, r.tickers "
        "FROM v_news_recent r "
        "LEFT JOIN v_news_all a ON a.item_table = r.item_table AND a.item_id = r.item_id "
        "WHERE r.item_id = :item_id "
    )
    binds: Dict[str, Any] = {"item_id": item_id}
    if table:
        sql_enriched += "AND e.item_table = :item_table "
        sql_recent += "AND r.item_table = :item_table "
        binds["item_table"] = table

    try:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute(sql_enriched, binds)
            row = cur.fetchone()
            if not row:
                cur.execute(sql_recent, binds)
                row = cur.fetchone()
            if not row:
                return {"item": None, "error": "News item not found."}

            (
                item_table,
                item_id_value,
                dt_pub,
                ticker_single,
                title,
                url,
                source_name,
                body,
                full_text,
                pillar_tags,
                categories,
                tags_raw,
                tickers_raw,
            ) = [_to_plain(val) for val in row]
    except Exception:
        return {"item": None, "error": "Unable to load news data."}

    parsed_tags = _parse_list(tags_raw)
    parsed_categories = _parse_list(categories)
    parsed_pillar_tags = _parse_list(pillar_tags)
    parsed_tickers = _parse_list(tickers_raw)

    ticker_value = ", ".join(parsed_tickers) if parsed_tickers else (
        ticker_single or tickers_raw or ""
    )

    item_id_str = None
    if item_table and item_id_value is not None:
        item_id_str = f"{item_table}:{item_id_value}"
    elif item_id_value is not None:
        item_id_str = str(item_id_value)

    full_body = full_text or None

    return {
        "item": {
            "id": item_id_str,
            "title": title,
            "source": source_name,
            "url": url,
            "summary": body or "",
            "body": full_body,
            "full_text": full_body,
            "tags": parsed_tags,
            "categories": parsed_categories,
            "pillar_tags": parsed_pillar_tags,
            "ticker": ticker_value or None,
            "published_at": _format_timestamp(dt_pub),
            "has_full_body": bool(full_body),
        },
        "error": None,
    }


__all__ = ["fetch_news_list", "fetch_news_detail"]
