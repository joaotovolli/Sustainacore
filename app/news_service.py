"""News helper utilities for the /api/news endpoint."""

from __future__ import annotations

import re
from datetime import datetime, timezone, date
from typing import Any, Dict, List, Optional, Sequence, Tuple

from db_helper import _to_plain, get_connection


def _parse_list(raw: Any) -> List[str]:
    """Split comma/semicolon-delimited strings into a clean list."""

    if raw is None:
        return []
    if isinstance(raw, (list, tuple)):
        return [str(val).strip() for val in raw if str(val).strip()]
    if isinstance(raw, str):
        parts = re.split(r"[;,]", raw)
        return [part.strip() for part in parts if part.strip()]
    return []


def _format_timestamp(value: Any) -> Optional[str]:
    """Normalize timestamp-like values to ISO 8601 strings in UTC with Z suffix."""

    if isinstance(value, datetime):
        ts = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return ts.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        ts = datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
        return ts.isoformat().replace("+00:00", "Z")
    if isinstance(value, str):
        candidate = value.strip()
        if not candidate:
            return None
        try:
            parsed = datetime.fromisoformat(candidate)
        except ValueError:
            return candidate
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return None


def _sanitize_limit(raw_limit: Optional[int], default: int = 20, max_limit: int = 100) -> int:
    """Clamp the requested limit to safe bounds."""

    try:
        limit = int(raw_limit) if raw_limit is not None else default
    except (TypeError, ValueError):
        limit = default
    if limit < 1:
        limit = 1
    if limit > max_limit:
        limit = max_limit
    return limit


def _build_news_sql(
    *,
    days: Optional[int],
    source: Optional[str],
    tags: Sequence[str],
    ticker: Optional[str],
    limit: int,
) -> Tuple[str, Dict[str, Any]]:
    """Build the news query against v_news_recent with optional filters."""

    clauses: List[str] = ["WHERE 1 = 1"]
    binds: Dict[str, Any] = {}

    if days is not None:
        clauses.append("AND dt_pub >= (SYSDATE - :days)")
        binds["days"] = days

    if source:
        clauses.append("AND UPPER(source_name) = UPPER(:source)")
        binds["source"] = source

    tag_binds: List[str] = []
    for idx, tag in enumerate(tags or []):
        key = f"tag_{idx}"
        tag_binds.append(f"UPPER(tags) LIKE '%' || UPPER(:{key}) || '%'")
        binds[key] = tag
    if tag_binds:
        clauses.append("AND (" + " OR ".join(tag_binds) + ")")

    if ticker:
        clauses.append("AND UPPER(tickers) LIKE '%' || UPPER(:ticker) || '%'")
        binds["ticker"] = ticker

    limit_plus_one = limit + 1
    binds["limit_plus_one"] = limit_plus_one

    sql = (
        "SELECT item_table, item_id, dt_pub, ticker, title, url, source_name, body, "
        "pillar_tags, categories, tags, tickers "
        "FROM v_news_recent "
        f"{' '.join(clauses)} "
        "ORDER BY dt_pub DESC "
        "FETCH FIRST :limit_plus_one ROWS ONLY"
    )

    return sql, binds


def fetch_news_items(
    *,
    limit: Optional[int] = None,
    days: Optional[int] = None,
    source: Optional[str] = None,
    tags: Optional[Sequence[str]] = None,
    ticker: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], bool, int]:
    """
    Fetch news rows from Oracle, applying filters and limit/has_more semantics.
    Returns (items, has_more, effective_limit).
    """

    effective_limit = _sanitize_limit(limit)
    tags = tags or []

    sql, binds = _build_news_sql(
        days=days, source=source, tags=tags, ticker=ticker, limit=effective_limit
    )

    items: List[Dict[str, Any]] = []
    has_more = False

    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, binds)
        rows = cur.fetchall() or []

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
            ) = [_to_plain(val) for val in raw]

            parsed_tags = _parse_list(tags_raw)
            parsed_categories = _parse_list(categories)
            parsed_pillar_tags = _parse_list(pillar_tags)
            parsed_tickers = _parse_list(tickers_raw)

            ticker_value = ", ".join(parsed_tickers) if parsed_tickers else (
                ticker_single or tickers_raw or ""
            )

            item_id_value = None
            if item_table and item_id:
                item_id_value = f"{item_table}:{item_id}"
            elif item_id is not None:
                item_id_value = str(item_id)
            elif item_table:
                item_id_value = str(item_table)

            items.append(
                {
                    "id": item_id_value,
                    "title": title,
                    "source": source_name,
                    "url": url,
                    "summary": body,
                    "tags": parsed_tags,
                    "categories": parsed_categories,
                    "pillar_tags": parsed_pillar_tags,
                    "ticker": ticker_value or None,
                    "published_at": _format_timestamp(dt_pub),
                }
            )

    return items, has_more, effective_limit


__all__ = ["_build_news_sql", "fetch_news_items"]
