"""Shared helpers for querying TECH100 news items."""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from db_helper import _to_plain, get_connection

LOGGER = logging.getLogger("sustainacore.news")

_DEFAULT_LIMIT = 20
_MAX_LIMIT = 100
_DEFAULT_DAYS = 30
_MAX_DAYS = 365


def _format_timestamp(value: Any) -> Optional[str]:
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


def _coerce_int(value: Any, default: int, max_value: int) -> int:
    try:
        coerced = int(value)
    except Exception:
        coerced = default
    if coerced < 1:
        coerced = default
    if coerced > max_value:
        coerced = max_value
    return coerced


def _normalize_tags(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        iterable: Iterable[Any] = value
    else:
        iterable = [value]

    normalized: List[str] = []
    for item in iterable:
        if item is None:
            continue
        if isinstance(item, str):
            parts = item.split(",")
        else:
            parts = [str(item)]
        for part in parts:
            candidate = part.strip()
            if candidate:
                normalized.append(candidate)
    return normalized


def _parse_tags(raw_value: Any) -> List[str]:
    if isinstance(raw_value, str):
        candidates = raw_value.split(",")
    elif isinstance(raw_value, Sequence):
        candidates = raw_value
    else:
        candidates = []

    parsed: List[str] = []
    for candidate in candidates:
        text = (candidate or "").strip()
        if not text:
            continue
        parsed.append(text)
    return parsed


def _build_news_sql(days: int, source: Optional[str], tags: List[str], limit: int) -> Tuple[str, Dict[str, Any]]:
    clauses = ["dt_pub >= (SYSDATE - :days)"]
    binds: Dict[str, Any] = {"days": days}

    if source:
        clauses.append("LOWER(source) = LOWER(:source)")
        binds["source"] = source

    tag_clauses: List[str] = []
    for idx, tag in enumerate(tags):
        bind_key = f"tag_{idx}"
        binds[bind_key] = f"%{tag.lower()}%"
        tag_clauses.append(f"LOWER(pillar_tags) LIKE :{bind_key}")
    if tag_clauses:
        clauses.append("(" + " OR ".join(tag_clauses) + ")")

    where_clause = " WHERE " + " AND ".join(clauses) if clauses else ""
    sql = (
        "SELECT id, dt_pub, ticker, title, url, source, summary, pillar_tags "
        "FROM v_tech100_news"
        f"{where_clause} "
        "ORDER BY dt_pub DESC "
        f"FETCH FIRST {limit + 1} ROWS ONLY"
    )
    return sql, binds


def fetch_news_items(
    *,
    limit: Any = None,
    days: Any = None,
    source: Optional[str] = None,
    tags: Optional[Iterable[str]] = None,
) -> Tuple[List[Dict[str, Any]], bool, int]:
    """Query TECH100 news with optional filters.

    Returns a tuple of (items, has_more, effective_limit).
    """

    effective_limit = _coerce_int(limit, _DEFAULT_LIMIT, _MAX_LIMIT)
    effective_days = _coerce_int(days, _DEFAULT_DAYS, _MAX_DAYS)
    normalized_source = source.strip() if isinstance(source, str) else None
    normalized_tags = _normalize_tags(tags)

    sql, binds = _build_news_sql(effective_days, normalized_source, normalized_tags, effective_limit)

    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, binds)
        columns = [desc[0].lower() for desc in cur.description]
        rows = cur.fetchall()

    has_more = len(rows) > effective_limit
    items: List[Dict[str, Any]] = []
    for raw in rows[:effective_limit]:
        row = {col: _to_plain(val) for col, val in zip(columns, raw)}
        raw_tags = row.get("pillar_tags")
        tags_list = _parse_tags(raw_tags)
        items.append(
            {
                "id": row.get("id"),
                "title": row.get("title"),
                "source": row.get("source"),
                "url": row.get("url"),
                "summary": row.get("summary"),
                "tags": tags_list,
                "published_at": _format_timestamp(row.get("dt_pub")),
                "company": row.get("ticker"),
            }
        )

    return items, has_more, effective_limit
