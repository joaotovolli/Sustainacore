"""Shared helpers for querying news items and curated entries."""

from __future__ import annotations

import logging
import re
from datetime import date, datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from db_helper import _to_plain, get_connection

LOGGER = logging.getLogger("sustainacore.news")

_DEFAULT_LIMIT = 20
_MAX_LIMIT = 100
_MAX_DAYS = 365


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


def _parse_list(raw_value: Any) -> List[str]:
    if isinstance(raw_value, str):
        normalized = raw_value.replace(";", ",")
        candidates: Sequence[Any] = normalized.split(",")
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


def _parse_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            return None
    return None


def _ensure_lookup_id(cur, table: str, id_col: str, name_col: str, value: str) -> Optional[int]:
    cur.execute(
        f"SELECT {id_col} FROM {table} WHERE LOWER({name_col}) = LOWER(:name)",
        {"name": value},
    )
    existing = cur.fetchone()
    if existing:
        return _to_plain(existing[0])

    id_var = cur.var(int) if hasattr(cur, "var") else None
    cur.execute(
        f"INSERT INTO {table} ({name_col}) VALUES (:name) RETURNING {id_col} INTO :id_out",
        {"name": value, "id_out": id_var},
    )
    if id_var is not None:
        returned = id_var.getvalue()
        if isinstance(returned, Sequence):
            return _to_plain(returned[0])
    row = cur.fetchone() if hasattr(cur, "fetchone") else None
    return _to_plain(row[0]) if row else None


def _insert_mapping(cur, table: str, columns: Sequence[str], values: Dict[str, Any]) -> None:
    cols = ", ".join(columns)
    cols_hint = ",".join(columns)
    binds = ", ".join(f":{col}" for col in columns)
    sql = f"INSERT /*+ ignore_row_on_dupkey_index({table} ({cols_hint})) */ INTO {table} ({cols}) VALUES ({binds})"
    cur.execute(sql, values)


def create_curated_news_item(payload: Dict[str, Any]) -> Dict[str, Any]:
    title = (payload.get("title") or "").strip()
    url = (payload.get("url") or "").strip()
    source = (payload.get("source") or "Manual").strip()
    summary = (payload.get("summary") or "").strip()
    pillar_tags = _parse_list(payload.get("pillar_tags"))
    categories = _parse_list(payload.get("categories"))
    tags = _parse_list(payload.get("tags"))
    tickers = _parse_list(payload.get("tickers"))

    if not title:
        raise ValueError("title is required")
    if not url:
        raise ValueError("url is required")
    if not source:
        raise ValueError("source is required")

    dt_pub = _parse_datetime(payload.get("dt_pub")) or datetime.now(timezone.utc)
    pillar_tags_str = ", ".join(pillar_tags) if pillar_tags else None
    main_ticker = tickers[0] if tickers else None

    with get_connection() as conn:
        cur = conn.cursor()
        id_var = cur.var(int) if hasattr(cur, "var") else None
        cur.execute(
            "INSERT INTO news_items (dt_pub, ticker, title, url, source, summary, pillar_tags) "
            "VALUES (:dt_pub, :ticker, :title, :url, :source, :summary, :pillar_tags) "
            "RETURNING id INTO :id_out",
            {
                "dt_pub": dt_pub,
                "ticker": main_ticker,
                "title": title,
                "url": url,
                "source": source,
                "summary": summary,
                "pillar_tags": pillar_tags_str,
                "id_out": id_var,
            },
        )
        if id_var is not None:
            new_id = _to_plain(id_var.getvalue()[0])
        else:  # pragma: no cover - defensive
            row = cur.fetchone()
            new_id = _to_plain(row[0]) if row else None

        if new_id is None:
            raise RuntimeError("news_item_insert_failed")

        item_table = "NEWS_ITEMS"
        if tags:
            tag_ids: List[int] = []
            for tag in tags:
                tag_id = _ensure_lookup_id(cur, "NEWS_TAGS", "TAG_ID", "NAME", tag)
                if tag_id is not None:
                    tag_ids.append(tag_id)
            for tag_id in tag_ids:
                _insert_mapping(
                    cur,
                    "NEWS_ITEM_TAGS",
                    ("item_table", "item_id", "tag_id"),
                    {"item_table": item_table, "item_id": new_id, "tag_id": tag_id},
                )

        if categories:
            cat_ids: List[int] = []
            for category in categories:
                cat_id = _ensure_lookup_id(
                    cur, "NEWS_CATEGORIES", "CATEGORY_ID", "NAME", category
                )
                if cat_id is not None:
                    cat_ids.append(cat_id)
            for cat_id in cat_ids:
                _insert_mapping(
                    cur,
                    "NEWS_ITEM_CATEGORIES",
                    ("item_table", "item_id", "category_id"),
                    {"item_table": item_table, "item_id": new_id, "category_id": cat_id},
                )

        if tickers:
            for ticker_value in tickers:
                _insert_mapping(
                    cur,
                    "NEWS_ITEM_TICKERS",
                    ("item_table", "item_id", "ticker"),
                    {
                        "item_table": item_table,
                        "item_id": new_id,
                        "ticker": ticker_value,
                    },
                )

        conn.commit()

    return {
        "id": f"{item_table}:{new_id}",
        "item_table": item_table,
        "item_id": new_id,
        "title": title,
        "source": source,
        "url": url,
        "summary": summary,
        "tags": tags,
        "categories": categories,
        "pillar_tags": pillar_tags,
        "tickers": tickers,
        "published_at": _format_timestamp(dt_pub),
        "company": main_ticker,
    }
