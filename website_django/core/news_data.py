from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import oracledb

from core.oracle_db import get_connection

NEWS_LIMIT_DEFAULT = 20
NEWS_LIMIT_MAX = 50
NEWS_DATE_RANGES = {"all": None, "7": 7, "30": 30, "90": 90, "365": 365}

_NEWS_SOURCE_CACHE: Dict[str, Dict[str, Any]] = {}


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


class NewsDataError(RuntimeError):
    pass


def _current_schema(conn) -> str:
    cur = conn.cursor()
    cur.execute("SELECT SYS_CONTEXT('USERENV','CURRENT_SCHEMA') FROM dual")
    row = cur.fetchone()
    return str(_to_plain(row[0]) or "").upper()


def _column_map(columns: List[str]) -> Dict[str, Optional[str]]:
    cols = {col.upper() for col in columns}

    def pick(options: List[str]) -> Optional[str]:
        for opt in options:
            if opt.upper() in cols:
                return opt.upper()
        return None

    return {
        "item_id": pick(["ITEM_ID", "NEWS_ID", "ID"]),
        "item_table": pick(["ITEM_TABLE", "TABLE_NAME", "ITEM_TABLE_NAME"]),
        "title": pick(["TITLE", "HEADLINE"]),
        "url": pick(["URL", "LINK", "SOURCE_URL"]),
        "published_at": pick(
            ["DT_PUB", "PUBLISHED_AT", "PUB_DATE", "PUBLISHED", "CREATED_AT", "CREATE_DATE"]
        ),
        "updated_at": pick(["UPDATED_AT", "UPDATE_DATE", "LAST_UPDATED", "LAST_UPDATE", "MODIFIED_AT"]),
        "source": pick(["SOURCE_NAME", "SOURCE"]),
        "body": pick(["BODY", "SUMMARY", "DESCRIPTION", "TEXT", "BODY_HTML", "FULL_TEXT", "CONTENT"]),
        "full_text": pick(["FULL_TEXT", "CONTENT", "BODY_HTML"]),
        "content": pick(["CONTENT"]),
        "article_text": pick(["ARTICLE_TEXT", "ARTICLE_BODY"]),
        "text": pick(["TEXT"]),
        "summary": pick(["SUMMARY", "DESCRIPTION"]),
        "tags": pick(["TAGS"]),
        "categories": pick(["CATEGORIES"]),
        "pillar_tags": pick(["PILLAR_TAGS"]),
        "tickers": pick(["TICKERS"]),
        "ticker": pick(["TICKER"]),
    }


def _score_candidate(name: str, mapping: Dict[str, Optional[str]], prefer_full: bool) -> int:
    score = 0
    if mapping.get("item_id"):
        score += 2
    if mapping.get("title"):
        score += 3
    if mapping.get("url"):
        score += 3
    if mapping.get("published_at"):
        score += 2
    if mapping.get("body"):
        score += 1
    if prefer_full and mapping.get("full_text"):
        score += 3
    preferred = {
        "V_NEWS_RECENT",
        "V_NEWS_ENRICHED",
        "V_NEWS_ALL",
        "NEWS_ITEMS",
        "NEWS",
        "SC_NEWS",
        "U_NEWS",
    }
    if name.upper() in preferred:
        score += 2
    return score


def _discover_objects(conn) -> Dict[str, Dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT table_name, column_name
        FROM user_tab_columns
        WHERE table_name LIKE '%NEWS%'
        """
    )
    rows = cur.fetchall()
    objects: Dict[str, List[str]] = {}
    if rows:
        for table_name, column_name in rows:
            name = str(_to_plain(table_name) or "").upper()
            col = str(_to_plain(column_name) or "").upper()
            objects.setdefault(name, []).append(col)
        current = _current_schema(conn)
        return {name: {"owner": current, "columns": cols} for name, cols in objects.items()}

    cur.execute(
        """
        SELECT owner, table_name, column_name
        FROM all_tab_columns
        WHERE table_name LIKE '%NEWS%'
        """
    )
    rows = cur.fetchall()
    for owner, table_name, column_name in rows:
        owner_val = str(_to_plain(owner) or "").upper()
        name = str(_to_plain(table_name) or "").upper()
        col = str(_to_plain(column_name) or "").upper()
        key = f"{owner_val}.{name}"
        objects.setdefault(key, []).append(col)
    return {name: {"owner": name.split(".")[0], "columns": cols} for name, cols in objects.items()}


def _resolve_source(conn, *, prefer_full: bool) -> Dict[str, Any]:
    objects = _discover_objects(conn)
    if not objects:
        raise NewsDataError("No NEWS objects found in Oracle schema.")

    has_full_text = False
    if prefer_full:
        for data in objects.values():
            mapping = _column_map(data["columns"])
            if mapping.get("full_text"):
                has_full_text = True
                break

    best: Optional[Dict[str, Any]] = None
    best_score = -1
    for name, data in objects.items():
        mapping = _column_map(data["columns"])
        if not mapping.get("item_id") or not mapping.get("title") or not mapping.get("url"):
            continue
        if prefer_full and has_full_text and not mapping.get("full_text"):
            continue
        score = _score_candidate(name.split(".")[-1], mapping, prefer_full)
        if score > best_score:
            best_score = score
            best = {
                "name": name,
                "owner": data["owner"],
                "columns": data["columns"],
                "mapping": mapping,
            }
    if not best:
        raise NewsDataError("No suitable NEWS source found with required columns.")
    return best


def _get_sources() -> Dict[str, Any]:
    cached = _NEWS_SOURCE_CACHE.get("sources")
    if cached:
        return cached
    with get_connection() as conn:
        list_source = _resolve_source(conn, prefer_full=False)
        detail_source = _resolve_source(conn, prefer_full=True)
        current_schema = _current_schema(conn)
    sources = {
        "list": list_source,
        "detail": detail_source,
        "schema": current_schema,
    }
    _NEWS_SOURCE_CACHE["sources"] = sources
    return sources


def _qualify_name(name: str, schema: str) -> str:
    if "." in name:
        return name
    if name.upper().startswith(schema.upper()):
        return name
    return f"{schema}.{name}"


def _build_filter_clauses(
    *,
    mapping: Dict[str, Optional[str]],
    days: Optional[int],
    source: Optional[str],
    tag: Optional[str],
    ticker: Optional[str],
) -> Tuple[str, Dict[str, Any]]:
    clauses: List[str] = ["WHERE 1 = 1"]
    binds: Dict[str, Any] = {}

    published_col = mapping.get("published_at")
    if days is not None and published_col:
        clauses.append(f"AND {published_col} >= (SYSDATE - :days)")
        binds["days"] = days

    source_col = mapping.get("source")
    if source and source_col:
        clauses.append(f"AND UPPER({source_col}) = UPPER(:source)")
        binds["source"] = source

    tag_col = mapping.get("tags")
    if tag and tag_col:
        clauses.append(f"AND UPPER({tag_col}) LIKE '%' || UPPER(:tag) || '%'")
        binds["tag"] = tag

    tickers_col = mapping.get("tickers") or mapping.get("ticker")
    if ticker and tickers_col:
        clauses.append(f"AND UPPER({tickers_col}) LIKE '%' || UPPER(:ticker) || '%'")
        binds["ticker"] = ticker

    return " ".join(clauses), binds


def fetch_filter_options() -> Dict[str, Any]:
    sources = _get_sources()
    list_source = sources["list"]
    mapping = list_source["mapping"]
    table_name = _qualify_name(list_source["name"], sources["schema"])

    source_col = mapping.get("source")
    tags_col = mapping.get("tags")
    tickers_col = mapping.get("tickers") or mapping.get("ticker")

    source_options: List[str] = []
    tag_options: List[str] = []

    with get_connection() as conn:
        cur = conn.cursor()
        if source_col:
            cur.execute(
                f"SELECT DISTINCT {source_col} FROM {table_name} WHERE {source_col} IS NOT NULL"
            )
            rows = cur.fetchall() or []
            source_options = sorted({str(_to_plain(row[0])).strip() for row in rows if row and row[0]})

        if tags_col:
            cur.execute(
                f"SELECT {tags_col} FROM {table_name} WHERE {tags_col} IS NOT NULL FETCH FIRST 200 ROWS ONLY"
            )
            rows = cur.fetchall() or []
            tags: List[str] = []
            for row in rows:
                if not row:
                    continue
                tags.extend(_parse_list(_to_plain(row[0])))
            tag_options = sorted({tag for tag in tags if tag})

    return {
        "source_options": source_options,
        "tag_options": tag_options,
        "supports_source": bool(source_col),
        "supports_tag": bool(tags_col),
        "supports_ticker": bool(tickers_col),
    }


def fetch_news_list(
    *,
    limit: Optional[int] = None,
    offset: int = 0,
    days: Optional[int] = None,
    source: Optional[str] = None,
    tag: Optional[str] = None,
    ticker: Optional[str] = None,
    date_range: Optional[str] = None,
) -> Dict[str, Any]:
    if date_range and date_range in NEWS_DATE_RANGES:
        days = NEWS_DATE_RANGES[date_range]
    effective_limit = _sanitize_limit(limit)
    offset_value = max(0, int(offset or 0))
    limit_plus_one = effective_limit + 1

    sources = _get_sources()
    list_source = sources["list"]
    mapping = list_source["mapping"]
    table_name = _qualify_name(list_source["name"], sources["schema"])

    where_clause, binds = _build_filter_clauses(
        mapping=mapping, days=days, source=source, tag=tag, ticker=ticker
    )
    binds.update({"offset": offset_value, "limit_plus_one": limit_plus_one})
    count_binds = {key: value for key, value in binds.items() if key not in {"offset", "limit_plus_one"}}

    item_id_col = mapping["item_id"]
    item_table_col = mapping.get("item_table")
    published_col = mapping.get("published_at")
    title_col = mapping.get("title")
    url_col = mapping.get("url")
    source_col = mapping.get("source")
    body_col = mapping.get("body")
    summary_col = mapping.get("summary")
    pillar_col = mapping.get("pillar_tags")
    categories_col = mapping.get("categories")
    tags_col = mapping.get("tags")
    tickers_col = mapping.get("tickers")
    ticker_col = mapping.get("ticker")
    full_text_col = mapping.get("full_text")

    if not all([item_id_col, title_col, url_col]):
        raise NewsDataError("News source missing required columns.")

    has_full_expr = "0"
    if full_text_col:
        has_full_expr = f"CASE WHEN {full_text_col} IS NOT NULL THEN 1 ELSE 0 END"

    sql = (
        f"SELECT {item_table_col or 'NULL'} AS item_table, {item_id_col} AS item_id, "
        f"{published_col or 'NULL'} AS dt_pub, {ticker_col or 'NULL'} AS ticker, "
        f"{title_col} AS title, {url_col} AS url, {source_col or 'NULL'} AS source_name, "
        f"{body_col or 'NULL'} AS body, {summary_col or 'NULL'} AS summary, "
        f"{pillar_col or 'NULL'} AS pillar_tags, "
        f"{categories_col or 'NULL'} AS categories, {tags_col or 'NULL'} AS tags, "
        f"{tickers_col or 'NULL'} AS tickers, {has_full_expr} AS has_full_body "
        f"FROM {table_name} r "
        f"{where_clause} "
        f"ORDER BY {published_col or title_col} DESC "
        "OFFSET :offset ROWS FETCH NEXT :limit_plus_one ROWS ONLY"
    )

    count_sql = f"SELECT COUNT(*) FROM {table_name} r {where_clause}"

    items: List[Dict[str, Any]] = []
    total_count = 0
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(count_sql, count_binds)
        count_row = cur.fetchone()
        if count_row:
            total_count = int(_to_plain(count_row[0]) or 0)

        cur.execute(sql, binds)
        raw_rows = cur.fetchall() or []
        rows = [[_to_plain(val) for val in row] for row in raw_rows]

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
            summary_value,
            pillar_tags,
            categories,
            tags_raw,
            tickers_raw,
            has_full_body,
        ) = raw

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
                "summary": (summary_value or body or ""),
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
    sources = _get_sources()
    detail_source = sources["detail"]
    mapping = detail_source["mapping"]
    table_name = _qualify_name(detail_source["name"], sources["schema"])

    table, item_id = _parse_item_key(news_id)
    if item_id is None:
        return {"item": None, "error": "Invalid news identifier."}

    item_id_col = mapping["item_id"]
    item_table_col = mapping.get("item_table")
    published_col = mapping.get("published_at")
    title_col = mapping.get("title")
    url_col = mapping.get("url")
    source_col = mapping.get("source")
    body_col = mapping.get("body")
    full_text_col = mapping.get("full_text")
    content_col = mapping.get("content")
    article_text_col = mapping.get("article_text")
    text_col = mapping.get("text")
    summary_col = mapping.get("summary")
    pillar_col = mapping.get("pillar_tags")
    categories_col = mapping.get("categories")
    tags_col = mapping.get("tags")
    tickers_col = mapping.get("tickers")
    ticker_col = mapping.get("ticker")

    if not item_id_col or not title_col:
        raise NewsDataError("News detail source missing required columns.")

    full_text_select = full_text_col or "NULL"
    content_select = content_col or "NULL"
    article_text_select = article_text_col or "NULL"
    text_select = text_col or "NULL"
    summary_select = summary_col or "NULL"

    sql = (
        f"SELECT {item_table_col or 'NULL'} AS item_table, {item_id_col} AS item_id, "
        f"{published_col or 'NULL'} AS dt_pub, {ticker_col or 'NULL'} AS ticker, "
        f"{title_col} AS title, {url_col or 'NULL'} AS url, {source_col or 'NULL'} AS source_name, "
        f"{body_col or 'NULL'} AS body, {full_text_select} AS full_text, "
        f"{content_select} AS content, {article_text_select} AS article_text, "
        f"{text_select} AS text, {summary_select} AS summary, "
        f"{pillar_col or 'NULL'} AS pillar_tags, {categories_col or 'NULL'} AS categories, "
        f"{tags_col or 'NULL'} AS tags, {tickers_col or 'NULL'} AS tickers "
        f"FROM {table_name} e "
        f"WHERE {item_id_col} = :item_id "
    )
    binds: Dict[str, Any] = {"item_id": item_id}
    if table and item_table_col:
        sql += f"AND {item_table_col} = :item_table "
        binds["item_table"] = table

    try:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute(sql, binds)
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
                content,
                article_text,
                text,
                summary_value,
                pillar_tags,
                categories,
                tags_raw,
                tickers_raw,
            ) = [_to_plain(val) for val in row]
    except Exception as exc:
        raise NewsDataError("Unable to load news detail.") from exc

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
            "summary": summary_value or body or "",
            "body": body,
            "full_text": full_body,
            "content": content,
            "article_text": article_text,
            "text": text,
            "tags": parsed_tags,
            "categories": parsed_categories,
            "pillar_tags": parsed_pillar_tags,
            "ticker": ticker_value or None,
            "published_at": _format_timestamp(dt_pub),
            "has_full_body": bool(full_body),
        },
        "error": None,
    }


def fetch_news_sitemap_items(limit: Optional[int] = None) -> List[Dict[str, Any]]:
    effective_limit = _sanitize_limit(limit) if limit is not None else None
    sources = _get_sources()
    list_source = sources["list"]
    mapping = list_source["mapping"]
    table_name = _qualify_name(list_source["name"], sources["schema"])

    item_id_col = mapping.get("item_id")
    item_table_col = mapping.get("item_table")
    published_col = mapping.get("published_at")
    updated_col = mapping.get("updated_at")

    if not item_id_col:
        raise NewsDataError("News source missing required columns.")

    timestamp_col = updated_col or published_col or item_id_col

    sql = (
        f"SELECT {item_table_col or 'NULL'} AS item_table, {item_id_col} AS item_id, "
        f"{timestamp_col} AS dt_updated, {published_col or 'NULL'} AS dt_pub "
        f"FROM {table_name} "
        f"WHERE {item_id_col} IS NOT NULL "
        f"ORDER BY {timestamp_col} DESC NULLS LAST"
    )
    if effective_limit:
        sql += " FETCH FIRST :limit ROWS ONLY"

    binds: Dict[str, Any] = {}
    if effective_limit:
        binds["limit"] = effective_limit

    items: List[Dict[str, Any]] = []
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, binds)
        rows = cur.fetchall() or []

    for row in rows:
        item_table, item_id, dt_updated, dt_pub = [_to_plain(val) for val in row]
        if item_id is None:
            continue
        if item_table:
            item_id_str = f"{item_table}:{item_id}"
        else:
            item_id_str = str(item_id)
        lastmod_value = _format_timestamp(dt_updated) or _format_timestamp(dt_pub)
        items.append(
            {
                "news_id": item_id_str,
                "lastmod": lastmod_value,
            }
        )

    return items


__all__ = [
    "fetch_news_list",
    "fetch_news_detail",
    "fetch_filter_options",
    "fetch_news_sitemap_items",
    "NewsDataError",
]
