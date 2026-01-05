from __future__ import annotations

from datetime import datetime, timezone
import re
from typing import Any, Dict, Iterable, List, Optional, Sequence

import oracledb
from django.conf import settings

from core.news_html import sanitize_news_html, summarize_html
from core.oracle_db import get_connection


class NewsStorageError(RuntimeError):
    def __init__(self, message: str, code: str | None = None) -> None:
        super().__init__(message)
        self.code = code


def _is_oracle_missing_table(exc: Exception, table_name: str) -> bool:
    message = str(exc)
    if "ORA-00942" not in message:
        return False
    return table_name.upper() in message.upper()


def _is_oracle_missing_column(exc: Exception, column_name: str) -> bool:
    message = str(exc)
    if "ORA-00904" not in message:
        return False
    return column_name.upper() in message.upper()


def _parse_tags(value: str | Iterable[str] | None) -> List[str]:
    if not value:
        return []
    if isinstance(value, str):
        candidates = value.replace(";", ",").split(",")
    else:
        candidates = value
    parsed: List[str] = []
    for candidate in candidates:
        text = str(candidate).strip()
        if text:
            parsed.append(text)
    seen = set()
    deduped: List[str] = []
    for tag in parsed:
        key = tag.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(tag)
    return deduped


def _ensure_lookup_id(cur, table: str, id_col: str, name_col: str, value: str) -> Optional[int]:
    cur.execute(
        f"SELECT {id_col} FROM {table} WHERE LOWER({name_col}) = LOWER(:name)",
        {"name": value},
    )
    existing = cur.fetchone()
    if existing:
        return int(existing[0])

    id_var = cur.var(int) if hasattr(cur, "var") else None
    cur.execute(
        f"INSERT INTO {table} ({name_col}) VALUES (:name) RETURNING {id_col} INTO :id_out",
        {"name": value, "id_out": id_var},
    )
    if id_var is not None:
        returned = id_var.getvalue()
        if isinstance(returned, list):
            return int(returned[0])
    row = cur.fetchone() if hasattr(cur, "fetchone") else None
    return int(row[0]) if row else None


def _insert_mapping(cur, table: str, columns: List[str], values: Dict[str, Any]) -> None:
    cols = ", ".join(columns)
    cols_hint = ",".join(columns)
    binds = ", ".join(f":{col}" for col in columns)
    sql = f"INSERT /*+ ignore_row_on_dupkey_index({table} ({cols_hint})) */ INTO {table} ({cols}) VALUES ({binds})"
    cur.execute(sql, values)


def _extract_asset_ids(html: str) -> List[int]:
    ids: List[int] = []
    for match in re.findall(r"/news/assets/(\\d+)/", html or ""):
        try:
            ids.append(int(match))
        except ValueError:
            continue
    return ids


def _link_assets(cur, news_id: int, asset_ids: Sequence[int]) -> None:
    if not asset_ids:
        return
    unique_ids = sorted({int(asset_id) for asset_id in asset_ids})
    for asset_id in unique_ids:
        cur.execute(
            "UPDATE news_assets SET news_id = :news_id WHERE asset_id = :asset_id",
            {"news_id": news_id, "asset_id": asset_id},
        )


def create_news_post(*, headline: str, tags: str | Iterable[str] | None, body_html: str) -> Dict[str, Any]:
    headline = (headline or "").strip()
    if not headline:
        raise ValueError("headline is required")

    body_html = (body_html or "").strip()
    if not body_html:
        raise ValueError("body is required")

    sanitized_html = sanitize_news_html(body_html)
    summary = summarize_html(sanitized_html)
    parsed_tags = _parse_tags(tags)
    published_at = datetime.now(timezone.utc)
    source = "SustainaCore"
    site_url = settings.SITE_URL.rstrip("/")

    asset_ids = _extract_asset_ids(sanitized_html)

    try:
        with get_connection() as conn:
            cur = conn.cursor()
            id_var = cur.var(oracledb.NUMBER)
            cur.setinputsizes(
                summary=oracledb.DB_TYPE_CLOB,
                body_html=oracledb.DB_TYPE_CLOB,
            )
            cur.execute(
                """
                INSERT INTO news_items (dt_pub, ticker, title, url, source, summary, pillar_tags, body_html)
                VALUES (:dt_pub, :ticker, :title, :url, :source, :summary, :pillar_tags, :body_html)
                RETURNING id INTO :id_out
                """,
                {
                    "dt_pub": published_at,
                    "ticker": None,
                    "title": headline,
                    "url": f"{site_url}/news/pending/",
                    "source": source,
                    "summary": summary,
                    "pillar_tags": None,
                    "body_html": sanitized_html,
                    "id_out": id_var,
                },
            )
            new_id = int(id_var.getvalue()[0])
            detail_url = f"{site_url}/news/NEWS_ITEMS:{new_id}/"
            cur.execute(
                "UPDATE news_items SET url = :url WHERE id = :id",
                {"url": detail_url, "id": new_id},
            )
            _link_assets(cur, new_id, asset_ids)

            item_table = "NEWS_ITEMS"
            if parsed_tags:
                tag_ids: List[int] = []
                for tag in parsed_tags:
                    tag_id = _ensure_lookup_id(cur, "NEWS_TAGS", "TAG_ID", "NAME", tag)
                    if tag_id is not None:
                        tag_ids.append(tag_id)
                for tag_id in tag_ids:
                    _insert_mapping(
                        cur,
                        "NEWS_ITEM_TAGS",
                        ["item_table", "item_id", "tag_id"],
                        {"item_table": item_table, "item_id": new_id, "tag_id": tag_id},
                    )

            conn.commit()
    except oracledb.DatabaseError as exc:
        if _is_oracle_missing_column(exc, "BODY_HTML"):
            raise NewsStorageError(
                "BODY_HTML column is missing. Apply migration V0004__news_rich_body.sql.",
                code="missing_body_html",
            ) from exc
        raise

    return {
        "id": f"{item_table}:{new_id}",
        "item_table": item_table,
        "item_id": new_id,
        "title": headline,
        "source": source,
        "url": detail_url,
        "summary": summary,
        "tags": parsed_tags,
        "published_at": published_at.isoformat().replace("+00:00", "Z"),
    }


def create_news_asset(
    *, news_id: Optional[int], file_name: str | None, mime_type: str | None, file_bytes: bytes
) -> int:
    try:
        with get_connection() as conn:
            cur = conn.cursor()
            id_var = cur.var(oracledb.NUMBER)
            cur.execute(
                """
                INSERT INTO news_assets (news_id, file_name, mime_type, file_blob, created_at)
                VALUES (:news_id, :file_name, :mime_type, :file_blob, SYSTIMESTAMP)
                RETURNING asset_id INTO :asset_id
                """,
                {
                    "news_id": news_id,
                    "file_name": file_name,
                    "mime_type": mime_type,
                    "file_blob": oracledb.Binary(file_bytes),
                    "asset_id": id_var,
                },
            )
            conn.commit()
            return int(id_var.getvalue()[0])
    except oracledb.DatabaseError as exc:
        if _is_oracle_missing_table(exc, "NEWS_ASSETS"):
            raise NewsStorageError(
                "NEWS_ASSETS table is missing. Apply migration V0004__news_rich_body.sql.",
                code="missing_news_assets",
            ) from exc
        raise


def get_news_asset(asset_id: int) -> Optional[Dict[str, Any]]:
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT asset_id, news_id, file_name, mime_type, file_blob
            FROM news_assets
            WHERE asset_id = :asset_id
            """,
            {"asset_id": asset_id},
        )
        row = cur.fetchone()
    if not row:
        return None

    file_blob = row[4].read() if hasattr(row[4], "read") else row[4]
    news_id_value = row[1]
    if news_id_value is not None:
        news_id_value = int(news_id_value)
    return {
        "asset_id": int(row[0]),
        "news_id": news_id_value,
        "file_name": row[2],
        "mime_type": row[3],
        "file_blob": file_blob,
    }
