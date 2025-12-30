#!/usr/bin/env python3
"""Targeted Oracle fix for rebalance quarter labels in news + vector text."""

from __future__ import annotations

import argparse
import datetime as dt
import os
import pathlib
import sys
from typing import Dict, List, Optional, Sequence, Tuple

ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    import oracledb  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    oracledb = None  # type: ignore

from tools.oracle.env_bootstrap import load_env_files

import db_helper
from embedding_client import embed_text
from embedder_settings import get_embed_settings


WINDOW_DAYS = 7
SAMPLE_LIMIT_DEFAULT = 5

WINDOWS = (
    {"label": "2025-10-01", "old": "Q3 2025", "new": "Q4 2025"},
    {"label": "2025-07-01", "old": "Q2 2025", "new": "Q3 2025"},
    {"label": "2025-04-01", "old": "Q1 2025", "new": "Q2 2025"},
    {"label": "2025-01-01", "old": "Q4 2024", "new": "Q1 2025"},
)

NEWS_TABLES = (
    {
        "name": "NEWS_ITEMS",
        "date_candidates": ("DT_PUB",),
        "text_columns": ("TITLE", "SUMMARY"),
    },
    {
        "name": "ESG_NEWS",
        "date_candidates": ("DATE_PUBLISHED",),
        "text_columns": ("TITLE", "TEXT"),
    },
)

VECTOR_TABLES = (
    {
        "name": "ESG_DOCS",
        "date_candidates": (
            "ASOF_DATE",
            "AS_OF_DATE",
            "REBALANCE_DATE",
            "PUBLISHED_AT",
            "CREATED_AT",
        ),
        "text_columns": ("CHUNK_TEXT",),
    },
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fix rebalance quarter labels in news + vector text."
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true", help="Report counts only.")
    mode.add_argument("--apply", action="store_true", help="Apply updates.")
    parser.add_argument(
        "--sample-limit",
        type=int,
        default=SAMPLE_LIMIT_DEFAULT,
        help="Max sample IDs per match in dry-run.",
    )
    parser.add_argument(
        "--refresh-embeddings",
        action="store_true",
        help="Re-embed updated ESG_DOCS chunks (default: off).",
    )
    return parser.parse_args()


def _normalize_owner() -> Optional[str]:
    raw = os.getenv("DB_OWNER") or os.getenv("DB_SCHEMA") or os.getenv("DB_USER")
    if not raw:
        return None
    return raw.strip().upper()


def _quote(name: str) -> str:
    return f"\"{name}\""


def _table_ref(owner: Optional[str], table: str) -> str:
    if owner:
        return f"{_quote(owner)}.{_quote(table)}"
    return _quote(table)


def _window_bounds(label: str) -> Tuple[dt.datetime, dt.datetime]:
    anchor = dt.date.fromisoformat(label)
    start = anchor - dt.timedelta(days=WINDOW_DAYS)
    end = anchor + dt.timedelta(days=WINDOW_DAYS)
    return (
        dt.datetime.combine(start, dt.time.min),
        dt.datetime.combine(end, dt.time.max),
    )


def _fetch_table_columns(
    cursor, owner: Optional[str], table: str
) -> Dict[str, str]:
    owner_clause = "owner = :2" if owner else "owner = SYS_CONTEXT('USERENV','CURRENT_SCHEMA')"
    sql = (
        "SELECT column_name, data_type "
        "FROM all_tab_columns "
        "WHERE table_name = :1 AND "
        f"{owner_clause}"
    )
    if owner:
        cursor.execute(sql, [table, owner])
    else:
        cursor.execute(sql, [table])
    return {row[0].upper(): row[1].upper() for row in cursor.fetchall()}


def _resolve_date_column(
    columns: Dict[str, str], candidates: Sequence[str]
) -> Optional[str]:
    for candidate in candidates:
        data_type = columns.get(candidate.upper())
        if not data_type:
            continue
        if data_type == "DATE" or data_type.startswith("TIMESTAMP"):
            return candidate.upper()
    return None


def _resolve_date_filter(
    table: str, columns: Dict[str, str], candidates: Sequence[str]
) -> Tuple[Optional[str], Optional[str]]:
    date_column = _resolve_date_column(columns, candidates)
    if date_column:
        return _quote(date_column), None
    if table == "ESG_DOCS" and "SOURCE_ID" in columns:
        date_expr = "TO_DATE(SUBSTR(SOURCE_ID, 1, 10), 'YYYY-MM-DD')"
        guard = "REGEXP_LIKE(SOURCE_ID, '^[0-9]{4}-[0-9]{2}-[0-9]{2}')"
        return date_expr, guard
    return None, None


def _resolve_text_columns(
    columns: Dict[str, str], candidates: Sequence[str]
) -> List[str]:
    text_types = {"VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR", "CLOB", "NCLOB"}
    resolved: List[str] = []
    for candidate in candidates:
        data_type = columns.get(candidate.upper())
        if data_type in text_types:
            resolved.append(candidate.upper())
    return resolved


def _fetch_pk_columns(
    cursor, owner: Optional[str], table: str
) -> List[str]:
    owner_clause = "cons.owner = :2" if owner else "cons.owner = SYS_CONTEXT('USERENV','CURRENT_SCHEMA')"
    sql = (
        "SELECT cols.column_name "
        "FROM all_constraints cons "
        "JOIN all_cons_columns cols "
        "ON cons.owner = cols.owner AND cons.constraint_name = cols.constraint_name "
        f"WHERE cons.constraint_type = 'P' AND cons.table_name = :1 AND {owner_clause} "
        "ORDER BY cols.position"
    )
    if owner:
        cursor.execute(sql, [table, owner])
    else:
        cursor.execute(sql, [table])
    return [row[0] for row in cursor.fetchall()]


def _count_matches(
    cursor,
    table_ref: str,
    column: str,
    date_expr: str,
    date_guard: Optional[str],
    start_dt: dt.datetime,
    end_dt: dt.datetime,
    old: str,
) -> int:
    guard_clause = f" AND {date_guard}" if date_guard else ""
    sql = (
        f"SELECT COUNT(*) FROM {table_ref} "
        f"WHERE {date_expr} BETWEEN :1 AND :2 "
        f"{guard_clause} "
        f"AND {_quote(column)} LIKE '%' || :3 || '%'"
    )
    cursor.execute(sql, [start_dt, end_dt, old])
    row = cursor.fetchone()
    return int(row[0]) if row else 0


def _sample_ids(
    cursor,
    table_ref: str,
    pk_columns: Sequence[str],
    date_expr: str,
    date_guard: Optional[str],
    column: str,
    start_dt: dt.datetime,
    end_dt: dt.datetime,
    old: str,
    limit: int,
) -> List[str]:
    guard_clause = f" AND {date_guard}" if date_guard else ""
    pk_clause = ", ".join(_quote(name) for name in pk_columns) if pk_columns else "ROWID"
    sql = (
        f"SELECT {pk_clause} FROM {table_ref} "
        f"WHERE {date_expr} BETWEEN :1 AND :2 "
        f"{guard_clause} "
        f"AND {_quote(column)} LIKE '%' || :3 || '%' "
        f"FETCH FIRST {limit} ROWS ONLY"
    )
    cursor.execute(sql, [start_dt, end_dt, old])
    rows = cursor.fetchall()
    samples: List[str] = []
    for row in rows:
        if pk_columns:
            samples.append("|".join(str(val) for val in row))
        else:
            samples.append(str(row[0]))
    return samples


def _apply_update(
    cursor,
    table_ref: str,
    column: str,
    date_expr: str,
    date_guard: Optional[str],
    start_dt: dt.datetime,
    end_dt: dt.datetime,
    old: str,
    new: str,
) -> int:
    guard_clause = f" AND {date_guard}" if date_guard else ""
    sql = (
        f"UPDATE {table_ref} "
        f"SET {_quote(column)} = REPLACE({_quote(column)}, :1, :2) "
        f"WHERE {date_expr} BETWEEN :3 AND :4 "
        f"{guard_clause} "
        f"AND {_quote(column)} LIKE '%' || :1 || '%'"
    )
    cursor.execute(sql, [old, new, start_dt, end_dt])
    return int(cursor.rowcount or 0)


def _collect_vector_rows(
    cursor,
    table_ref: str,
    date_expr: str,
    date_guard: Optional[str],
    start_dt: dt.datetime,
    end_dt: dt.datetime,
    old: str,
) -> List[Tuple[str, str]]:
    guard_clause = f" AND {date_guard}" if date_guard else ""
    sql = (
        f"SELECT ROWIDTOCHAR(ROWID), {_quote('CHUNK_TEXT')} "
        f"FROM {table_ref} "
        f"WHERE {date_expr} BETWEEN :1 AND :2 "
        f"{guard_clause} "
        f"AND {_quote('CHUNK_TEXT')} LIKE '%' || :3 || '%'"
    )
    cursor.execute(sql, [start_dt, end_dt, old])
    return [(row[0], row[1] or "") for row in cursor.fetchall()]


def _resolve_embedding_column(columns: Dict[str, str]) -> Optional[str]:
    for candidate in ("EMBEDDING", "EMBED", "VECTOR"):
        if columns.get(candidate) == "VECTOR":
            return candidate
    return None


def _refresh_embeddings(
    cursor,
    table_ref: str,
    embedding_column: str,
    rows: Sequence[Tuple[str, str]],
    old: str,
    new: str,
) -> int:
    if not rows:
        return 0
    settings = get_embed_settings()
    if oracledb is not None and hasattr(oracledb, "DB_TYPE_VECTOR"):
        cursor.setinputsizes(vec=oracledb.DB_TYPE_VECTOR)
    updated = 0
    for rowid, text in rows:
        updated_text = (text or "").replace(old, new)
        vec = embed_text(updated_text, timeout=15.0, settings=settings)
        cursor.execute(
            f"UPDATE {table_ref} SET {_quote(embedding_column)} = :vec "
            "WHERE ROWID = CHARTOROWID(:rid)",
            {"vec": vec, "rid": rowid},
        )
        updated += 1
    return updated


def _dry_run(
    cursor,
    owner: Optional[str],
    tables: Sequence[Dict[str, object]],
    sample_limit: int,
) -> None:
    for table_cfg in tables:
        table = str(table_cfg["name"]).upper()
        table_ref = _table_ref(owner, table)
        columns = _fetch_table_columns(cursor, owner, table)
        date_expr, date_guard = _resolve_date_filter(
            table, columns, table_cfg["date_candidates"]  # type: ignore[arg-type]
        )
        text_columns = _resolve_text_columns(columns, table_cfg["text_columns"])  # type: ignore[arg-type]
        if not date_expr or not text_columns:
            print(f"- table={table} skipped=no_date_or_text_column")
            continue
        pk_columns = _fetch_pk_columns(cursor, owner, table)
        for window in WINDOWS:
            start_dt, end_dt = _window_bounds(window["label"])
            for column in text_columns:
                count = _count_matches(
                    cursor,
                    table_ref,
                    column,
                    date_expr,
                    date_guard,
                    start_dt,
                    end_dt,
                    window["old"],
                )
                print(
                    f"- table={table} column={column} window={window['label']} "
                    f"old='{window['old']}' new='{window['new']}' count={count}"
                )
                if count:
                    samples = _sample_ids(
                        cursor,
                        table_ref,
                        pk_columns,
                        date_expr,
                        date_guard,
                        column,
                        start_dt,
                        end_dt,
                        window["old"],
                        sample_limit,
                    )
                    print(f"  sample_ids={','.join(samples)}")


def _apply(
    conn,
    cursor,
    owner: Optional[str],
    tables: Sequence[Dict[str, object]],
    refresh_embeddings: bool,
) -> None:
    for window in WINDOWS:
        start_dt, end_dt = _window_bounds(window["label"])
        for table_cfg in tables:
            table = str(table_cfg["name"]).upper()
            table_ref = _table_ref(owner, table)
            columns = _fetch_table_columns(cursor, owner, table)
            date_expr, date_guard = _resolve_date_filter(
                table, columns, table_cfg["date_candidates"]  # type: ignore[arg-type]
            )
            text_columns = _resolve_text_columns(columns, table_cfg["text_columns"])  # type: ignore[arg-type]
            if not date_expr or not text_columns:
                print(f"- table={table} window={window['label']} skipped=no_date_or_text_column")
                continue
            try:
                total_updated = 0
                total_refreshed = 0
                vector_rows: List[Tuple[str, str]] = []
                embedding_column = None
                if refresh_embeddings and table == "ESG_DOCS":
                    embedding_column = _resolve_embedding_column(columns)
                for column in text_columns:
                    if refresh_embeddings and table == "ESG_DOCS" and column == "CHUNK_TEXT":
                        vector_rows = _collect_vector_rows(
                            cursor,
                            table_ref,
                            date_expr,
                            date_guard,
                            start_dt,
                            end_dt,
                            window["old"],
                        )
                    updated = _apply_update(
                        cursor,
                        table_ref,
                        column,
                        date_expr,
                        date_guard,
                        start_dt,
                        end_dt,
                        window["old"],
                        window["new"],
                    )
                    total_updated += updated
                if refresh_embeddings and table == "ESG_DOCS" and embedding_column:
                    total_refreshed = _refresh_embeddings(
                        cursor,
                        table_ref,
                        embedding_column,
                        vector_rows,
                        window["old"],
                        window["new"],
                    )
                conn.commit()
                print(
                    f"- table={table} window={window['label']} updated={total_updated} "
                    f"refreshed_embeddings={total_refreshed}"
                )
            except Exception:
                conn.rollback()
                raise


def main() -> int:
    args = _parse_args()

    load_env_files()
    owner = _normalize_owner()

    tables = list(NEWS_TABLES) + list(VECTOR_TABLES)

    try:
        with db_helper.get_connection() as conn:
            cursor = conn.cursor()
            if args.dry_run:
                _dry_run(cursor, owner, tables, args.sample_limit)
                return 0
            _apply(conn, cursor, owner, tables, args.refresh_embeddings)
            return 0
    except Exception as exc:
        print(f"error: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
