#!/usr/bin/env python3
"""Targeted ESG_DOCS fix for rebalance quarter labels with audit + embeddings."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import os
import pathlib
import sys
from typing import List, Optional, Sequence, Tuple

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


AUDIT_TABLE = "W_FIX_REBALANCE_LABELS_AUDIT"
TABLE_NAME = "ESG_DOCS"
TEXT_COLUMN = "CHUNK_TEXT"
TITLE_COLUMN = "TITLE"
PK_COLUMN = "DOC_ID"
EMBED_COLUMN = "EMBEDDING"
SAMPLE_LIMIT_DEFAULT = 5
AUDIT_LOOKBACK_HOURS = 24

BASE_TERMS = [
    "rebalance",
    "rebalance details",
    "tech100",
    "ai governance & ethics index",
    "scores are refreshed",
    "index composition",
    "timeline",
    "portfolio",
]

RULES = [
    {
        "old": "Q3 2025",
        "new": "Q4 2025",
        "extra_terms": ["oct", "october", "2025-10", "q4", "rebalance"],
    },
    {"old": "Q2 2025", "new": "Q3 2025", "extra_terms": None},
    {"old": "Q1 2025", "new": "Q2 2025", "extra_terms": None},
    {"old": "Q4 2024", "new": "Q1 2025", "extra_terms": None},
]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fix rebalance quarter labels in ESG_DOCS with audit + embeddings."
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true", help="Report counts only.")
    mode.add_argument("--apply", action="store_true", help="Apply updates.")
    parser.add_argument(
        "--sample-limit",
        type=int,
        default=SAMPLE_LIMIT_DEFAULT,
        help="Max sample DOC_IDs per rule in dry-run.",
    )
    parser.add_argument(
        "--refresh-embeddings",
        action="store_true",
        help="Re-embed changed rows (default: off).",
    )
    parser.add_argument(
        "--audit-lookback-hours",
        type=int,
        default=AUDIT_LOOKBACK_HOURS,
        help="Hours to look back for audit-based embedding refresh.",
    )
    return parser.parse_args()


def _normalize_owner() -> Optional[str]:
    raw = os.getenv("DB_OWNER") or os.getenv("DB_SCHEMA") or os.getenv("DB_USER")
    if not raw:
        return None
    return raw.strip().upper()


def _quote(name: str) -> str:
    return f"\"{name}\""


def _table_ref(owner: Optional[str]) -> str:
    if owner:
        return f"{_quote(owner)}.{_quote(TABLE_NAME)}"
    return _quote(TABLE_NAME)


def _hash_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _ensure_audit_table(cursor, owner: Optional[str]) -> None:
    sql = "SELECT 1 FROM all_tables WHERE table_name = :1"
    params: List[str] = [AUDIT_TABLE]
    if owner:
        sql += " AND owner = :2"
        params.append(owner)
    cursor.execute(sql, params)
    if cursor.fetchone():
        return
    cursor.execute(
        f"""
        CREATE TABLE {_table_ref(owner).replace(_quote(TABLE_NAME), _quote(AUDIT_TABLE))} (
            ID NUMBER GENERATED ALWAYS AS IDENTITY,
            TABLE_NAME VARCHAR2(128),
            PK_VALUE VARCHAR2(200),
            COLUMN_NAME VARCHAR2(128),
            OLD_HASH VARCHAR2(64),
            NEW_HASH VARCHAR2(64),
            CHANGED_AT TIMESTAMP DEFAULT SYSTIMESTAMP
        )
        """
    )


def _contains_any_clause(
    column_expr: str, terms: Sequence[str], start_index: int
) -> Tuple[str, List[str], int]:
    clauses: List[str] = []
    binds: List[str] = []
    idx = start_index
    for term in terms:
        clauses.append(f"DBMS_LOB.INSTR({column_expr}, :{idx}) > 0")
        binds.append(term)
        idx += 1
    return "(" + " OR ".join(clauses) + ")", binds, idx


def _build_where_clause(
    *,
    old_label: str,
    extra_terms: Optional[Sequence[str]],
) -> Tuple[str, List[str]]:
    binds: List[str] = []
    idx = 1
    column_expr = f"LOWER({_quote(TEXT_COLUMN)})"
    label_clause = f"DBMS_LOB.INSTR({column_expr}, :{idx}) > 0"
    binds.append(old_label.lower())
    idx += 1
    base_clause, base_binds, idx = _contains_any_clause(column_expr, BASE_TERMS, idx)
    binds.extend([term.lower() for term in base_binds])
    clauses = [label_clause, base_clause]
    if extra_terms:
        extra_clause, extra_binds, idx = _contains_any_clause(column_expr, extra_terms, idx)
        binds.extend([term.lower() for term in extra_binds])
        clauses.append(extra_clause)
    return " AND ".join(clauses), binds


def _count_matches(cursor, owner: Optional[str], old_label: str, extra_terms: Optional[Sequence[str]]) -> int:
    table_ref = _table_ref(owner)
    where_sql, binds = _build_where_clause(old_label=old_label, extra_terms=extra_terms)
    sql = f"SELECT COUNT(*) FROM {table_ref} WHERE {where_sql}"
    cursor.execute(sql, binds)
    row = cursor.fetchone()
    return int(row[0]) if row else 0


def _sample_doc_ids(
    cursor,
    owner: Optional[str],
    old_label: str,
    extra_terms: Optional[Sequence[str]],
    limit: int,
) -> List[str]:
    table_ref = _table_ref(owner)
    where_sql, binds = _build_where_clause(old_label=old_label, extra_terms=extra_terms)
    sql = (
        f"SELECT {_quote(PK_COLUMN)} FROM {table_ref} "
        f"WHERE {where_sql} "
        f"FETCH FIRST {limit} ROWS ONLY"
    )
    cursor.execute(sql, binds)
    return [str(row[0]) for row in cursor.fetchall()]


def _fetch_rows(
    cursor,
    owner: Optional[str],
    old_label: str,
    extra_terms: Optional[Sequence[str]],
) -> List[Tuple[str, str, str, str]]:
    table_ref = _table_ref(owner)
    where_sql, binds = _build_where_clause(old_label=old_label, extra_terms=extra_terms)
    sql = (
        f"SELECT ROWIDTOCHAR(ROWID), {_quote(PK_COLUMN)}, {_quote(TEXT_COLUMN)}, {_quote(TITLE_COLUMN)} "
        f"FROM {table_ref} WHERE {where_sql}"
    )
    cursor.execute(sql, binds)
    rows: List[Tuple[str, str, str, str]] = []
    for row in cursor.fetchall():
        text_value = row[2]
        if hasattr(text_value, "read"):
            try:
                text_value = text_value.read()
            except Exception:
                text_value = ""
        title_value = row[3] or ""
        rows.append((row[0], str(row[1]), text_value or "", title_value))
    return rows


def _insert_audit(
    cursor,
    owner: Optional[str],
    pk_value: str,
    old_hash: str,
    new_hash: str,
) -> None:
    audit_ref = _table_ref(owner).replace(_quote(TABLE_NAME), _quote(AUDIT_TABLE))
    cursor.execute(
        f"""
        INSERT INTO {audit_ref}
            (TABLE_NAME, PK_VALUE, COLUMN_NAME, OLD_HASH, NEW_HASH)
        VALUES (:1, :2, :3, :4, :5)
        """,
        [TABLE_NAME, pk_value, TEXT_COLUMN, old_hash, new_hash],
    )


def _update_text(
    cursor,
    owner: Optional[str],
    rowid: str,
    column: str,
    old_label: str,
    new_label: str,
) -> None:
    table_ref = _table_ref(owner)
    cursor.execute(
        f"""
        UPDATE {table_ref}
        SET {_quote(column)} = REPLACE({_quote(column)}, :1, :2)
        WHERE ROWID = CHARTOROWID(:3)
        """,
        [old_label, new_label, rowid],
    )


def _refresh_embedding(
    cursor,
    owner: Optional[str],
    rowid: str,
    updated_text: str,
) -> None:
    table_ref = _table_ref(owner)
    settings = get_embed_settings()
    vec = embed_text(updated_text, timeout=15.0, settings=settings)
    if oracledb is not None and hasattr(oracledb, "DB_TYPE_VECTOR"):
        try:
            cursor.setinputsizes(vec=oracledb.DB_TYPE_VECTOR)
        except Exception:
            pass
    cursor.execute(
        f"""
        UPDATE {table_ref}
        SET {_quote(EMBED_COLUMN)} = :vec
        WHERE ROWID = CHARTOROWID(:rid)
        """,
        {"vec": vec, "rid": rowid},
    )


def _fetch_rows_from_audit(
    cursor,
    owner: Optional[str],
    since: dt.datetime,
) -> List[Tuple[str, str, str, str]]:
    audit_ref = _table_ref(owner).replace(_quote(TABLE_NAME), _quote(AUDIT_TABLE))
    table_ref = _table_ref(owner)
    sql = (
        f"SELECT ROWIDTOCHAR(d.ROWID), d.{_quote(PK_COLUMN)}, d.{_quote(TEXT_COLUMN)}, d.{_quote(TITLE_COLUMN)} "
        f"FROM {table_ref} d "
        f"JOIN ("
        f"  SELECT DISTINCT TO_NUMBER(PK_VALUE) AS DOC_ID "
        f"  FROM {audit_ref} "
        f"  WHERE TABLE_NAME = :1 AND COLUMN_NAME = :2 AND CHANGED_AT >= :3"
        f") a "
        f"ON d.{_quote(PK_COLUMN)} = a.DOC_ID"
    )
    cursor.execute(sql, [TABLE_NAME, TEXT_COLUMN, since])
    rows: List[Tuple[str, str, str, str]] = []
    for row in cursor.fetchall():
        text_value = row[2]
        if hasattr(text_value, "read"):
            try:
                text_value = text_value.read()
            except Exception:
                text_value = ""
        title_value = row[3] or ""
        rows.append((row[0], str(row[1]), text_value or "", title_value))
    return rows


def _count_title_from_audit(
    cursor,
    owner: Optional[str],
    since: dt.datetime,
    old_label: str,
) -> int:
    audit_ref = _table_ref(owner).replace(_quote(TABLE_NAME), _quote(AUDIT_TABLE))
    table_ref = _table_ref(owner)
    sql = (
        f"SELECT COUNT(*) "
        f"FROM {table_ref} d "
        f"JOIN ("
        f"  SELECT DISTINCT TO_NUMBER(PK_VALUE) AS DOC_ID "
        f"  FROM {audit_ref} "
        f"  WHERE TABLE_NAME = :1 AND COLUMN_NAME = :2 AND CHANGED_AT >= :3"
        f") a "
        f"ON d.{_quote(PK_COLUMN)} = a.DOC_ID "
        f"WHERE LOWER(d.{_quote(TITLE_COLUMN)}) LIKE '%' || :4 || '%'"
    )
    cursor.execute(sql, [TABLE_NAME, TEXT_COLUMN, since, old_label.lower()])
    row = cursor.fetchone()
    return int(row[0]) if row else 0


def _dry_run(
    cursor,
    owner: Optional[str],
    sample_limit: int,
    audit_lookback_hours: int,
) -> None:
    since = dt.datetime.utcnow() - dt.timedelta(hours=audit_lookback_hours)
    for rule in RULES:
        count = _count_matches(cursor, owner, rule["old"], rule["extra_terms"])
        print(
            f"- table={TABLE_NAME} column={TEXT_COLUMN} "
            f"old='{rule['old']}' new='{rule['new']}' count={count}"
        )
        if count:
            samples = _sample_doc_ids(cursor, owner, rule["old"], rule["extra_terms"], sample_limit)
            print(f"  sample_ids={','.join(samples)}")
        title_count = _count_title_from_audit(cursor, owner, since, rule["old"])
        if title_count:
            print(
                f"  audit_title_matches old='{rule['old']}' "
                f"count={title_count} lookback_hours={audit_lookback_hours}"
            )


def _apply(
    conn,
    cursor,
    owner: Optional[str],
    refresh_embeddings: bool,
    audit_lookback_hours: int,
) -> Tuple[int, int]:
    total_updated = 0
    total_refreshed = 0
    _ensure_audit_table(cursor, owner)
    refreshed_rowids: set[str] = set()
    since = dt.datetime.utcnow() - dt.timedelta(hours=audit_lookback_hours)
    for rule in RULES:
        try:
            rows = _fetch_rows(cursor, owner, rule["old"], rule["extra_terms"])
            audit_rows = _fetch_rows_from_audit(cursor, owner, since)
            row_map: dict[str, Tuple[str, str, str]] = {}
            for rowid, pk_value, chunk_text, title in rows:
                row_map[rowid] = (pk_value, chunk_text, title)
            for rowid, pk_value, chunk_text, title in audit_rows:
                row_map.setdefault(rowid, (pk_value, chunk_text, title))
            updated_text = 0
            updated_title = 0
            refreshed = 0
            for rowid, (pk_value, text, title) in row_map.items():
                text_changed = False
                title_changed = False
                new_text = text
                new_title = title
                if rule["old"] in (text or ""):
                    new_text = (text or "").replace(rule["old"], rule["new"])
                    if new_text != text:
                        _insert_audit(cursor, owner, pk_value, _hash_text(text), _hash_text(new_text))
                        _update_text(cursor, owner, rowid, TEXT_COLUMN, rule["old"], rule["new"])
                        updated_text += 1
                        text_changed = True
                if rule["old"] in (title or ""):
                    new_title = (title or "").replace(rule["old"], rule["new"])
                    if new_title != title:
                        _insert_audit(cursor, owner, pk_value, _hash_text(title), _hash_text(new_title))
                        _update_text(cursor, owner, rowid, TITLE_COLUMN, rule["old"], rule["new"])
                        updated_title += 1
                        title_changed = True
                if refresh_embeddings and text_changed and rowid not in refreshed_rowids:
                    _refresh_embedding(cursor, owner, rowid, new_text)
                    refreshed_rowids.add(rowid)
                    refreshed += 1
            conn.commit()
            total_updated += updated_text + updated_title
            total_refreshed += refreshed
            print(
                f"- table={TABLE_NAME} old='{rule['old']}' new='{rule['new']}' "
                f"updated_text={updated_text} updated_title={updated_title} "
                f"refreshed_embeddings={refreshed}"
            )
        except Exception:
            conn.rollback()
            raise
    if refresh_embeddings:
        since = dt.datetime.utcnow() - dt.timedelta(hours=audit_lookback_hours)
        audit_rows = _fetch_rows_from_audit(cursor, owner, since)
        audit_refreshed = 0
        for rowid, _pk_value, text, _title in audit_rows:
            if rowid in refreshed_rowids:
                continue
            _refresh_embedding(cursor, owner, rowid, text)
            refreshed_rowids.add(rowid)
            audit_refreshed += 1
        conn.commit()
        total_refreshed += audit_refreshed
        print(f"- table={TABLE_NAME} audit_refresh_embeddings={audit_refreshed}")
    return total_updated, total_refreshed


def main() -> int:
    args = _parse_args()
    load_env_files()
    owner = _normalize_owner()

    try:
        with db_helper.get_connection() as conn:
            cursor = conn.cursor()
            if args.dry_run:
                _dry_run(cursor, owner, args.sample_limit, args.audit_lookback_hours)
                return 0
            _apply(conn, cursor, owner, args.refresh_embeddings, args.audit_lookback_hours)
            return 0
    except Exception as exc:
        print(f"error: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
