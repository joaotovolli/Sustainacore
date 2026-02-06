#!/usr/bin/env python3
"""Ingest public corpus chunks into the Oracle ESG_DOCS vector table.

This is intended for controlled, reversible updates from WSL2 or VM1.

Idempotency strategy:
- Skip when a row already exists with the same (SOURCE_TYPE, SOURCE_URL, CHUNK_IX).

Safety:
- Single-process, sequential embedding + inserts.
- Batch size is configurable; keep it small on VM1.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from tools.oracle.env_bootstrap import load_env_files

from db_helper import get_connection
from embedder_settings import get_embed_settings
from embedding_client import embed_text

try:  # pragma: no cover - optional dependency at import-time
    import oracledb  # type: ignore
except Exception:  # pragma: no cover
    oracledb = None  # type: ignore


EXPECTED_DIM = 384


@dataclass(frozen=True)
class IngestRow:
    source_type: str
    source_id: str
    source_url: str
    title: str
    chunk_ix: int
    chunk_text: str


def _read_jsonl(path: str) -> Iterable[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def _coerce_int(value: Any, default: int = 0) -> int:
    try:
        out = int(value)
    except Exception:
        return default
    return out


def _normalize_source_type(value: Any) -> str:
    text = str(value or "").strip()
    return text[:40] if text else ""


def _normalize_source_id(value: Any) -> str:
    text = str(value or "").strip()
    return text[:120] if text else ""


def _normalize_url(value: Any) -> str:
    text = str(value or "").strip()
    return text[:800] if text else ""


def _normalize_title(value: Any, fallback: str) -> str:
    text = str(value or "").strip() or fallback
    return text[:512]


def _normalize_chunk_text(value: Any) -> str:
    text = str(value or "").strip()
    return text


def _rows_from_corpus(records: Iterable[Dict[str, Any]]) -> List[IngestRow]:
    out: List[IngestRow] = []
    for rec in records:
        if not isinstance(rec, dict):
            continue
        source_type = _normalize_source_type(rec.get("oracle_source_type"))
        url = _normalize_url(rec.get("url"))
        if not source_type or not url:
            continue
        chunk_ix = _coerce_int(rec.get("chunk_ix"), default=0)
        if chunk_ix <= 0:
            continue
        chunk_text = _normalize_chunk_text(rec.get("content"))
        if not chunk_text:
            continue

        entity_keys = rec.get("entity_keys") if isinstance(rec.get("entity_keys"), dict) else {}
        ticker = ""
        if isinstance(entity_keys, dict):
            ticker = str(entity_keys.get("ticker") or "").strip().upper()
        source_id = ticker or _normalize_source_id(rec.get("source_id")) or "PUBLIC"
        title = _normalize_title(rec.get("title"), fallback=url)

        out.append(
            IngestRow(
                source_type=source_type,
                source_id=source_id,
                source_url=url,
                title=title,
                chunk_ix=chunk_ix,
                chunk_text=chunk_text,
            )
        )
    return out


def _batched(values: Sequence[str], batch_size: int) -> Iterable[List[str]]:
    for i in range(0, len(values), batch_size):
        yield list(values[i : i + batch_size])


def _fetch_existing_keys(
    conn,
    table: str,
    *,
    source_type: str,
    urls: Sequence[str],
) -> set[Tuple[str, str, int]]:
    if not urls:
        return set()
    cur = conn.cursor()
    existing: set[Tuple[str, str, int]] = set()
    batch_size = 900  # Oracle bind limit safety margin
    for batch in _batched(list(urls), batch_size):
        binds = {"st": source_type}
        placeholders: List[str] = []
        for idx, url in enumerate(batch):
            key = f"u_{idx}"
            binds[key] = url
            placeholders.append(f":{key}")
        sql = (
            f"SELECT source_type, source_url, chunk_ix "
            f"FROM {table} "
            f"WHERE source_type = :st AND source_url IN ({', '.join(placeholders)})"
        )
        cur.execute(sql, binds)
        for st, su, cix in cur.fetchall():
            try:
                cix_int = int(cix or 0)
            except Exception:
                cix_int = 0
            existing.add((str(st or "").strip(), str(su or "").strip(), cix_int))
    return existing


def _insert_rows(
    conn,
    table: str,
    rows: Sequence[IngestRow],
    *,
    batch_size: int,
    dry_run: bool,
) -> Tuple[int, int]:
    if not rows:
        return 0, 0

    embed_settings = get_embed_settings()
    if embed_settings.expected_dimension != EXPECTED_DIM:
        raise RuntimeError(
            f"embed_dim_mismatch expected={EXPECTED_DIM} configured={embed_settings.expected_dimension}"
        )

    insert_sql = (
        f"INSERT INTO {table} "
        "(SOURCE_TYPE, SOURCE_ID, SOURCE_URL, TITLE, CHUNK_IX, CHUNK_TEXT, EMBEDDING) "
        "VALUES (:source_type, :source_id, :source_url, :title, :chunk_ix, :chunk_text, :embedding)"
    )

    cur = conn.cursor()
    if oracledb is not None and hasattr(oracledb, "DB_TYPE_VECTOR"):
        try:
            cur.setinputsizes(embedding=oracledb.DB_TYPE_VECTOR)
        except Exception:
            pass

    inserted = 0
    skipped = 0
    buf: List[Dict[str, Any]] = []

    # Existence check key is (source_type, source_url, chunk_ix).
    by_type: Dict[str, List[IngestRow]] = {}
    for row in rows:
        by_type.setdefault(row.source_type, []).append(row)

    existing_keys: set[Tuple[str, str, int]] = set()
    for st, group in by_type.items():
        urls = sorted({r.source_url for r in group})
        existing_keys |= _fetch_existing_keys(conn, table, source_type=st, urls=urls)

    for row in rows:
        key = (row.source_type, row.source_url, row.chunk_ix)
        if key in existing_keys:
            skipped += 1
            continue
        if dry_run:
            inserted += 1
            existing_keys.add(key)
            continue
        embedding = embed_text(row.chunk_text, timeout=15.0, settings=embed_settings)
        if len(embedding) != EXPECTED_DIM:
            raise RuntimeError(f"embedding_dim_mismatch expected={EXPECTED_DIM} got={len(embedding)}")
        buf.append(
            {
                "source_type": row.source_type,
                "source_id": row.source_id,
                "source_url": row.source_url,
                "title": row.title,
                "chunk_ix": row.chunk_ix,
                "chunk_text": row.chunk_text,
                "embedding": embedding,
            }
        )
        existing_keys.add(key)
        if len(buf) >= batch_size:
            cur.executemany(insert_sql, buf)
            inserted += len(buf)
            buf.clear()

    if buf and not dry_run:
        cur.executemany(insert_sql, buf)
        inserted += len(buf)
        buf.clear()

    if not dry_run:
        conn.commit()
    return inserted, skipped


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in-jsonl", required=True, help="Path to corpus_chunks.jsonl")
    ap.add_argument("--table", default=os.environ.get("ORACLE_VECTOR_TABLE", "ESG_DOCS"))
    ap.add_argument(
        "--source-types",
        default="",
        help="Comma-separated oracle_source_type whitelist (e.g., methodology,company_profile,regulatory,news_release,performance). Empty = all.",
    )
    ap.add_argument("--skip", type=int, default=0, help="Skip the first N eligible records after filtering.")
    ap.add_argument("--limit", type=int, default=0, help="Max rows to consider (0 = no limit).")
    ap.add_argument("--batch-size", type=int, default=25, help="Insert batch size (keep small on VM1).")
    ap.add_argument("--dry-run", action="store_true", help="Do not embed or insert; only count.")
    args = ap.parse_args(list(argv) if argv is not None else None)

    load_env_files()

    allowed = {s.strip() for s in (args.source_types or "").split(",") if s.strip()}
    records: List[Dict[str, Any]] = []
    skipped = 0
    for rec in _read_jsonl(args.in_jsonl):
        if allowed:
            st = str(rec.get("oracle_source_type") or "").strip()
            if st not in allowed:
                continue
        if args.skip and skipped < args.skip:
            skipped += 1
            continue
        records.append(rec)
        if args.limit and len(records) >= args.limit:
            break

    rows = _rows_from_corpus(records)
    if not rows:
        print("no_rows")
        return 0

    table = str(args.table or "ESG_DOCS").strip().upper()
    with get_connection() as conn:
        inserted, skipped = _insert_rows(
            conn,
            table,
            rows,
            batch_size=max(1, int(args.batch_size)),
            dry_run=bool(args.dry_run),
        )

    print(f"table={table} dry_run={int(bool(args.dry_run))} considered={len(rows)} inserted={inserted} skipped_existing={skipped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
