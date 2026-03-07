#!/usr/bin/env python3
"""Restore post-cutover data assets required for production correctness."""

from __future__ import annotations

import argparse
import pickle
from pathlib import Path
from typing import Any, Iterable, Sequence

import db_helper


def _load_pickle(path: Path) -> Any:
    return pickle.loads(path.read_bytes())


def _table_exists(cursor: Any, table_name: str) -> bool:
    cursor.execute(
        "select count(*) from user_tables where table_name = :1",
        [table_name.upper()],
    )
    return bool(cursor.fetchone()[0])


def _render_oracle_type(column: dict[str, Any]) -> str:
    data_type = (column.get("data_type") or "").upper()
    data_length = column.get("data_length")
    precision = column.get("data_precision")
    scale = column.get("data_scale")
    char_length = column.get("char_length")
    char_used = (column.get("char_used") or "").upper()

    if data_type in {"VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR"}:
        length = char_length or data_length or 1
        unit = " CHAR" if char_used == "C" else ""
        return f"{data_type}({int(length)}{unit})"
    if data_type == "RAW":
        return f"RAW({int(data_length or 1)})"
    if data_type == "FLOAT":
        return f"FLOAT({int(precision)})" if precision is not None else "FLOAT"
    if data_type == "NUMBER":
        if precision is None:
            return "NUMBER"
        if scale is None:
            return f"NUMBER({int(precision)})"
        return f"NUMBER({int(precision)},{int(scale)})"
    if data_type.startswith("TIMESTAMP"):
        return data_type
    return data_type


def _create_table(cursor: Any, table_name: str, columns: Sequence[dict[str, Any]]) -> None:
    rendered = []
    for column in columns:
        line = f'"{column["column_name"]}" {_render_oracle_type(column)}'
        if (column.get("nullable") or "Y").upper() == "N":
            line += " NOT NULL"
        rendered.append(line)
    ddl = f'CREATE TABLE "{table_name}" (\n  ' + ",\n  ".join(rendered) + "\n)"
    cursor.execute(ddl)


def _load_table(cursor: Any, table_name: str, table_data: dict[str, Any]) -> None:
    columns = table_data["columns"]
    rows = table_data["rows"]
    if not _table_exists(cursor, table_name):
        _create_table(cursor, table_name, columns)
        print(f"created_table {table_name}")

    cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
    existing = int(cursor.fetchone()[0])
    if existing and existing != len(rows):
        raise RuntimeError(
            f"table {table_name} already has {existing} rows; expected {len(rows)} or 0"
        )
    if existing == len(rows):
        print(f"table_rows_ok {table_name} {existing}")
        return
    if not rows:
        print(f"table_empty {table_name}")
        return

    col_list = ", ".join(f'"{c["column_name"]}"' for c in columns)
    bind_list = ", ".join(f":{idx}" for idx in range(1, len(columns) + 1))
    cursor.executemany(
        f'INSERT INTO "{table_name}" ({col_list}) VALUES ({bind_list})',
        rows,
    )
    print(f"loaded_rows {table_name} {len(rows)}")


def _batched(values: Sequence[Any], batch_size: int) -> Iterable[Sequence[Any]]:
    for start in range(0, len(values), batch_size):
        yield values[start : start + batch_size]


def _restore_embeddings(cursor: Any, embeddings: Sequence[Sequence[Any]], batch_size: int) -> None:
    cursor.execute(
        "select count(*), count(case when embedding is not null then 1 end) from esg_docs"
    )
    total_docs, current_embeddings = [int(v) for v in cursor.fetchone()]
    if total_docs != len(embeddings):
        raise RuntimeError(
            f"expected {len(embeddings)} esg_docs rows but found {total_docs} on target"
        )
    if current_embeddings == len(embeddings):
        print(f"embeddings_already_present {current_embeddings}")
        return

    sql = 'update "ESG_DOCS" set "EMBEDDING" = :1 where "DOC_ID" = :2'
    restored = 0
    for batch in _batched(embeddings, batch_size):
        cursor.executemany(sql, [(embedding, doc_id) for doc_id, embedding in batch])
        restored += len(batch)
        print(f"embedding_batch {restored}")


def _ensure_text_index(cursor: Any) -> None:
    cursor.execute(
        "select count(*) from user_indexes where index_name = 'ESG_DOCS_TEXT_IDX'"
    )
    if cursor.fetchone()[0]:
        print("text_index_exists ESG_DOCS_TEXT_IDX")
        return
    cursor.execute(
        "CREATE INDEX ESG_DOCS_TEXT_IDX ON ESG_DOCS (CHUNK_TEXT) "
        "INDEXTYPE IS CTXSYS.CONTEXT PARAMETERS ('SYNC (ON COMMIT)')"
    )
    print("text_index_created ESG_DOCS_TEXT_IDX")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Restore post-cutover AI regulation data and Ask2 retrieval assets."
    )
    parser.add_argument("--ai-reg-export", type=Path, help="Pickle export for AI regulation tables.")
    parser.add_argument(
        "--embeddings-export",
        type=Path,
        help="Pickle export for ESG_DOCS embeddings.",
    )
    parser.add_argument(
        "--embedding-batch-size",
        type=int,
        default=250,
        help="Batch size for ESG_DOCS embedding updates.",
    )
    parser.add_argument(
        "--create-text-index",
        action="store_true",
        help="Create ESG_DOCS_TEXT_IDX if it is missing.",
    )
    args = parser.parse_args()

    if not args.ai_reg_export and not args.embeddings_export and not args.create_text_index:
        parser.error("provide --ai-reg-export, --embeddings-export, and/or --create-text-index")

    ai_reg_export = _load_pickle(args.ai_reg_export) if args.ai_reg_export else None
    embeddings_export = _load_pickle(args.embeddings_export) if args.embeddings_export else None

    with db_helper.get_connection() as conn:
        cursor = conn.cursor()
        if ai_reg_export:
            for table_name, table_data in ai_reg_export.items():
                _load_table(cursor, table_name, table_data)
            conn.commit()
        if embeddings_export:
            _restore_embeddings(cursor, embeddings_export, args.embedding_batch_size)
            conn.commit()
        if args.create_text_index:
            _ensure_text_index(cursor)
            conn.commit()

        cursor.execute(
            "select count(*), count(case when embedding is not null then 1 end) from esg_docs"
        )
        print("target_esg_docs", cursor.fetchone())
        if ai_reg_export and "FACT_INSTRUMENT_SNAPSHOT" in ai_reg_export:
            cursor.execute("select count(*) from FACT_INSTRUMENT_SNAPSHOT")
            print("target_fact_instrument_snapshot", cursor.fetchone()[0])

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
