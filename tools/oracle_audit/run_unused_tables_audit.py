#!/usr/bin/env python3
from __future__ import annotations

import csv
import datetime as dt
import os
import pathlib
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.oracle import env_bootstrap


@dataclass(frozen=True)
class TableInventoryRow:
    table_name: str
    status: str | None
    last_ddl_time: str | None


def _write_csv(path: Path, header: Iterable[str], rows: Iterable[Iterable[Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(list(header))
        for row in rows:
            writer.writerow(list(row))


def _safe_ident(name: str) -> str:
    return name.replace('\"', '\"\"')


def _query_dict(cur, sql: str, binds: dict[str, Any]) -> dict[str, int]:
    cur.execute(sql, binds)
    out: dict[str, int] = {}
    for key, count in cur.fetchall():
        if key is None:
            continue
        out[str(key)] = int(count or 0)
    return out


def _can_select(cur, table: str) -> bool:
    try:
        cur.execute(f"SELECT 1 FROM {table} WHERE 1=0")
        return True
    except Exception:
        return False


def _text_hit_count(cur, sql: str, *, owner: str, needle_upper: str) -> int:
    cur.execute(sql, {"owner": owner, "needle": needle_upper})
    row = cur.fetchone()
    return int(row[0] or 0) if row else 0


def main() -> None:
    # Mandatory: load Oracle env safely (no shell source).
    env_bootstrap.load_env_files()

    import db_helper  # noqa: WPS433 (repo-local import, intentionally after env bootstrap)

    out_root = Path("/home/opc/reports/oracle_unused_audit")
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = out_root / ts
    out_dir.mkdir(parents=True, exist_ok=False)

    blocks_threshold = int(os.environ.get("ORACLE_AUDIT_BLOCKS_THRESHOLD", "200"))

    with db_helper.get_connection() as conn:
        cur = conn.cursor()

        cur.execute("SELECT USER FROM dual")
        owner = str(cur.fetchone()[0]).upper()

        cur.execute(
            """
            SELECT object_name, status, TO_CHAR(last_ddl_time, 'YYYY-MM-DD HH24:MI:SS')
            FROM user_objects
            WHERE object_type = 'TABLE'
            ORDER BY object_name
            """
        )
        inventory_rows: list[TableInventoryRow] = [
            TableInventoryRow(str(name), str(status) if status is not None else None, last_ddl)
            for name, status, last_ddl in cur.fetchall()
        ]
        tables = [row.table_name for row in inventory_rows]

        dependency_counts = _query_dict(
            cur,
            """
            SELECT referenced_name, COUNT(*)
            FROM all_dependencies
            WHERE referenced_owner = :owner
              AND referenced_type = 'TABLE'
            GROUP BY referenced_name
            """,
            {"owner": owner},
        )

        fk_inbound_counts = _query_dict(
            cur,
            """
            SELECT pk.table_name AS referenced_table, COUNT(DISTINCT fk.constraint_name) AS inbound_fk_count
            FROM all_constraints pk
            JOIN all_constraints fk
              ON fk.r_owner = pk.owner
             AND fk.r_constraint_name = pk.constraint_name
            WHERE pk.owner = :owner
              AND pk.constraint_type IN ('P', 'U')
              AND fk.constraint_type = 'R'
            GROUP BY pk.table_name
            """,
            {"owner": owner},
        )

        synonym_counts = _query_dict(
            cur,
            """
            SELECT table_name, COUNT(*)
            FROM all_synonyms
            WHERE table_owner = :owner
            GROUP BY table_name
            """,
            {"owner": owner},
        )

        cur.execute(
            """
            SELECT table_name, num_rows,
                   TO_CHAR(last_analyzed, 'YYYY-MM-DD HH24:MI:SS'),
                   blocks
            FROM all_tables
            WHERE owner = :owner
            """,
            {"owner": owner},
        )
        row_signal_raw: dict[str, dict[str, Any]] = {}
        for table_name, num_rows, last_analyzed, blocks in cur.fetchall():
            row_signal_raw[str(table_name)] = {
                "num_rows": num_rows,
                "last_analyzed": last_analyzed,
                "blocks": blocks,
            }

        text_sources: dict[str, dict[str, Any]] = {
            "all_source": {
                "enabled": _can_select(cur, "all_source"),
                "sql": (
                    "SELECT COUNT(*) FROM all_source "
                    "WHERE owner = :owner AND UPPER(text) LIKE '%' || :needle || '%'"
                ),
            },
            "all_views": {
                "enabled": _can_select(cur, "all_views"),
                "sql": (
                    "SELECT COUNT(*) FROM all_views "
                    "WHERE owner = :owner AND UPPER(TO_LOB(text)) LIKE '%' || :needle || '%'"
                ),
            },
            "all_triggers": {
                "enabled": _can_select(cur, "all_triggers"),
                "sql": (
                    "SELECT COUNT(*) FROM all_triggers "
                    "WHERE owner = :owner AND UPPER(TO_LOB(trigger_body)) LIKE '%' || :needle || '%'"
                ),
            },
        }

        for key in ("all_views", "all_triggers"):
            if not text_sources[key]["enabled"]:
                continue
            try:
                cur.execute(text_sources[key]["sql"], {"owner": owner, "needle": "___PROBE___"})
            except Exception:
                text_sources[key]["enabled"] = False

        per_table_text_hits: dict[str, dict[str, Any]] = {}
        for table in tables:
            needle_upper = table.upper()
            hits: dict[str, Any] = {"all_source": None, "all_views": None, "all_triggers": None}
            total = 0
            for source_name, meta in text_sources.items():
                if not meta["enabled"]:
                    continue
                count = _text_hit_count(cur, meta["sql"], owner=owner, needle_upper=needle_upper)
                hits[source_name] = count
                total += count
            hits["text_hit_count"] = total
            hits["text_scan_complete"] = bool(text_sources["all_source"]["enabled"])
            per_table_text_hits[table] = hits

        row_signals_rows: list[list[Any]] = []
        per_table_row_signal: dict[str, dict[str, Any]] = {}
        for table in tables:
            raw = row_signal_raw.get(table, {})
            num_rows = raw.get("num_rows")
            last_analyzed = raw.get("last_analyzed")
            blocks = raw.get("blocks")
            row_count_value = None
            method = "UNKNOWN"
            if num_rows is not None:
                row_count_value = int(num_rows)
                method = "STATS"
            elif blocks is not None and int(blocks) <= blocks_threshold:
                try:
                    quoted = f"\"{_safe_ident(owner)}\".\"{_safe_ident(table)}\""
                    cur.execute(f"SELECT COUNT(*) FROM {quoted}")
                    row_count_value = int(cur.fetchone()[0] or 0)
                    method = "COUNT"
                except Exception:
                    row_count_value = None
                    method = "UNKNOWN"
            per_table_row_signal[table] = {
                "num_rows": num_rows,
                "last_analyzed": last_analyzed,
                "blocks": blocks,
                "row_count_value": row_count_value,
                "row_count_method": method,
                "row_count_threshold_blocks": blocks_threshold,
            }
            row_signals_rows.append(
                [
                    table,
                    num_rows,
                    last_analyzed,
                    blocks,
                    row_count_value,
                    method,
                    blocks_threshold,
                ]
            )

    inventory_csv_rows = [[r.table_name, r.status, r.last_ddl_time] for r in inventory_rows]
    _write_csv(
        out_dir / "tables_inventory.csv",
        ["table_name", "status", "last_ddl_time"],
        inventory_csv_rows,
    )

    _write_csv(
        out_dir / "dependency_counts.csv",
        ["table_name", "dependency_count"],
        [[t, dependency_counts.get(t, 0)] for t in tables],
    )
    _write_csv(
        out_dir / "fk_inbound_counts.csv",
        ["table_name", "fk_inbound_count"],
        [[t, fk_inbound_counts.get(t, 0)] for t in tables],
    )
    _write_csv(
        out_dir / "synonym_counts.csv",
        ["table_name", "synonym_count"],
        [[t, synonym_counts.get(t, 0)] for t in tables],
    )
    _write_csv(
        out_dir / "row_signals.csv",
        [
            "table_name",
            "num_rows",
            "last_analyzed",
            "blocks",
            "row_count_value",
            "row_count_method",
            "row_count_threshold_blocks",
        ],
        row_signals_rows,
    )

    text_hits_rows: list[list[Any]] = []
    for t in tables:
        hits = per_table_text_hits.get(t, {})
        text_hits_rows.append(
            [
                t,
                hits.get("all_source"),
                hits.get("all_views"),
                hits.get("all_triggers"),
                hits.get("text_hit_count"),
                hits.get("text_scan_complete"),
            ]
        )
    _write_csv(
        out_dir / "text_hits.csv",
        [
            "table_name",
            "all_source_hits",
            "all_views_hits",
            "all_triggers_hits",
            "text_hit_count",
            "text_scan_complete",
        ],
        text_hits_rows,
    )

    summary_rows: list[list[Any]] = []
    unused_tables: list[str] = []
    counts_by_class: dict[str, int] = {"USED_LIKELY": 0, "USED_POSSIBLE": 0, "UNUSED_CANDIDATE": 0}
    for inv in inventory_rows:
        t = inv.table_name
        dep = int(dependency_counts.get(t, 0))
        fk = int(fk_inbound_counts.get(t, 0))
        syn = int(synonym_counts.get(t, 0))
        text_info = per_table_text_hits.get(t, {})
        text_scan_complete = bool(text_info.get("text_scan_complete"))
        text_hits = text_info.get("text_hit_count")
        text_hits_int = int(text_hits or 0)
        row_info = per_table_row_signal.get(t, {})

        notes: list[str] = []
        if not text_scan_complete:
            notes.append("text_scan_incomplete")

        if dep > 0 or fk > 0:
            classification = "USED_LIKELY"
        elif syn > 0 or (text_scan_complete and text_hits_int > 0) or (not text_scan_complete):
            classification = "USED_POSSIBLE"
        else:
            classification = "UNUSED_CANDIDATE"

        if classification == "UNUSED_CANDIDATE":
            unused_tables.append(t)
        counts_by_class[classification] = counts_by_class.get(classification, 0) + 1

        summary_rows.append(
            [
                t,
                inv.status,
                inv.last_ddl_time,
                dep,
                fk,
                syn,
                text_hits_int if text_scan_complete else None,
                text_scan_complete,
                row_info.get("num_rows"),
                row_info.get("last_analyzed"),
                row_info.get("blocks"),
                row_info.get("row_count_value"),
                row_info.get("row_count_method"),
                classification,
                ";".join(notes) if notes else "",
            ]
        )

    _write_csv(
        out_dir / "usage_summary_tables.csv",
        [
            "table_name",
            "status",
            "last_ddl_time",
            "dependency_count",
            "fk_inbound_count",
            "synonym_count",
            "text_hit_count",
            "text_scan_complete",
            "num_rows",
            "last_analyzed",
            "blocks",
            "row_count_value",
            "row_count_method",
            "classification",
            "notes",
        ],
        summary_rows,
    )

    (out_dir / "unused_tables.txt").write_text(
        "\n".join(unused_tables) + ("\n" if unused_tables else ""),
        encoding="utf-8",
    )

    print(f"output_folder={out_dir}")
    print(
        "classification_counts="
        + ",".join(
            f"{k}:{counts_by_class.get(k, 0)}"
            for k in ("USED_LIKELY", "USED_POSSIBLE", "UNUSED_CANDIDATE")
        )
    )
    print("unused_preview_first_50:")
    for line in unused_tables[:50]:
        print(line)


if __name__ == "__main__":
    main()
