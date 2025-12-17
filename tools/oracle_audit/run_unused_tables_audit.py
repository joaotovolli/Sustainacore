#!/usr/bin/env python3
from __future__ import annotations

import argparse
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

from tools.oracle.env_bootstrap import load_env_files


@dataclass(frozen=True)
class ObjectInventoryRow:
    object_name: str
    object_type: str
    status: str | None
    last_ddl_time: str | None


def _write_csv(path: Path, header: Iterable[str], rows: Iterable[Iterable[Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(list(header))
        for row in rows:
            writer.writerow(list(row))


def _safe_ident(name: str) -> str:
    return name.replace('"', '""')


def _query_dict(cur, sql: str, binds: dict[str, Any]) -> dict[tuple[str, str], int]:
    cur.execute(sql, binds)
    out: dict[tuple[str, str], int] = {}
    for obj_type, obj_name, count in cur.fetchall():
        if obj_name is None:
            continue
        key = (str(obj_type).upper(), str(obj_name).upper())
        out[key] = int(count or 0)
    return out


def _query_dict_single(cur, sql: str, binds: dict[str, Any]) -> dict[str, int]:
    cur.execute(sql, binds)
    out: dict[str, int] = {}
    for obj_name, count in cur.fetchall():
        if obj_name is None:
            continue
        out[str(obj_name).upper()] = int(count or 0)
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


def run_audit(*, out_dir: Path, blocks_threshold: int = 200) -> dict[str, Any]:
    load_env_files()

    import db_helper  # noqa: WPS433 (repo-local import, intentionally after env bootstrap)

    if out_dir.exists() and any(out_dir.iterdir()):
        raise FileExistsError(f"out_dir_not_empty={out_dir}")
    out_dir.mkdir(parents=True, exist_ok=True)

    with db_helper.get_connection() as conn:
        cur = conn.cursor()

        cur.execute("SELECT USER FROM dual")
        owner = str(cur.fetchone()[0]).upper()

        cur.execute(
            """
            SELECT object_name,
                   object_type,
                   status,
                   TO_CHAR(last_ddl_time, 'YYYY-MM-DD HH24:MI:SS')
            FROM user_objects
            WHERE object_type IN ('TABLE', 'VIEW')
            ORDER BY object_type, object_name
            """
        )
        inventory_rows: list[ObjectInventoryRow] = [
            ObjectInventoryRow(
                str(name).upper(),
                str(obj_type).upper(),
                str(status) if status is not None else None,
                last_ddl,
            )
            for name, obj_type, status, last_ddl in cur.fetchall()
        ]

        objects = [(r.object_type, r.object_name) for r in inventory_rows]
        tables = [r.object_name for r in inventory_rows if r.object_type == "TABLE"]
        views = [r.object_name for r in inventory_rows if r.object_type == "VIEW"]

        dependency_counts = _query_dict(
            cur,
            """
            SELECT referenced_type, referenced_name, COUNT(*)
            FROM all_dependencies
            WHERE referenced_owner = :owner
              AND referenced_type IN ('TABLE', 'VIEW')
            GROUP BY referenced_type, referenced_name
            """,
            {"owner": owner},
        )

        fk_inbound_counts = _query_dict_single(
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

        synonym_counts = _query_dict_single(
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
            SELECT table_name,
                   num_rows,
                   TO_CHAR(last_analyzed, 'YYYY-MM-DD HH24:MI:SS'),
                   blocks
            FROM all_tables
            WHERE owner = :owner
            """,
            {"owner": owner},
        )
        row_signal_raw: dict[str, dict[str, Any]] = {}
        for table_name, num_rows, last_analyzed, blocks in cur.fetchall():
            row_signal_raw[str(table_name).upper()] = {
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

        text_scan_complete = bool(text_sources["all_source"]["enabled"])
        per_object_text_hits: dict[tuple[str, str], dict[str, Any]] = {}
        for obj_type, obj_name in objects:
            needle_upper = obj_name.upper()
            hits: dict[str, Any] = {"all_source": None, "all_views": None, "all_triggers": None}
            total = 0
            for source_name, meta in text_sources.items():
                if not meta["enabled"]:
                    continue
                count = _text_hit_count(cur, meta["sql"], owner=owner, needle_upper=needle_upper)
                hits[source_name] = count
                total += count
            hits["text_hit_count"] = total
            hits["text_scan_complete"] = text_scan_complete
            per_object_text_hits[(obj_type, obj_name)] = hits

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

    _write_csv(
        out_dir / "objects_inventory.csv",
        ["object_name", "object_type", "status", "last_ddl_time"],
        [[r.object_name, r.object_type, r.status, r.last_ddl_time] for r in inventory_rows],
    )

    _write_csv(
        out_dir / "dependency_counts.csv",
        ["object_name", "object_type", "dependency_count"],
        [
            [obj_name, obj_type, dependency_counts.get((obj_type, obj_name), 0)]
            for obj_type, obj_name in objects
        ],
    )

    _write_csv(
        out_dir / "fk_inbound_counts.csv",
        ["table_name", "fk_inbound_count"],
        [[t, fk_inbound_counts.get(t, 0)] for t in tables],
    )

    _write_csv(
        out_dir / "synonym_counts.csv",
        ["object_name", "synonym_count"],
        [[obj_name, synonym_counts.get(obj_name, 0)] for _, obj_name in objects],
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

    _write_csv(
        out_dir / "text_hits.csv",
        [
            "object_name",
            "object_type",
            "all_source_hits",
            "all_views_hits",
            "all_triggers_hits",
            "text_hit_count",
            "text_scan_complete",
        ],
        [
            [
                obj_name,
                obj_type,
                (per_object_text_hits.get((obj_type, obj_name), {}) or {}).get("all_source"),
                (per_object_text_hits.get((obj_type, obj_name), {}) or {}).get("all_views"),
                (per_object_text_hits.get((obj_type, obj_name), {}) or {}).get("all_triggers"),
                (per_object_text_hits.get((obj_type, obj_name), {}) or {}).get("text_hit_count"),
                bool((per_object_text_hits.get((obj_type, obj_name), {}) or {}).get("text_scan_complete")),
            ]
            for obj_type, obj_name in objects
        ],
    )

    summary_rows_objects: list[list[Any]] = []
    unused_tables: list[str] = []
    unused_views: list[str] = []
    unused_objects: list[str] = []
    counts_by_class: dict[str, int] = {"USED_LIKELY": 0, "USED_POSSIBLE": 0, "UNUSED_CANDIDATE": 0}

    for inv in inventory_rows:
        key = (inv.object_type, inv.object_name)
        dep = int(dependency_counts.get(key, 0))
        fk = int(fk_inbound_counts.get(inv.object_name, 0)) if inv.object_type == "TABLE" else 0
        syn = int(synonym_counts.get(inv.object_name, 0))
        text_info = per_object_text_hits.get(key, {})
        scan_complete = bool(text_info.get("text_scan_complete"))
        text_hits_int = int(text_info.get("text_hit_count") or 0) if scan_complete else 0

        notes: list[str] = []
        if not scan_complete:
            notes.append("text_scan_incomplete")

        if dep > 0 or fk > 0:
            classification = "USED_LIKELY"
        elif syn > 0 or (scan_complete and text_hits_int > 0) or (not scan_complete):
            classification = "USED_POSSIBLE"
        else:
            classification = "UNUSED_CANDIDATE"

        if classification == "UNUSED_CANDIDATE":
            unused_objects.append(f"{inv.object_type}:{inv.object_name}")
            if inv.object_type == "TABLE":
                unused_tables.append(inv.object_name)
            elif inv.object_type == "VIEW":
                unused_views.append(inv.object_name)

        counts_by_class[classification] = counts_by_class.get(classification, 0) + 1

        row_info = per_table_row_signal.get(inv.object_name, {}) if inv.object_type == "TABLE" else {}
        summary_rows_objects.append(
            [
                inv.object_name,
                inv.object_type,
                inv.status,
                inv.last_ddl_time,
                dep,
                fk,
                syn,
                text_hits_int if scan_complete else None,
                scan_complete,
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
        out_dir / "usage_summary_objects.csv",
        [
            "object_name",
            "object_type",
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
        summary_rows_objects,
    )

    # Back-compat: tables-only summary file.
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
        [
            [
                r[0],
                r[2],
                r[3],
                r[4],
                r[5],
                r[6],
                r[7],
                r[8],
                r[9],
                r[10],
                r[11],
                r[12],
                r[13],
                r[14],
                r[15],
            ]
            for r in summary_rows_objects
            if r[1] == "TABLE"
        ],
    )

    (out_dir / "unused_tables.txt").write_text(
        "\n".join(unused_tables) + ("\n" if unused_tables else ""),
        encoding="utf-8",
    )
    (out_dir / "unused_views.txt").write_text(
        "\n".join(unused_views) + ("\n" if unused_views else ""),
        encoding="utf-8",
    )
    (out_dir / "unused_objects.txt").write_text(
        "\n".join(unused_objects) + ("\n" if unused_objects else ""),
        encoding="utf-8",
    )

    return {
        "out_dir": str(out_dir),
        "owner": owner,
        "total_objects": len(objects),
        "counts_by_class": counts_by_class,
    }


def _default_out_dir() -> Path:
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    return Path("/home/opc/reports/oracle_unused_audit") / ts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Oracle unused TABLE/VIEW audit (SELECT-only).")
    parser.add_argument("--out-dir", default=str(_default_out_dir()), help="Output directory for CSV/TXT artifacts.")
    parser.add_argument(
        "--blocks-threshold",
        type=int,
        default=int(os.environ.get("ORACLE_AUDIT_BLOCKS_THRESHOLD", "200")),
        help="If NUM_ROWS is NULL and BLOCKS <= threshold, run COUNT(*) (still SELECT-only).",
    )
    args = parser.parse_args(argv)

    result = run_audit(out_dir=Path(args.out_dir), blocks_threshold=int(args.blocks_threshold))
    counts = result["counts_by_class"]
    print(f"output_folder={result['out_dir']}")
    print(f"total_objects={result['total_objects']}")
    print(
        "classification_counts="
        + ",".join(f"{k}:{counts.get(k, 0)}" for k in ("USED_LIKELY", "USED_POSSIBLE", "UNUSED_CANDIDATE"))
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
