#!/usr/bin/env python3
from __future__ import annotations

import csv
import hashlib
import pathlib
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.oracle.env_bootstrap import load_env_files


REPORT_DIR = Path("/home/opc/reports/oracle_unused_audit/20251217_090932")
UNUSED_TXT = REPORT_DIR / "unused_tables.txt"
OUT_CSV = REPORT_DIR / "rename_plan_full.csv"


@dataclass(frozen=True)
class Evidence:
    dependency_count: int = 0
    fk_inbound_count: int = 0
    synonym_count: int = 0
    text_hit_count: int = 0


def _read_unused_tables(path: Path) -> list[str]:
    tables: list[str] = []
    for raw in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        name = raw.strip()
        if not name or name.startswith("#"):
            continue
        tables.append(name.upper())
    return tables


def _read_counts_csv(path: Path, key_col: str, value_col: str) -> dict[str, int]:
    if not path.exists():
        return {}
    out: dict[str, int] = {}
    with path.open("r", newline="", encoding="utf-8", errors="ignore") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            key = (row.get(key_col) or "").strip().upper()
            if not key:
                continue
            raw = (row.get(value_col) or "").strip()
            try:
                out[key] = int(raw) if raw != "" else 0
            except ValueError:
                out[key] = 0
    return out


def _load_evidence(report_dir: Path) -> dict[str, Evidence]:
    dep = _read_counts_csv(report_dir / "dependency_counts.csv", "table_name", "dependency_count")
    fk = _read_counts_csv(report_dir / "fk_inbound_counts.csv", "table_name", "fk_inbound_count")
    syn = _read_counts_csv(report_dir / "synonym_counts.csv", "table_name", "synonym_count")
    text = _read_counts_csv(report_dir / "text_hits.csv", "table_name", "text_hit_count")

    tables = set(dep) | set(fk) | set(syn) | set(text)
    out: dict[str, Evidence] = {}
    for t in tables:
        out[t] = Evidence(
            dependency_count=int(dep.get(t, 0)),
            fk_inbound_count=int(fk.get(t, 0)),
            synonym_count=int(syn.get(t, 0)),
            text_hit_count=int(text.get(t, 0)),
        )
    return out


def _hash8(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:8].upper()


def _suggest_old_name(name: str) -> str:
    prefix = "OLD_"
    candidate = prefix + name
    if len(candidate) <= 30:
        return candidate
    digest = _hash8(name)
    max_trunc = 30 - len(prefix) - len(digest) - 1
    trunc = name[: max(0, max_trunc)]
    return f"{prefix}{digest}_{trunc}"[:30]


def _parse_oracle_text_index_name(dr_table: str) -> str | None:
    if not dr_table.upper().startswith("DR$"):
        return None
    body = dr_table[3:]
    parts = body.split("$")
    if len(parts) < 2:
        return None
    return "$".join(parts[:-1]).upper().strip() or None


def _can_select(cur, view: str) -> bool:
    try:
        cur.execute(f"SELECT 1 FROM {view} WHERE 1=0")
        return True
    except Exception:
        return False


def _lookup_text_index(cur, text_index_name: str | None) -> dict[str, Any]:
    if not text_index_name:
        return {"exists": False, "base_table_name": None, "notes": "unable_to_parse_text_index_name"}

    # 1) Oracle Text dictionary (if accessible)
    if _can_select(cur, "ctx_user_indexes"):
        try:
            cur.execute(
                "SELECT idx_name, idx_table, idx_text_name FROM ctx_user_indexes WHERE idx_name = :n",
                {"n": text_index_name},
            )
            row = cur.fetchone()
            if row:
                idx_name, base_table, text_col = row
                notes = f"ctx_user_indexes: index={idx_name} base_table={base_table} text_col={text_col}"
                return {
                    "exists": True,
                    "base_table_name": str(base_table).upper() if base_table else None,
                    "notes": notes,
                }
        except Exception:
            pass

    # 2) Normal dictionary views
    try:
        cur.execute(
            "SELECT table_name FROM user_indexes WHERE index_name = :n",
            {"n": text_index_name},
        )
        row = cur.fetchone()
        if row:
            return {
                "exists": True,
                "base_table_name": str(row[0]).upper() if row[0] else None,
                "notes": "user_indexes_match",
            }
    except Exception:
        pass

    return {"exists": False, "base_table_name": None, "notes": "index_not_found_or_inaccessible"}


def main() -> int:
    load_env_files()

    import db_helper  # noqa: WPS433 (repo-local import, intentionally after env bootstrap)

    unused_tables = _read_unused_tables(UNUSED_TXT)
    evidence = _load_evidence(REPORT_DIR)

    rows: list[dict[str, Any]] = []
    counts_by_category: dict[str, int] = {"oracle_text_internal": 0, "application_or_apex_table": 0}

    with db_helper.get_connection() as conn:
        cur = conn.cursor()

        for obj in unused_tables:
            is_dr = obj.startswith("DR$")
            category = "oracle_text_internal" if is_dr else "application_or_apex_table"
            counts_by_category[category] = counts_by_category.get(category, 0) + 1

            recommended_action = "RENAME_TEXT_INDEX_NOT_TABLE" if is_dr else "RENAME_TABLE_TO_OLD"
            suggested_old_name = "" if is_dr else _suggest_old_name(obj)

            text_index_name = ""
            base_table_name = ""
            notes = ""
            if is_dr:
                parsed = _parse_oracle_text_index_name(obj)
                text_index_name = parsed or ""
                lookup = _lookup_text_index(cur, parsed)
                base_table_name = lookup.get("base_table_name") or ""
                notes = lookup.get("notes") or ""

            ev = evidence.get(obj, Evidence())
            rows.append(
                {
                    "object_name": obj,
                    "category": category,
                    "recommended_action": recommended_action,
                    "suggested_old_name": suggested_old_name,
                    "text_index_name": text_index_name,
                    "base_table_name": base_table_name,
                    "notes": notes,
                    "hard_deps": ev.dependency_count,
                    "fk_inbound": ev.fk_inbound_count,
                    "synonyms": ev.synonym_count,
                    "text_hits": ev.text_hit_count,
                }
            )

    header = [
        "object_name",
        "category",
        "recommended_action",
        "suggested_old_name",
        "text_index_name",
        "base_table_name",
        "notes",
        "hard_deps",
        "fk_inbound",
        "synonyms",
        "text_hits",
    ]
    with OUT_CSV.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=header)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    print(f"wrote_csv={OUT_CSV}")
    print(f"counts_by_category=oracle_text_internal:{counts_by_category.get('oracle_text_internal', 0)},"
          f"application_or_apex_table:{counts_by_category.get('application_or_apex_table', 0)}")
    print("preview_first_20_lines:")
    with OUT_CSV.open("r", encoding="utf-8", errors="ignore") as handle:
        for i, line in enumerate(handle):
            if i >= 20:
                break
            print(line.rstrip("\n"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

