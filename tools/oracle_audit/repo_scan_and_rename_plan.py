#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import os
import pathlib
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


DDL_PREFIXES = (
    "db/schema/",
    "db/migrations/",
    "app/apex/",
)


RUNTIME_EXCLUDE_PREFIXES = DDL_PREFIXES


ALLOWED_GLOBS = (
    "*.py",
    "*.sql",
    "*.md",
    "*.sh",
    "*.yml",
    "*.yaml",
    "*.json",
    "*.html",
    "*.js",
    "*.ts",
    "*.txt",
)

EXCLUDE_GLOBS = (
    # Avoid counting hits in previously generated audit bundles / inputs.
    "!ops/audits/oracle_unused_audit/**",
    "!tools/oracle_audit/inputs/**",
)


INTERNAL_PREFIXES = (
    "DR$",
    "VECTOR$",
    "MLOG$",
    "RUPD$",
    "AQ$",
    "SYS_IOT_OVER_",
    "SYS_IOT_TOP_",
    "SYS_IL",
    "BIN$",
)


@dataclass(frozen=True)
class ObjectEvidence:
    object_name: str
    object_type: str
    dependency_count: int
    fk_inbound_count: int
    synonym_count: int
    classification: str


def _is_internal(name: str) -> bool:
    upper = name.upper()
    return any(upper.startswith(prefix) for prefix in INTERNAL_PREFIXES)


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


def _load_usage_summary(path: Path) -> dict[str, ObjectEvidence]:
    out: dict[str, ObjectEvidence] = {}
    with path.open("r", newline="", encoding="utf-8", errors="ignore") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            name = (row.get("object_name") or "").strip().upper()
            obj_type = (row.get("object_type") or "").strip().upper()
            if not name or obj_type not in ("TABLE", "VIEW"):
                continue
            dep = int((row.get("dependency_count") or "0") or 0)
            fk = int((row.get("fk_inbound_count") or "0") or 0)
            syn = int((row.get("synonym_count") or "0") or 0)
            classification = (row.get("classification") or "").strip()
            out[f"{obj_type}:{name}"] = ObjectEvidence(
                object_name=name,
                object_type=obj_type,
                dependency_count=dep,
                fk_inbound_count=fk,
                synonym_count=syn,
                classification=classification,
            )
    return out


def _rg_locations(repo_root: Path, object_name: str) -> list[str]:
    escaped = re.escape(object_name)
    # Identifier-ish boundary: avoid substring false positives.
    pattern = rf"(?<![A-Za-z0-9_$#_]){escaped}(?![A-Za-z0-9_$#_])"

    cmd = ["rg", "--pcre2", "-n", "--no-heading", "--color=never", "-i"]
    for g in ALLOWED_GLOBS:
        cmd += ["-g", g]
    for g in EXCLUDE_GLOBS:
        cmd += ["-g", g]
    cmd += ["-e", pattern, str(repo_root)]

    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if proc.returncode not in (0, 1):
        raise RuntimeError(proc.stderr.strip() or f"rg_failed_rc={proc.returncode}")
    return [ln for ln in proc.stdout.splitlines() if ln.strip()]


def _classify_locations(repo_root: Path, raw_lines: list[str]) -> tuple[list[str], list[str]]:
    ddl_locs: list[str] = []
    runtime_locs: list[str] = []
    for ln in raw_lines:
        parts = ln.split(":", 2)
        if len(parts) < 2:
            continue
        file_path = parts[0]
        line_no = parts[1]
        try:
            rel = str(Path(file_path).resolve().relative_to(repo_root.resolve()))
        except Exception:
            rel = file_path
        rel_norm = rel.replace("\\", "/")
        loc = f"{rel_norm}:{line_no}"
        if rel_norm.startswith(DDL_PREFIXES):
            ddl_locs.append(loc)
        else:
            runtime_locs.append(loc)
    return runtime_locs, ddl_locs


def _write_hits_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Repo-wide scan and rename plan for Oracle unused objects.")
    parser.add_argument("--repo-root", default=str(ROOT), help="Repository root to scan.")
    parser.add_argument("--out-dir", required=True, help="Audit bundle output directory.")
    args = parser.parse_args(argv)

    repo_root = Path(args.repo_root)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    usage_summary = out_dir / "usage_summary_objects.csv"
    if not usage_summary.exists():
        raise SystemExit(f"missing_required_input={usage_summary}")

    evidence = _load_usage_summary(usage_summary)
    candidates = [
        ev
        for ev in evidence.values()
        if ev.classification == "UNUSED_CANDIDATE" and ev.object_type in ("TABLE", "VIEW")
    ]

    repo_hits_all_rows: list[dict[str, Any]] = []
    repo_hits_runtime_rows: list[dict[str, Any]] = []
    repo_hits_ddl_rows: list[dict[str, Any]] = []

    rename_candidates: list[str] = []
    rename_blocked: list[str] = []
    rename_plan_rows: list[dict[str, Any]] = []

    for ev in sorted(candidates, key=lambda x: (x.object_type, x.object_name)):
        internal = _is_internal(ev.object_name)
        raw_lines = _rg_locations(repo_root, ev.object_name)
        runtime_locs, ddl_locs = _classify_locations(repo_root, raw_lines)
        runtime_hits = len(runtime_locs)
        ddl_hits = len(ddl_locs)
        total_hits = runtime_hits + ddl_hits

        repo_hits_all_rows.append(
            {
                "object_name": ev.object_name,
                "object_type": ev.object_type,
                "runtime_hits": runtime_hits,
                "ddl_hits": ddl_hits,
                "total_hits": total_hits,
                "sample_locations": "; ".join((runtime_locs + ddl_locs)[:5]),
            }
        )
        repo_hits_runtime_rows.append(
            {
                "object_name": ev.object_name,
                "object_type": ev.object_type,
                "hit_count": runtime_hits,
                "hit_locations": "; ".join(runtime_locs[:5]),
            }
        )
        repo_hits_ddl_rows.append(
            {
                "object_name": ev.object_name,
                "object_type": ev.object_type,
                "hit_count": ddl_hits,
                "hit_locations": "; ".join(ddl_locs[:5]),
            }
        )

        category = "oracle_managed_internal" if internal else "application_or_apex_object"

        notes: list[str] = []
        if internal:
            recommended_action = "DO_NOT_RENAME_INTERNAL"
            proposed_old_name = ""
            notes.append("oracle_managed_internal")
        elif ev.dependency_count == 0 and runtime_hits == 0:
            recommended_action = "RENAME_TO_OLD"
            proposed_old_name = _suggest_old_name(ev.object_name)
            if ddl_hits > 0:
                notes.append("ddl_mentions_only")
        else:
            recommended_action = "BLOCKED_NEEDS_REVIEW"
            proposed_old_name = _suggest_old_name(ev.object_name)
            if ev.dependency_count > 0:
                notes.append("oracle_dependencies_present")
            if runtime_hits > 0:
                notes.append("runtime_code_mentions")

        if (not internal) and ev.dependency_count == 0 and runtime_hits == 0:
            rename_candidates.append(ev.object_name)
        else:
            reason_bits: list[str] = []
            if internal:
                reason_bits.append("internal")
            if ev.dependency_count > 0:
                reason_bits.append(f"hard_deps={ev.dependency_count}")
            if runtime_hits > 0:
                reason_bits.append(f"runtime_hits={runtime_hits}")
            if ddl_hits > 0:
                reason_bits.append(f"ddl_hits={ddl_hits}")
            rename_blocked.append(f"{ev.object_type}:{ev.object_name}\t" + ",".join(reason_bits))

        rename_plan_rows.append(
            {
                "object_name": ev.object_name,
                "object_type": ev.object_type,
                "category": category,
                "proposed_old_name": proposed_old_name,
                "runtime_hits": runtime_hits,
                "ddl_hits": ddl_hits,
                "hard_deps": ev.dependency_count,
                "inbound_fk": ev.fk_inbound_count if ev.object_type == "TABLE" else 0,
                "synonyms": ev.synonym_count,
                "notes": ";".join(notes),
                "recommended_action": recommended_action,
            }
        )

    _write_hits_csv(
        out_dir / "repo_hits_all.csv",
        repo_hits_all_rows,
        ["object_name", "object_type", "runtime_hits", "ddl_hits", "total_hits", "sample_locations"],
    )
    _write_hits_csv(
        out_dir / "repo_hits_runtime.csv",
        repo_hits_runtime_rows,
        ["object_name", "object_type", "hit_count", "hit_locations"],
    )
    _write_hits_csv(
        out_dir / "repo_hits_ddl.csv",
        repo_hits_ddl_rows,
        ["object_name", "object_type", "hit_count", "hit_locations"],
    )

    (out_dir / "rename_candidates_tables_views.txt").write_text(
        "\n".join(rename_candidates) + ("\n" if rename_candidates else ""),
        encoding="utf-8",
    )
    (out_dir / "rename_blocked_tables_views.txt").write_text(
        "\n".join(rename_blocked) + ("\n" if rename_blocked else ""),
        encoding="utf-8",
    )

    _write_hits_csv(
        out_dir / "rename_plan_proposed.csv",
        rename_plan_rows,
        [
            "object_name",
            "object_type",
            "category",
            "proposed_old_name",
            "runtime_hits",
            "ddl_hits",
            "hard_deps",
            "inbound_fk",
            "synonyms",
            "notes",
            "recommended_action",
        ],
    )

    print(f"out_dir={out_dir}")
    print(f"unused_candidates_scanned={len(candidates)}")
    print(f"rename_candidates={len(rename_candidates)} rename_blocked={len(rename_blocked)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
