"""Repository runtime reference scanner for Oracle rename planning.

This utility reads the latest rename plan CSV, scans the repository for
word-boundary, case-insensitive matches to the listed object names, and writes
`repo_hits_runtime.csv` plus optional final rename artifacts.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
import re
from typing import Dict, Iterable, List, Sequence


DEFAULT_INCLUDE_PATTERNS: Sequence[str] = (
    "app/**",
    "tools/**",
    "index/**",
    "retrieval/**",
    "website_django/**",
    "infra/**",
)

DEFAULT_EXCLUDE_PATTERNS: Sequence[str] = (
    "db/schema/**",
    "db/migrations/**",
    "app/apex/**",
    "**/*.md",
)

DEFAULT_MAX_SAMPLES = 5


@dataclass
class PlanEntry:
    object_name: str
    category: str
    recommended_action: str
    suggested_old_name: str
    text_index_name: str


@dataclass
class HitResult:
    hit_count: int
    sample_locations: List[str]


def load_plan_entries(rename_plan: Path) -> List[PlanEntry]:
    entries: List[PlanEntry] = []
    with rename_plan.open(newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            entries.append(
                PlanEntry(
                    object_name=row["object_name"].strip(),
                    category=row.get("category", "").strip(),
                    recommended_action=row.get("recommended_action", "").strip(),
                    suggested_old_name=row.get("suggested_old_name", "").strip(),
                    text_index_name=row.get("text_index_name", "").strip(),
                )
            )
    return entries


def compile_pattern(name: str) -> re.Pattern[str]:
    """Compile a word-boundary-aware, case-insensitive pattern for the object name."""

    boundary_chars = r"[A-Za-z0-9_$]"
    escaped = re.escape(name)
    pattern = rf"(?<!{boundary_chars}){escaped}(?!{boundary_chars})"
    return re.compile(pattern, flags=re.IGNORECASE)


def should_scan(path: Path, include_patterns: Sequence[str], exclude_patterns: Sequence[str]) -> bool:
    rel = path.as_posix()
    included = any(path.match(pattern) for pattern in include_patterns)
    excluded = any(path.match(pattern) for pattern in exclude_patterns)
    return included and not excluded


def iter_candidate_files(root: Path, include_patterns: Sequence[str], exclude_patterns: Sequence[str]) -> Iterable[Path]:
    for file_path in root.rglob("*"):
        if not file_path.is_file():
            continue
        rel_path = file_path.relative_to(root)
        if should_scan(rel_path, include_patterns, exclude_patterns):
            yield file_path


def count_newlines_before(content: str, index: int) -> int:
    return content.count("\n", 0, index)


def scan_repo(
    root: Path,
    entries: Sequence[PlanEntry],
    include_patterns: Sequence[str],
    exclude_patterns: Sequence[str],
    max_samples: int,
) -> Dict[str, HitResult]:
    patterns = {entry.object_name: compile_pattern(entry.object_name) for entry in entries}
    results: Dict[str, HitResult] = {
        entry.object_name: HitResult(hit_count=0, sample_locations=[]) for entry in entries
    }

    for file_path in iter_candidate_files(root, include_patterns, exclude_patterns):
        try:
            content = file_path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue

        rel_display = file_path.relative_to(root).as_posix()
        for object_name, pattern in patterns.items():
            hit_result = results[object_name]
            for match in pattern.finditer(content):
                hit_result.hit_count += 1
                if len(hit_result.sample_locations) < max_samples:
                    line_no = count_newlines_before(content, match.start()) + 1
                    hit_result.sample_locations.append(f"{rel_display}:{line_no}")

    return results


def ensure_old_name(object_name: str, suggested_old_name: str) -> str:
    """Return an Oracle-safe OLD_ name within 30 chars using deterministic hashing."""

    candidate = suggested_old_name or f"OLD_{object_name}"
    if len(candidate) <= 30:
        return candidate

    digest = hashlib.sha1(object_name.encode()).hexdigest().upper()
    suffix = digest[:6]
    room_for_name = 30 - len("OLD_") - len(suffix) - 1
    trimmed = object_name[:room_for_name]
    return f"OLD_{trimmed}_{suffix}"


def write_repo_hits(output: Path, entries: Sequence[PlanEntry], results: Dict[str, HitResult], max_samples: int) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["object_name", "category", "hit_count", "sample_locations"])
        for entry in entries:
            hit = results.get(entry.object_name, HitResult(0, []))
            samples = " | ".join(hit.sample_locations[:max_samples])
            writer.writerow([entry.object_name, entry.category, hit.hit_count, samples])


def summarize_sample_paths(sample_locations: Sequence[str]) -> str:
    paths = []
    for sample in sample_locations:
        path_part = sample.split(":", 1)[0]
        if path_part not in paths:
            paths.append(path_part)
    return ", ".join(paths)


def build_final_tables(entries: Sequence[PlanEntry], results: Dict[str, HitResult]) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for entry in entries:
        if entry.recommended_action != "RENAME_TABLE_TO_OLD":
            continue
        hit = results.get(entry.object_name, HitResult(0, []))
        if entry.object_name.startswith("VECTOR$"):
            action = "SKIP"
            reason = "VECTOR$ internal – rename owning vector index during VM1 execution"
        elif hit.hit_count > 0:
            action = "SKIP"
            path_summary = summarize_sample_paths(hit.sample_locations)
            reason = f"runtime hits in {path_summary}" if path_summary else "runtime hits detected"
        else:
            action = "RENAME_TABLE"
            reason = "0 runtime hits"

        rows.append(
            {
                "object_name": entry.object_name,
                "suggested_old_name": ensure_old_name(entry.object_name, entry.suggested_old_name),
                "action": action,
                "reason": reason,
            }
        )
    return rows


def build_final_text_indexes(entries: Sequence[PlanEntry], results: Dict[str, HitResult]) -> List[Dict[str, str]]:
    grouped: Dict[str, List[PlanEntry]] = defaultdict(list)
    for entry in entries:
        if entry.text_index_name:
            grouped[entry.text_index_name].append(entry)

    rows: List[Dict[str, str]] = []
    for text_index_name, members in sorted(grouped.items()):
        total_hits = 0
        all_samples: List[str] = []
        for member in members:
            hit = results.get(member.object_name, HitResult(0, []))
            total_hits += hit.hit_count
            all_samples.extend(hit.sample_locations)

        if total_hits > 0:
            action = "SKIP"
            path_summary = summarize_sample_paths(all_samples)
            reason = (
                f"runtime hits on DR$ segments ({path_summary})" if path_summary else "runtime hits on DR$ segments"
            )
        else:
            action = "RENAME_INDEX"
            reason = "0 runtime hits on DR$ segments"

        rows.append(
            {
                "text_index_name": text_index_name,
                "suggested_old_name": ensure_old_name(text_index_name, ""),
                "action": action,
                "reason": reason,
            }
        )

    return rows


def write_csv(path: Path, headers: Sequence[str], rows: Sequence[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_lines(path: Path, values: Iterable[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as handle:
        for value in values:
            handle.write(f"{value}\n")


def write_final_artifacts(final_dir: Path, table_rows: List[Dict[str, str]], text_rows: List[Dict[str, str]], vector_rows: List[str], include_patterns: Sequence[str], exclude_patterns: Sequence[str]) -> None:
    write_csv(final_dir / "final_rename_tables.csv", ["object_name", "suggested_old_name", "action", "reason"], table_rows)
    write_lines(
        final_dir / "final_rename_tables.txt",
        [row["object_name"] for row in table_rows if row["action"] == "RENAME_TABLE"],
    )

    write_csv(final_dir / "final_rename_text_indexes.csv", ["text_index_name", "suggested_old_name", "action", "reason"], text_rows)
    write_lines(
        final_dir / "final_rename_text_indexes.txt",
        [row["text_index_name"] for row in text_rows if row["action"] == "RENAME_INDEX"],
    )

    write_lines(
        final_dir / "final_vector_internals.txt",
        [f"{name} -> rename the owning vector index (to be handled in VM1 execution step)" for name in vector_rows],
    )

    readme_path = final_dir / "README.md"
    rename_table_count = sum(1 for row in table_rows if row["action"] == "RENAME_TABLE")
    skip_table_count = sum(1 for row in table_rows if row["action"] == "SKIP")
    rename_index_count = sum(1 for row in text_rows if row["action"] == "RENAME_INDEX")
    skip_index_count = sum(1 for row in text_rows if row["action"] == "SKIP")

    readme_lines = [
        "# Runtime rename plan summary",
        "",
        "## Scan parameters",
        "- Case-insensitive, word-boundary matching",
        f"- Include paths: {', '.join(include_patterns)}",
        f"- Exclude paths: {', '.join(exclude_patterns)}",
        "",
        "## Counts",
        f"- Tables to rename: {rename_table_count}",
        f"- Tables skipped due to runtime signals or VECTOR$ internals: {skip_table_count}",
        f"- Oracle Text indexes to rename: {rename_index_count}",
        f"- Oracle Text indexes skipped: {skip_index_count}",
        f"- VECTOR$ internal objects logged: {len(vector_rows)}",
    ]

    readme_path.parent.mkdir(parents=True, exist_ok=True)
    readme_path.write_text("\n".join(readme_lines))


def collect_vector_rows(entries: Sequence[PlanEntry]) -> List[str]:
    return [entry.object_name for entry in entries if entry.object_name.startswith("VECTOR$")]


def derive_default_final_dir(rename_plan: Path) -> Path:
    timestamp = rename_plan.parent.name
    return rename_plan.parent.parent.parent / "final" / timestamp


def parse_args() -> argparse.Namespace:
    repo_root = Path(__file__).resolve().parents[2]
    default_plan = repo_root / "tools" / "oracle_audit" / "inputs" / "20251217_090932" / "rename_plan_full.csv"
    default_final_dir = derive_default_final_dir(default_plan)

    parser = argparse.ArgumentParser(description="Scan repository runtime references for Oracle rename planning.")
    parser.add_argument("--rename-plan", type=Path, default=default_plan, help="Path to rename_plan_full.csv")
    parser.add_argument("--root", type=Path, default=repo_root, help="Repository root to scan")
    parser.add_argument("--output", type=Path, default=default_final_dir / "repo_hits_runtime.csv", help="Output CSV path for runtime hits")
    parser.add_argument("--include", nargs="*", default=list(DEFAULT_INCLUDE_PATTERNS), help="Glob patterns to include")
    parser.add_argument("--exclude", nargs="*", default=list(DEFAULT_EXCLUDE_PATTERNS), help="Glob patterns to exclude")
    parser.add_argument("--max-samples", type=int, default=DEFAULT_MAX_SAMPLES, help="Maximum sample locations to retain per object")
    parser.add_argument("--generate-final-plan", action="store_true", help="Also write final rename artifacts to the derived final directory")
    parser.add_argument("--final-dir", type=Path, help="Optional override for final artifact directory")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    entries = load_plan_entries(args.rename_plan)

    results = scan_repo(
        root=args.root,
        entries=entries,
        include_patterns=args.include,
        exclude_patterns=args.exclude,
        max_samples=args.max_samples,
    )

    write_repo_hits(args.output, entries, results, args.max_samples)

    if args.generate_final_plan:
        final_dir = args.final_dir or derive_default_final_dir(args.rename_plan)
        table_rows = build_final_tables(entries, results)
        text_rows = build_final_text_indexes(entries, results)
        vector_rows = collect_vector_rows(entries)
        write_final_artifacts(final_dir, table_rows, text_rows, vector_rows, args.include, args.exclude)


if __name__ == "__main__":
    main()
