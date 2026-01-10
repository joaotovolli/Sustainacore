#!/usr/bin/env python3
"""Patch AI regulation CSVs with unambiguous AS_OF_DATE fixes."""
from __future__ import annotations

import argparse
import csv
import datetime as dt
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parents[3]


@dataclass
class PatchResult:
    filename: str
    rows_total: int
    blanks: int
    rows_fixed: int
    derived_date: Optional[str]
    unique_dates: int
    dominant_share: Optional[float]
    unambiguous: bool
    had_as_of_date: bool


def _parse_date(value: str, *, day_first: bool) -> Optional[dt.date]:
    text = value.strip()
    if not text:
        return None
    try:
        return dt.date.fromisoformat(text)
    except ValueError:
        pass
    fmt_candidates = ["%Y/%m/%d"]
    if day_first:
        fmt_candidates.insert(0, "%d/%m/%Y")
    else:
        fmt_candidates.insert(0, "%m/%d/%Y")
    for fmt in fmt_candidates:
        try:
            return dt.datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    try:
        return dt.datetime.fromisoformat(text).date()
    except ValueError:
        return None


def _normalize_date(value: str, *, day_first: bool) -> Tuple[str, bool]:
    parsed = _parse_date(value, day_first=day_first)
    if parsed is None:
        return value, False
    return parsed.isoformat(), True


def _derive_as_of_date(counts: Dict[str, int]) -> Tuple[Optional[str], Optional[float], bool]:
    if not counts:
        return None, None, False
    sorted_counts = sorted(counts.items(), key=lambda x: (-x[1], x[0]))
    if len(sorted_counts) == 1:
        return sorted_counts[0][0], 1.0, True
    total = sum(counts.values())
    dominant_value, dominant_count = sorted_counts[0]
    second_count = sorted_counts[1][1]
    share = dominant_count / total if total else 0.0
    if share >= 0.95 and dominant_count > second_count:
        return dominant_value, share, True
    return None, share, False


def _patch_csv(path: Path, *, day_first: bool) -> PatchResult:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        try:
            header = next(reader)
        except StopIteration:
            return PatchResult(
                filename=path.name,
                rows_total=0,
                blanks=0,
                rows_fixed=0,
                derived_date=None,
                unique_dates=0,
                dominant_share=None,
                unambiguous=True,
                had_as_of_date=False,
            )
        rows = [row for row in reader]

    rows_total = len(rows)
    if "AS_OF_DATE" not in header:
        return PatchResult(
            filename=path.name,
            rows_total=rows_total,
            blanks=0,
            rows_fixed=0,
            derived_date=None,
            unique_dates=0,
            dominant_share=None,
            unambiguous=True,
            had_as_of_date=False,
        )

    idx = header.index("AS_OF_DATE")
    counts: Dict[str, int] = {}
    blanks = 0
    for row in rows:
        value = row[idx].strip() if idx < len(row) else ""
        if not value:
            blanks += 1
            continue
        normalized, _ = _normalize_date(value, day_first=day_first)
        counts[normalized] = counts.get(normalized, 0) + 1

    derived_date, dominant_share, unambiguous = _derive_as_of_date(counts)
    if blanks > 0 and not derived_date:
        raise RuntimeError(
            f"as_of_date_ambiguous file={path.name} blanks={blanks} unique_dates={len(counts)}"
        )

    rows_fixed = 0
    patched_rows: List[List[str]] = []
    for row in rows:
        while len(row) < len(header):
            row.append("")
        value = row[idx].strip()
        if value:
            normalized, _ = _normalize_date(value, day_first=day_first)
            row[idx] = normalized
        elif derived_date:
            row[idx] = derived_date
            rows_fixed += 1
        patched_rows.append(row)

    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        writer.writerows(patched_rows)

    return PatchResult(
        filename=path.name,
        rows_total=rows_total,
        blanks=blanks,
        rows_fixed=rows_fixed,
        derived_date=derived_date,
        unique_dates=len(counts),
        dominant_share=dominant_share,
        unambiguous=unambiguous,
        had_as_of_date=True,
    )


def _write_audit(results: Iterable[PatchResult], *, input_dir: Path, day_first: bool) -> None:
    audit_path = REPO_ROOT / "infra" / "geo_ai_reg" / "output" / "patch_audit.md"
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    run_ts = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    with audit_path.open("a", encoding="utf-8") as handle:
        handle.write(f"\n## Patch run {run_ts} UTC\n")
        handle.write(f"- Input dir: {input_dir}\n")
        handle.write(f"- Day-first parsing: {day_first}\n")
        handle.write("\n| File | Rows | Blanks | Rows fixed | Derived as-of | Unique dates | Dominant share |\n")
        handle.write("| --- | ---: | ---: | ---: | --- | ---: | ---: |\n")
        for result in results:
            share = f"{result.dominant_share:.2f}" if result.dominant_share is not None else "n/a"
            derived = result.derived_date or "n/a"
            handle.write(
                f"| {result.filename} | {result.rows_total} | {result.blanks} | {result.rows_fixed} | {derived} | {result.unique_dates} | {share} |\n"
            )


def main() -> int:
    parser = argparse.ArgumentParser(description="Patch AI regulation bundle CSVs")
    parser.add_argument("--input-dir", required=True, help="Directory with extracted CSVs")
    parser.add_argument("--day-first", action="store_true", default=True, help="Parse DD/MM/YYYY dates")
    args = parser.parse_args()

    input_dir = Path(args.input_dir).expanduser()
    if not input_dir.exists():
        raise SystemExit(f"missing_input_dir={input_dir}")

    results: List[PatchResult] = []
    for path in sorted(input_dir.glob("*.csv")):
        result = _patch_csv(path, day_first=args.day_first)
        results.append(result)
        if result.had_as_of_date:
            print(
                "patch_result "
                f"file={result.filename} rows={result.rows_total} blanks={result.blanks} "
                f"fixed={result.rows_fixed} derived={result.derived_date or 'n/a'}"
            )

    _write_audit(results, input_dir=input_dir, day_first=args.day_first)
    print("patch_complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
