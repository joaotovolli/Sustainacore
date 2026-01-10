#!/usr/bin/env python3
"""Load the AI regulation Oracle bundle into the target schema."""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import io
import os
import re
import sys
import zipfile
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Tuple

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.oracle.env_bootstrap import load_env_files
import db_helper

try:  # pragma: no cover - optional typing helper
    import oracledb  # type: ignore
except Exception:  # pragma: no cover
    oracledb = None  # type: ignore


DDL_TABLE_PATTERN = re.compile(r"CREATE TABLE\s+(\w+)\s*\((.*?)\);", re.IGNORECASE | re.DOTALL)
COL_DEF_PATTERN = re.compile(r"^([A-Z0-9_]+)\s+([A-Z0-9]+(?:\([^\)]+\))?)", re.IGNORECASE)
FK_PATTERN = re.compile(
    r"FOREIGN KEY\s*\(([^\)]+)\)\s+REFERENCES\s+(\w+)\s*\(([^\)]+)\)",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class BundleFile:
    name: str
    opener: callable


@dataclass
class TableSpec:
    name: str
    columns: Dict[str, str]
    dependencies: List[str]


def _is_zip(path: Path) -> bool:
    return path.is_file() and zipfile.is_zipfile(path)


def _find_bundle_files(bundle: Path) -> Tuple[BundleFile, Dict[str, BundleFile]]:
    ddl_file: Optional[BundleFile] = None
    csv_files: Dict[str, BundleFile] = {}

    if _is_zip(bundle):
        archive = zipfile.ZipFile(bundle)
        names = archive.namelist()

        def opener(name: str) -> io.TextIOBase:
            return io.TextIOWrapper(archive.open(name), encoding="utf-8", newline="")

        for name in names:
            base = Path(name).name
            if base.lower().endswith(".sql") and ddl_file is None:
                ddl_file = BundleFile(name=name, opener=lambda n=name: opener(n))
            if base.lower().endswith(".csv"):
                csv_files[base.upper()] = BundleFile(name=name, opener=lambda n=name: opener(n))
    else:
        if not bundle.exists():
            raise FileNotFoundError(f"bundle_missing path={bundle}")
        for path in bundle.iterdir():
            if path.is_dir():
                continue
            name = path.name
            if name.lower().endswith(".sql") and ddl_file is None:
                ddl_file = BundleFile(name=name, opener=lambda p=path: p.open("r", encoding="utf-8", newline=""))
            if name.lower().endswith(".csv"):
                csv_files[name.upper()] = BundleFile(name=name, opener=lambda p=path: p.open("r", encoding="utf-8", newline=""))

    if ddl_file is None:
        raise FileNotFoundError("ddl_missing: no .sql file found in bundle")
    return ddl_file, csv_files


def _parse_ddl(ddl_text: str) -> Dict[str, TableSpec]:
    tables: Dict[str, TableSpec] = {}
    for match in DDL_TABLE_PATTERN.finditer(ddl_text):
        table = match.group(1).upper()
        body = match.group(2)
        columns: Dict[str, str] = {}
        deps: List[str] = []
        for raw_line in body.splitlines():
            line = raw_line.strip()
            if not line or line.startswith("--"):
                continue
            if line.endswith(","):
                line = line[:-1]
            if not line:
                continue
            if line.upper().startswith("CONSTRAINT"):
                fk_match = FK_PATTERN.search(line)
                if fk_match:
                    deps.append(fk_match.group(2).upper())
                continue
            col_match = COL_DEF_PATTERN.match(line)
            if col_match:
                col_name = col_match.group(1).upper()
                col_type = col_match.group(2).upper()
                columns[col_name] = col_type
        tables[table] = TableSpec(name=table, columns=columns, dependencies=sorted(set(deps)))
    return tables


def _toposort(tables: Mapping[str, TableSpec]) -> List[str]:
    remaining = set(tables.keys())
    resolved: List[str] = []
    while remaining:
        ready = sorted([name for name in remaining if not set(tables[name].dependencies) - set(resolved)])
        if not ready:
            raise RuntimeError(f"dependency_cycle tables={sorted(remaining)}")
        resolved.extend(ready)
        remaining -= set(ready)
    return resolved


def _split_sql_units(text: str) -> List[str]:
    units: List[str] = []
    buffer: List[str] = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("--"):
            continue
        buffer.append(raw_line.rstrip())
        if line.endswith(";"):
            statement = "\n".join(buffer).strip()
            if statement:
                units.append(statement.rstrip(";"))
            buffer = []
    if buffer:
        statement = "\n".join(buffer).strip()
        if statement:
            units.append(statement.rstrip(";"))
    return units


def _parse_date(value: str) -> Optional[dt.date]:
    try:
        return dt.date.fromisoformat(value)
    except ValueError:
        try:
            return dt.datetime.fromisoformat(value).date()
        except ValueError:
            return None


def _parse_timestamp(value: str) -> Optional[dt.datetime]:
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return dt.datetime.strptime(value, fmt)
        except ValueError:
            continue
    try:
        return dt.datetime.fromisoformat(value)
    except ValueError:
        return None


def _parse_number(value: str) -> object:
    if re.fullmatch(r"-?\d+", value):
        return int(value)
    if re.fullmatch(r"-?\d+\.\d+", value):
        return float(value)
    try:
        return Decimal(value)
    except Exception:
        return value


def _normalize_value(raw: str, col_type: Optional[str]) -> object:
    if raw is None:
        return None
    text = raw.strip()
    if not text or text.upper() == "NULL":
        return None
    if not col_type:
        return text
    upper_type = col_type.upper()
    if upper_type.startswith("DATE"):
        parsed = _parse_date(text)
        return parsed if parsed is not None else text
    if upper_type.startswith("TIMESTAMP"):
        parsed = _parse_timestamp(text)
        return parsed if parsed is not None else text
    if upper_type.startswith("NUMBER"):
        return _parse_number(text)
    return text


def _reader_from_bundle(bundle_file: BundleFile) -> Iterator[List[str]]:
    with bundle_file.opener() as handle:
        reader = csv.reader(handle)
        for row in reader:
            yield row


def _count_csv_rows(bundle_file: BundleFile) -> Tuple[List[str], int]:
    iterator = _reader_from_bundle(bundle_file)
    try:
        header = next(iterator)
    except StopIteration:
        return [], 0
    row_count = 0
    for row in iterator:
        if not row:
            continue
        row_count += 1
    return header, row_count


def _inputsizes(columns: Sequence[str], column_types: Mapping[str, str]) -> Optional[Sequence[object]]:
    if oracledb is None:
        return None
    sizes: List[object] = []
    for col in columns:
        col_type = column_types.get(col)
        if col_type and col_type.startswith("CLOB"):
            sizes.append(oracledb.CLOB)
        else:
            sizes.append(None)
    return sizes


def _load_table(
    conn,
    table: TableSpec,
    bundle_file: BundleFile,
    *,
    dry_run: bool,
    batch_size: int,
) -> Tuple[int, int]:
    iterator = _reader_from_bundle(bundle_file)
    try:
        header = next(iterator)
    except StopIteration:
        return 0, 0

    columns = [col.strip() for col in header]
    columns_upper = [col.upper() for col in columns]
    insert_sql = (
        f"INSERT INTO {table.name} (" + ",".join(columns_upper) + ") "
        f"VALUES (" + ",".join(f":{idx + 1}" for idx in range(len(columns_upper))) + ")"
    )

    expected_rows = 0
    loaded_rows = 0
    batch: List[List[object]] = []

    if dry_run:
        for row in iterator:
            if not row:
                continue
            expected_rows += 1
        return expected_rows, 0

    cur = conn.cursor()
    sizes = _inputsizes(columns_upper, table.columns)
    if sizes:
        cur.setinputsizes(*sizes)

    for row in iterator:
        if not row:
            continue
        expected_rows += 1
        if len(row) < len(columns_upper):
            row = row + [""] * (len(columns_upper) - len(row))
        values = [
            _normalize_value(row[idx], table.columns.get(columns_upper[idx]))
            for idx in range(len(columns_upper))
        ]
        batch.append(values)
        if len(batch) >= batch_size:
            cur.executemany(insert_sql, batch)
            loaded_rows += len(batch)
            batch = []

    if batch:
        cur.executemany(insert_sql, batch)
        loaded_rows += len(batch)

    return expected_rows, loaded_rows


def _table_count(conn, table_name: str) -> int:
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    return int(cur.fetchone()[0])


def _apply_ddl(conn, ddl_text: str) -> None:
    for unit in _split_sql_units(ddl_text):
        conn.cursor().execute(unit)
    conn.commit()


def _drop_tables(conn, table_order: Sequence[str]) -> None:
    for table in table_order:
        try:
            conn.cursor().execute(f"DROP TABLE {table} CASCADE CONSTRAINTS PURGE")
        except Exception as exc:
            if "ORA-00942" in str(exc):
                continue
            raise


def _truncate_tables(conn, table_order: Sequence[str]) -> None:
    for table in table_order:
        conn.cursor().execute(f"TRUNCATE TABLE {table}")


def _resolve_bundle_path(path: str) -> Path:
    resolved = Path(path).expanduser()
    if not resolved.is_absolute():
        resolved = (Path.cwd() / resolved).resolve()
    return resolved


def main() -> int:
    parser = argparse.ArgumentParser(description="Load AI regulation bundle into Oracle")
    parser.add_argument("--bundle", required=True, help="Path to bundle zip or extracted folder")
    parser.add_argument("--drop-and-recreate", action="store_true", help="Drop all tables and recreate schema")
    parser.add_argument("--truncate", action="store_true", help="Truncate tables before loading")
    parser.add_argument("--ddl-only", action="store_true", help="Apply DDL and exit")
    parser.add_argument("--dry-run", action="store_true", help="Preview load order and row counts")
    parser.add_argument("--batch-size", type=int, default=500, help="Rows per batch insert")
    args = parser.parse_args()

    bundle_path = _resolve_bundle_path(args.bundle)
    ddl_file, csv_files = _find_bundle_files(bundle_path)

    with ddl_file.opener() as handle:
        ddl_text = handle.read()

    tables = _parse_ddl(ddl_text)
    if not tables:
        print("ddl_parse_failed: no tables found")
        return 2

    load_env_files()

    load_order = _toposort(tables)
    reverse_order = list(reversed(load_order))

    csv_table_names = {Path(name).stem.upper(): bundle for name, bundle in csv_files.items()}
    tables_with_csv = [name for name in load_order if name in csv_table_names]
    missing_csv = sorted(name for name in tables if name not in csv_table_names)
    extra_csv = sorted(name for name in csv_table_names if name not in tables)
    if missing_csv:
        print("warning=missing_csv " + ",".join(missing_csv))
    if extra_csv:
        print("warning=extra_csv " + ",".join(extra_csv))

    if args.dry_run:
        print("dry_run=1")
        print(f"bundle={bundle_path}")
        print(f"ddl_file={ddl_file.name}")
        print("load_order=" + ",".join(tables_with_csv))
        for table_name in tables_with_csv:
            header, count = _count_csv_rows(csv_table_names[table_name])
            print(f"table={table_name} csv_rows={count} columns={len(header)}")
        return 0

    with db_helper.get_connection() as conn:
        if args.drop_and_recreate:
            print("action=drop_tables")
            _drop_tables(conn, reverse_order)
            print("action=apply_ddl")
            _apply_ddl(conn, ddl_text)
            if args.ddl_only:
                return 0
        elif args.ddl_only:
            print("action=apply_ddl")
            _apply_ddl(conn, ddl_text)
            return 0

        if args.truncate:
            print("action=truncate_tables")
            _truncate_tables(conn, [t for t in reverse_order if t in tables])

        for table_name in tables_with_csv:
            table = tables[table_name]
            bundle_file = csv_table_names[table_name]
            expected_rows, loaded_rows = _load_table(
                conn,
                table,
                bundle_file,
                dry_run=False,
                batch_size=args.batch_size,
            )
            conn.commit()
            actual_rows = _table_count(conn, table_name)
            print(
                "table_load_complete "
                f"table={table_name} csv_rows={expected_rows} loaded_rows={loaded_rows} "
                f"db_rows={actual_rows}"
            )
            if actual_rows < expected_rows:
                raise RuntimeError(
                    f"row_count_mismatch table={table_name} expected_min={expected_rows} actual={actual_rows}"
                )

    print("load_complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
