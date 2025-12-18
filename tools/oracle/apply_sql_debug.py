"""Debug SQL apply helper for Oracle (prints unit previews)."""
from __future__ import annotations

import argparse
import pathlib
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
APP_ROOT = REPO_ROOT / "app"
for path in (REPO_ROOT, APP_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from tools.index_engine.env_loader import load_default_env
from db_helper import get_connection


def _is_comment(line: str) -> bool:
    stripped = line.strip()
    return not stripped or stripped.startswith("--")


def _split_sql_units(text: str) -> list[str]:
    units: list[str] = []
    buffer: list[str] = []
    in_plsql = False

    for raw_line in text.splitlines():
        if _is_comment(raw_line):
            continue
        line = raw_line.rstrip()
        if line.strip() == "/":
            if buffer:
                units.append("\n".join(buffer).strip())
                buffer = []
            in_plsql = False
            continue
        buffer.append(line)
        if not in_plsql:
            if line.strip().upper().startswith("BEGIN"):
                in_plsql = True
            elif line.strip().endswith(";"):
                statement = "\n".join(buffer).strip()
                units.append(statement)
                buffer = []
        elif line.strip().upper().startswith("END;"):
            # Expect a trailing slash next; keep block until then.
            continue

    if buffer:
        units.append("\n".join(buffer).strip())
    return [unit for unit in units if unit.strip()]


def _trim_trailing_semicolon(unit: str) -> str:
    text = unit.rstrip()
    upper = text.lstrip().upper()
    if upper.startswith("BEGIN") or upper.startswith("DECLARE"):
        return text
    if text.endswith(";"):
        return text[:-1].rstrip()
    return text


def apply_sql_file(path: pathlib.Path) -> None:
    content = path.read_text(encoding="utf-8")
    units = _split_sql_units(content)
    if not units:
        print(f"no_units file={path}")
        return

    with get_connection() as conn:
        cur = conn.cursor()
        for idx, unit in enumerate(units, start=1):
            preview = unit.replace("\n", " ")[:80]
            print(f"unit file={path.name} index={idx} preview={preview}")
            try:
                cur.execute(_trim_trailing_semicolon(unit))
            except Exception as exc:
                error_preview = unit.replace("\n", " ")[:200]
                print(f"ERROR file={path.name} index={idx} preview={error_preview} error={exc}")
                raise
        conn.commit()


def main() -> int:
    parser = argparse.ArgumentParser(description="Debug apply Oracle SQL file.")
    parser.add_argument("sql_file", help="Path to .sql file")
    args = parser.parse_args()

    load_default_env()
    path = pathlib.Path(args.sql_file)
    if not path.exists():
        print(f"missing_file={path}")
        return 1
    apply_sql_file(path)
    print("apply_ok")
    return 0


if __name__ == "__main__":
    sys.exit(main())
