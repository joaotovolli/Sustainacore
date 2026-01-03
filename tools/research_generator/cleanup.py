"""Cleanup research generator artifacts and Oracle rows."""
from __future__ import annotations

import argparse
import sys
from typing import Dict, Optional, Tuple

from .oracle import get_connection, init_env, table_exists


def _count(conn, sql: str, params: Optional[Dict[str, str]] = None) -> int:
    cur = conn.cursor()
    cur.execute(sql, params or {})
    row = cur.fetchone()
    return int(row[0]) if row else 0


def _delete(conn, sql: str, params: Optional[Dict[str, str]] = None) -> int:
    cur = conn.cursor()
    cur.execute(sql, params or {})
    return cur.rowcount or 0


def _table_count(conn, table_name: str) -> Optional[int]:
    if not table_exists(conn, table_name):
        return None
    return _count(conn, f"SELECT COUNT(*) FROM {table_name}")


def _cleanup_table(conn, table_name: str, *, dry_run: bool) -> Tuple[Optional[int], Optional[int]]:
    before = _table_count(conn, table_name)
    if before is None:
        return None, None
    if dry_run:
        return before, before
    deleted = _delete(conn, f"DELETE FROM {table_name}")
    conn.commit()
    after = _table_count(conn, table_name)
    return before, after


def _cleanup_approvals(conn, *, dry_run: bool) -> Tuple[Optional[int], Optional[int]]:
    if not table_exists(conn, "PROC_GEMINI_APPROVALS"):
        return None, None
    before = _count(
        conn,
        "SELECT COUNT(*) FROM PROC_GEMINI_APPROVALS WHERE UPPER(TRIM(REQUEST_TYPE)) = 'RESEARCH_POST'",
    )
    if dry_run:
        return before, before
    _delete(
        conn,
        "DELETE FROM PROC_GEMINI_APPROVALS WHERE UPPER(TRIM(REQUEST_TYPE)) = 'RESEARCH_POST'",
    )
    conn.commit()
    after = _count(
        conn,
        "SELECT COUNT(*) FROM PROC_GEMINI_APPROVALS WHERE UPPER(TRIM(REQUEST_TYPE)) = 'RESEARCH_POST'",
    )
    return before, after


def run_cleanup(*, dry_run: bool) -> int:
    init_env()
    try:
        with get_connection() as conn:
            summary = []
            approvals = _cleanup_approvals(conn, dry_run=dry_run)
            summary.append(("PROC_GEMINI_APPROVALS (RESEARCH_POST)", approvals))

            for table in (
                "PROC_RESEARCH_REQUESTS",
                "PROC_REPORTS",
                "PROC_RESEARCH_REPORTS",
                "PROC_RESEARCH_ALERTS",
            ):
                summary.append((table, _cleanup_table(conn, table, dry_run=dry_run)))

            print("Cleanup summary:")
            for name, counts in summary:
                if counts == (None, None):
                    print(f"- {name}: not found")
                else:
                    before, after = counts
                    print(f"- {name}: before={before} after={after}")
            return 0
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"cleanup_failed: {exc}")
        return 1


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Cleanup research generator rows.")
    parser.add_argument("--dry-run", action="store_true", help="Show counts without deleting.")
    parser.add_argument("--yes", action="store_true", help="Actually delete rows.")
    args = parser.parse_args(argv)

    if not args.yes:
        return run_cleanup(dry_run=True)
    if args.dry_run:
        return run_cleanup(dry_run=True)
    return run_cleanup(dry_run=False)


if __name__ == "__main__":
    raise SystemExit(main())
