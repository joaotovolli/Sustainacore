from __future__ import annotations

from tools.index_engine.env_loader import load_default_env
from db_helper import get_connection


def main() -> int:
    load_default_env()
    try:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT USER FROM dual")
            row = cur.fetchone()
            user = row[0] if row else None
            print(f"PASS oracle_user={user}")
            return 0
    except Exception as exc:
        print(f"FAIL error={exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
