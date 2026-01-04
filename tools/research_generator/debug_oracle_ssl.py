"""Debug Oracle SSL connectivity for research generator."""
from __future__ import annotations

import os
import traceback

from tools.oracle.env_bootstrap import load_env_files
import db_helper


def main() -> int:
    load_env_files()
    tns_admin = os.environ.get("TNS_ADMIN")
    dsn = os.environ.get("DB_DSN")
    user = os.environ.get("DB_USER")
    print(f"TNS_ADMIN={tns_admin}")
    print(f"DB_DSN={dsn}")
    print(f"DB_USER={user}")
    try:
        with db_helper.get_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM dual")
            row = cur.fetchone()
            print(f"SELECT 1 -> {row}")
        print("Connection OK")
        return 0
    except Exception as exc:
        print("Connection failed:")
        print(repr(exc))
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
