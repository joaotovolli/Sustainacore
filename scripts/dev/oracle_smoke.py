#!/usr/bin/env python3
"""Oracle smoke check (local-only).

Runs a simple SELECT 1 FROM dual if required env vars are present.
Prints only success/failure and timing; never prints credentials.
"""
from __future__ import annotations

import os
import time

try:
    import oracledb
except Exception as exc:
    print(f"Oracle driver unavailable: {exc}")
    raise SystemExit(1)


def _get_env(name: str) -> str | None:
    value = os.getenv(name)
    return value.strip() if value else None


def main() -> int:
    user = _get_env("ORACLE_USER") or _get_env("DB_USER")
    password = _get_env("ORACLE_PASSWORD") or _get_env("DB_PASSWORD") or _get_env("DB_PASS")
    dsn = _get_env("ORACLE_DSN") or _get_env("DB_DSN")

    if not user or not password or not dsn:
        print("Oracle not configured; skipping")
        return 0

    start = time.monotonic()
    try:
        conn = oracledb.connect(user=user, password=password, dsn=dsn)
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM dual")
            cur.fetchone()
        conn.close()
    except Exception as exc:
        elapsed = (time.monotonic() - start) * 1000
        print(f"Oracle smoke check failed (elapsed_ms={elapsed:.1f}): {exc.__class__.__name__}")
        return 1

    elapsed = (time.monotonic() - start) * 1000
    print(f"Oracle smoke check OK (elapsed_ms={elapsed:.1f})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
