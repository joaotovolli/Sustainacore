"""Deterministic Oracle connectivity preflight for VM1."""

from __future__ import annotations

import pathlib
import sys
import time

ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.oracle.env_bootstrap import load_env_files, required_keys_present

from db_helper import get_connection


def _print_presence(presence: dict[str, bool]) -> None:
    for key in (
        "DB_USER",
        "DB_DSN",
        "TNS_ADMIN",
        "DB_PASSWORD|DB_PASS|DB_PWD",
        "WALLET_PWD",
    ):
        status = "present" if presence.get(key) else "missing"
        print(f"{key}: {status}")


def main() -> int:
    load_env_files()
    presence = required_keys_present()
    _print_presence(presence)

    start = time.perf_counter()
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM dual")
                row = cur.fetchone()
                ok = row == (1,)
    except Exception as exc:
        elapsed_ms = int((time.perf_counter() - start) * 1000)
        print(f"CONNECTIVITY_FAIL elapsed_ms={elapsed_ms} error={exc}")
        return 1

    elapsed_ms = int((time.perf_counter() - start) * 1000)
    if ok:
        print(f"CONNECTIVITY_OK elapsed_ms={elapsed_ms}")
        return 0

    print(f"CONNECTIVITY_FAIL elapsed_ms={elapsed_ms} error=unexpected_result")
    return 1


if __name__ == "__main__":
    sys.exit(main())
