"""Minimal Oracle connectivity probe (no secrets logged)."""

from __future__ import annotations

import os
import sys
import time

try:
    import oracledb
except Exception as exc:  # pragma: no cover - diagnostic only
    print(f"import_fail: {type(exc).__name__}: {exc}", file=sys.stderr)
    sys.exit(1)


def _redact(value: str) -> str:
    if not value:
        return "<empty>"
    if len(value) <= 2:
        return value[0] + "*"
    return value[0] + "*" * max(1, len(value) - 2) + value[-1]


def main() -> None:
    user = os.environ.get("DB_USER")
    pwd = (
        os.environ.get("DB_PASSWORD")
        or os.environ.get("DB_PASS")
        or os.environ.get("DB_PWD")
    )
    dsn = os.environ.get("DB_DSN")
    tns_admin = os.environ.get("TNS_ADMIN")
    wallet_pwd = os.environ.get("WALLET_PWD")

    try:
        oracledb.init_oracle_client(config_dir=tns_admin)
    except Exception:
        # harmless if already initialized or thin mode
        pass

    print("env_check:", {
        "DB_USER": _redact(user or ""),
        "DB_PASS/DB_PASSWORD": "<set>" if pwd else "<missing>",
        "DB_DSN": dsn or "<missing>",
        "TNS_ADMIN": tns_admin or "<missing>",
    })

    if not (user and pwd and dsn):
        print("missing required envs; aborting", file=sys.stderr)
        sys.exit(2)

    start = time.perf_counter()
    try:
        conn = oracledb.connect(
            user=user,
            password=pwd,
            dsn=dsn,
            config_dir=tns_admin,
            wallet_location=tns_admin,
            wallet_password=wallet_pwd,
            # keep it lightweight and fast-fail
            stmtcachesize=0,
            retry_count=0,
            retry_delay=1,
            tcp_connect_timeout=5.0,
        )
    except Exception as exc:
        elapsed = int((time.perf_counter() - start) * 1000)
        code = getattr(getattr(exc, "args", [None])[0], "code", None)
        print(f"connect_fail: {type(exc).__name__} code={code} elapsed_ms={elapsed} msg={exc}")
        sys.exit(3)

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM dual")
            row = cur.fetchone()
            ok = row == (1,)
    except Exception as exc:
        elapsed = int((time.perf_counter() - start) * 1000)
        print(f"select_fail: {type(exc).__name__} elapsed_ms={elapsed} msg={exc}")
        conn.close()
        sys.exit(4)

    conn.close()
    elapsed = int((time.perf_counter() - start) * 1000)
    print(f"connect_ok: row={row} ok={ok} elapsed_ms={elapsed}")


if __name__ == "__main__":
    main()
