#!/usr/bin/env python3
"""Oracle smoke check (local-only).

Runs a simple SELECT 1 FROM dual if required env vars are present.
Prints only success/failure and timing; never prints credentials.
"""
from __future__ import annotations

import os
import time
import concurrent.futures

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
    tns_admin = _get_env("ORACLE_TNS_ADMIN") or _get_env("TNS_ADMIN")
    wallet_dir = _get_env("ORACLE_WALLET_DIR")
    lib_dir = os.path.expanduser("~/.oracle/instantclient/current")

    if not user or not password or not dsn:
        print("Oracle not configured; skipping")
        return 0
    if tns_admin:
        os.environ["TNS_ADMIN"] = tns_admin
        try:
            oracledb.defaults.config_dir = tns_admin
        except Exception:
            pass
    elif wallet_dir:
        os.environ["TNS_ADMIN"] = wallet_dir
        try:
            oracledb.defaults.config_dir = wallet_dir
        except Exception:
            pass

    try:
        oracledb.init_oracle_client(lib_dir=lib_dir)
    except Exception:
        pass

    start = time.monotonic()

    def _run_query(dsn_value: str) -> None:
        conn_kwargs = {"user": user, "password": password, "dsn": dsn_value}
        conn = oracledb.connect(**conn_kwargs)
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM dual")
            cur.fetchone()
        conn.close()

    def _build_descriptor_from_tns(alias: str) -> str | None:
        config_dir = os.environ.get("TNS_ADMIN")
        if not config_dir:
            return None
        tns_path = os.path.join(config_dir, "tnsnames.ora")
        try:
            with open(tns_path, "r", encoding="utf-8", errors="ignore") as handle:
                text = handle.read()
        except Exception:
            return None
        block = []
        found = False
        for line in text.splitlines():
            if not found:
                if line.strip().startswith("#"):
                    continue
                if line.split("=")[0].strip() == alias:
                    found = True
                    block.append(line)
                continue
            block.append(line)
            if line.strip() == "":
                break
        if not found:
            return None
        import re

        hosts = re.findall(r"HOST\\s*=\\s*([^\\)\\s]+)", "\n".join(block), flags=re.IGNORECASE)
        ports = re.findall(r"PORT\\s*=\\s*([0-9]+)", "\n".join(block), flags=re.IGNORECASE)
        services = re.findall(
            r"SERVICE_NAME\\s*=\\s*([^\\)\\s]+)", "\n".join(block), flags=re.IGNORECASE
        )
        if not hosts or not ports or not services:
            return None
        host = hosts[0]
        port = ports[0]
        service = services[0]
        return (
            "(DESCRIPTION=(ADDRESS=(PROTOCOL=tcps)(HOST="
            + host
            + ")(PORT="
            + port
            + "))(CONNECT_DATA=(SERVICE_NAME="
            + service
            + "))(SECURITY=(SSL_SERVER_DN_MATCH=yes)))"
        )

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    future = executor.submit(_run_query, dsn)
    try:
        future.result(timeout=15)
    except concurrent.futures.TimeoutError:
        elapsed = (time.monotonic() - start) * 1000
        executor.shutdown(wait=False, cancel_futures=True)
        descriptor = _build_descriptor_from_tns(dsn)
        if descriptor:
            try:
                _run_query(descriptor)
                print(f"Oracle PASS (elapsed_ms={elapsed:.1f})")
                return 0
            except Exception as exc:
                msg = ""
                try:
                    raw = str(exc)
                    if "ORA-" in raw:
                        msg = raw[raw.find("ORA-") :].split()[0]
                except Exception:
                    msg = ""
                if msg:
                    print(f"Oracle FAIL (elapsed_ms={elapsed:.1f}): {exc.__class__.__name__} {msg}")
                else:
                    print(f"Oracle FAIL (elapsed_ms={elapsed:.1f}): {exc.__class__.__name__}")
                return 1
        print(f"Oracle FAIL (elapsed_ms={elapsed:.1f}): Timeout")
        return 1
    except Exception as exc:
        elapsed = (time.monotonic() - start) * 1000
        executor.shutdown(wait=False, cancel_futures=True)
        msg = ""
        try:
            raw = str(exc)
            # Keep only ORA-xxxxx code if present.
            if "ORA-" in raw:
                msg = raw[raw.find("ORA-") :].split()[0]
        except Exception:
            msg = ""
        if msg:
            print(f"Oracle FAIL (elapsed_ms={elapsed:.1f}): {exc.__class__.__name__} {msg}")
        else:
            print(f"Oracle FAIL (elapsed_ms={elapsed:.1f}): {exc.__class__.__name__}")
        return 1

    executor.shutdown(wait=False, cancel_futures=True)
    elapsed = (time.monotonic() - start) * 1000
    print(f"Oracle PASS (elapsed_ms={elapsed:.1f})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
