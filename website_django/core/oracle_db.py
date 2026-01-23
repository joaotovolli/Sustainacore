from __future__ import annotations

import os

import oracledb

_INIT_DONE = False
_POOL = None


def _init_oracle_client() -> None:
    global _INIT_DONE
    if _INIT_DONE:
        return
    lib_dir = os.getenv("ORACLE_CLIENT_LIB_DIR", "/opt/oracle/instantclient_23")
    config_dir = os.getenv("TNS_ADMIN", "/opt/adb_wallet_tp")
    oracledb.init_oracle_client(lib_dir=lib_dir, config_dir=config_dir)
    _INIT_DONE = True


def _get_pool() -> oracledb.ConnectionPool | None:
    global _POOL
    if _POOL is not None:
        return _POOL
    _init_oracle_client()
    user = os.getenv("DB_USER") or os.getenv("ORACLE_USER")
    password = os.getenv("DB_PASSWORD") or os.getenv("DB_PASS") or os.getenv("ORACLE_PASSWORD")
    dsn = os.getenv("DB_DSN") or os.getenv("ORACLE_DSN") or "dbri4x6_high"
    if not user or not password:
        return None
    try:
        min_size = int(os.getenv("ORACLE_POOL_MIN", "1"))
        max_size = int(os.getenv("ORACLE_POOL_MAX", "4"))
        increment = int(os.getenv("ORACLE_POOL_INCREMENT", "1"))
        timeout = int(os.getenv("ORACLE_POOL_TIMEOUT", "60"))
        wait_timeout = int(os.getenv("ORACLE_POOL_WAIT_TIMEOUT", "3"))
    except ValueError:
        min_size, max_size, increment, timeout, wait_timeout = 1, 4, 1, 60, 3
    try:
        _POOL = oracledb.create_pool(
            user=user,
            password=password,
            dsn=dsn,
            min=min_size,
            max=max_size,
            increment=increment,
            timeout=timeout,
            wait_timeout=wait_timeout,
            getmode=oracledb.POOL_GETMODE_WAIT,
        )
        return _POOL
    except Exception:
        _POOL = None
        return None


def get_connection() -> oracledb.Connection:
    _init_oracle_client()
    user = os.getenv("DB_USER") or os.getenv("ORACLE_USER")
    password = os.getenv("DB_PASSWORD") or os.getenv("DB_PASS") or os.getenv("ORACLE_PASSWORD")
    dsn = os.getenv("DB_DSN") or os.getenv("ORACLE_DSN") or "dbri4x6_high"
    pool = _get_pool()
    if pool is not None:
        return pool.acquire()
    return oracledb.connect(user=user, password=password, dsn=dsn)
