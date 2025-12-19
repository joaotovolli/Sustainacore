import os

import oracledb

_INIT_DONE = False


def _init_oracle_client() -> None:
    global _INIT_DONE
    if _INIT_DONE:
        return
    lib_dir = os.getenv("ORACLE_CLIENT_LIB_DIR", "/opt/oracle/instantclient_23")
    config_dir = os.getenv("TNS_ADMIN", "/opt/adb_wallet_tp")
    oracledb.init_oracle_client(lib_dir=lib_dir, config_dir=config_dir)
    _INIT_DONE = True


def get_connection() -> oracledb.Connection:
    _init_oracle_client()
    user = os.getenv("DB_USER") or os.getenv("ORACLE_USER")
    password = os.getenv("DB_PASSWORD") or os.getenv("DB_PASS") or os.getenv("ORACLE_PASSWORD")
    dsn = os.getenv("DB_DSN") or os.getenv("ORACLE_DSN") or "dbri4x6_high"
    return oracledb.connect(user=user, password=password, dsn=dsn)
