from __future__ import annotations

import os

import oracledb as _oracledb

_lib_dir = os.getenv("ORACLE_CLIENT_LIB_DIR", "/opt/oracle/instantclient_23")
_config_dir = os.getenv("TNS_ADMIN", "/opt/adb_wallet_tp")
try:
    _oracledb.init_oracle_client(lib_dir=_lib_dir, config_dir=_config_dir)
except Exception:
    pass

globals().update(_oracledb.__dict__)
