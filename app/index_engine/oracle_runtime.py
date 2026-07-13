"""Oracle execution bounds for controlled SC_IDX reconstruction."""

from __future__ import annotations

import os

DEFAULT_RECON_ORACLE_CALL_TIMEOUT_MS = 300_000
RECON_ORACLE_CALL_TIMEOUT_ENV = "SC_IDX_RECON_ORACLE_CALL_TIMEOUT_MS"


def reconstruction_call_timeout_ms() -> int:
    raw = os.getenv(
        RECON_ORACLE_CALL_TIMEOUT_ENV,
        str(DEFAULT_RECON_ORACLE_CALL_TIMEOUT_MS),
    )
    value = int(raw)
    if value <= 0:
        raise ValueError("reconstruction_oracle_call_timeout_must_be_positive")
    return value


def configure_reconstruction_connection(conn):
    """Apply the bounded Oracle statement timeout and return ``conn``."""
    conn.call_timeout = reconstruction_call_timeout_ms()
    return conn


def configure_reconstruction_connection_if_enabled(conn):
    """Leave ordinary scheduled connections unchanged unless reconstruction opted in."""
    if os.getenv(RECON_ORACLE_CALL_TIMEOUT_ENV) is not None:
        return configure_reconstruction_connection(conn)
    return conn


__all__ = [
    "DEFAULT_RECON_ORACLE_CALL_TIMEOUT_MS",
    "RECON_ORACLE_CALL_TIMEOUT_ENV",
    "configure_reconstruction_connection",
    "configure_reconstruction_connection_if_enabled",
    "reconstruction_call_timeout_ms",
]
