"""Job run bookkeeping helpers for the index engine."""
from __future__ import annotations

import logging
from typing import Any

from db_helper import get_connection

LOGGER = logging.getLogger(__name__)


def fetch_calls_used_today(provider: str) -> int:
    """
    Return the total provider_calls_used for today (UTC day via SYSTIMESTAMP).

    Falls back to 0 on errors to avoid blocking the job, but logs a warning.
    """

    sql = (
        "SELECT COALESCE(SUM(provider_calls_used), 0) "
        "FROM SC_IDX_JOB_RUNS "
        "WHERE provider = :provider "
        "AND started_at >= TRUNC(SYSTIMESTAMP) "
        "AND started_at < TRUNC(SYSTIMESTAMP) + 1"
    )

    try:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute(sql, {"provider": provider})
            row = cur.fetchone()
            return int(row[0]) if row and row[0] is not None else 0
    except Exception as exc:  # pragma: no cover - database availability
        LOGGER.warning("run_log: failed to fetch daily usage: %s", exc)
        return 0


__all__ = ["fetch_calls_used_today"]
