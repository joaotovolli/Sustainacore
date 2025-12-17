"""Job run bookkeeping helpers for the index engine."""
from __future__ import annotations

import logging
import uuid
from typing import Any, Dict, Optional

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


def start_run(
    run_type: str,
    *,
    end_date: Any,
    provider: str,
    max_provider_calls: Any,
    meta: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Insert a START row; returns the run_id used.

    meta allows optional fields: run_id, start_date, oracle_user, usage_current,
    usage_limit, usage_remaining, credit_buffer.
    """

    meta = meta or {}
    run_id = meta.get("run_id") or str(uuid.uuid4())
    sql = (
        "INSERT INTO SC_IDX_JOB_RUNS "
        "(run_id, job_name, started_at, start_date, end_date, provider, oracle_user, usage_current, usage_limit, "
        "usage_remaining, credit_buffer, max_provider_calls, status) "
        "VALUES (:run_id, :job_name, SYSTIMESTAMP, :start_date, :end_date, :provider, :oracle_user, "
        ":usage_current, :usage_limit, :usage_remaining, :credit_buffer, :max_provider_calls, 'STARTED')"
    )
    binds = {
        "run_id": run_id,
        "job_name": run_type,
        "start_date": meta.get("start_date"),
        "end_date": end_date,
        "provider": provider,
        "oracle_user": meta.get("oracle_user"),
        "usage_current": meta.get("usage_current"),
        "usage_limit": meta.get("usage_limit"),
        "usage_remaining": meta.get("usage_remaining"),
        "credit_buffer": meta.get("credit_buffer"),
        "max_provider_calls": max_provider_calls,
    }
    try:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute(sql, binds)
            conn.commit()
    except Exception as exc:  # pragma: no cover - database availability
        LOGGER.warning("run_log: failed to insert start row: %s", exc)
    return run_id


def finish_run(
    run_id: str,
    *,
    status: str,
    provider_calls_used: Any = None,
    raw_upserts: Any = None,
    canon_upserts: Any = None,
    raw_ok: Any = None,
    raw_missing: Any = None,
    raw_error: Any = None,
    max_provider_calls: Any = None,
    usage_current: Any = None,
    usage_limit: Any = None,
    usage_remaining: Any = None,
    oracle_user: Any = None,
    error: Any = None,
) -> None:
    """Update a run row with summary fields."""

    sql = (
        "UPDATE SC_IDX_JOB_RUNS SET "
        "ended_at = SYSTIMESTAMP, "
        "status = :status, "
        "error_msg = :error_msg, "
        "provider_calls_used = :provider_calls_used, "
        "raw_upserts = :raw_upserts, "
        "canon_upserts = :canon_upserts, "
        "raw_ok = :raw_ok, "
        "raw_error = :raw_error, "
        "raw_missing = :raw_missing, "
        "max_provider_calls = :max_provider_calls, "
        "usage_current = :usage_current, "
        "usage_limit = :usage_limit, "
        "usage_remaining = :usage_remaining, "
        "oracle_user = :oracle_user "
        "WHERE run_id = :run_id"
    )
    binds = {
        "run_id": run_id,
        "status": status,
        "error_msg": error,
        "provider_calls_used": provider_calls_used,
        "raw_upserts": raw_upserts,
        "canon_upserts": canon_upserts,
        "raw_ok": raw_ok,
        "raw_error": raw_error,
        "raw_missing": raw_missing,
        "max_provider_calls": max_provider_calls,
        "usage_current": usage_current,
        "usage_limit": usage_limit,
        "usage_remaining": usage_remaining,
        "oracle_user": oracle_user,
    }
    try:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute(sql, binds)
            conn.commit()
    except Exception as exc:  # pragma: no cover - database availability
        LOGGER.warning("run_log: failed to update run row: %s", exc)


__all__ = ["fetch_calls_used_today", "finish_run", "start_run"]
