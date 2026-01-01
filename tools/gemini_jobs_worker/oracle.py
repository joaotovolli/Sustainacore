"""Oracle helpers for the Gemini Jobs Worker."""
from __future__ import annotations

import datetime as dt
import logging
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional, Set

import db_helper
from tools.oracle.env_bootstrap import load_env_files

from .learned_notes import LearnedNote, append_learned_note

LOGGER = logging.getLogger("gemini_jobs_worker.oracle")


@dataclass
class JobRecord:
    job_id: int
    routine_code: Optional[str]
    routine_label: Optional[str]
    routine_value: Optional[str]
    content_text: Optional[str]
    instructions: Optional[str]
    file_name: Optional[str]
    file_mime: Optional[str]
    file_blob: Optional[bytes]
    status: Optional[str]
    created_at: Optional[dt.datetime]
    updated_at: Optional[dt.datetime]
    result_text: Optional[str]
    error_text: Optional[str]


@dataclass
class ApprovalRecord:
    approval_id: int
    source_job_id: int
    request_type: Optional[str]
    title: Optional[str]
    proposed_text: Optional[str]
    details: Optional[str]
    gemini_comments: Optional[str]
    file_name: Optional[str]
    file_mime: Optional[str]
    file_blob: Optional[bytes]
    status: Optional[str]
    created_at: Optional[dt.datetime]
    decided_at: Optional[dt.datetime]
    decided_by: Optional[str]
    decision_notes: Optional[str]


def init_env() -> None:
    load_env_files()


def get_connection():
    return db_helper.get_connection()


def _read_lob(value: Any) -> Any:
    if value is None:
        return None
    if hasattr(value, "read"):
        try:
            return value.read()
        except Exception:
            return None
    return value


_TABLE_COLUMN_CACHE: Dict[str, Set[str]] = {}


def _get_table_columns(conn, table_name: str) -> Set[str]:
    cached = _TABLE_COLUMN_CACHE.get(table_name)
    if cached is not None:
        return cached
    cur = conn.cursor()
    cur.execute(
        """
        SELECT column_name
          FROM all_tab_columns
         WHERE table_name = :table_name
           AND owner = SYS_CONTEXT('USERENV','CURRENT_SCHEMA')
        """,
        {"table_name": table_name},
    )
    columns = {row[0].upper() for row in cur.fetchall()}
    _TABLE_COLUMN_CACHE[table_name] = columns
    return columns


def fetch_job(conn, job_id: int) -> Optional[JobRecord]:
    cur = conn.cursor()
    columns = _get_table_columns(conn, "PROC_GEMINI_JOBS")
    routine_col = "ROUTINE" if "ROUTINE" in columns else None
    routine_select = ", routine" if routine_col else ""
    cur.execute(
        f"""
        SELECT job_id, routine_code, routine_label{routine_select}, content_text, instructions,
               file_name, file_mime, file_blob, status, created_at, updated_at,
               result_text, error_text
          FROM proc_gemini_jobs
         WHERE job_id = :job_id
        """,
        {"job_id": job_id},
    )
    row = cur.fetchone()
    if not row:
        return None
    offset = 3 if routine_col else 2
    return JobRecord(
        job_id=int(row[0]),
        routine_code=row[1],
        routine_label=row[2],
        routine_value=row[3] if routine_col else None,
        content_text=_read_lob(row[offset + 1]),
        instructions=_read_lob(row[offset + 2]),
        file_name=row[offset + 3],
        file_mime=row[offset + 4],
        file_blob=_read_lob(row[offset + 5]),
        status=row[offset + 6],
        created_at=row[offset + 7],
        updated_at=row[offset + 8],
        result_text=_read_lob(row[offset + 9]),
        error_text=_read_lob(row[offset + 10]),
    )


def _select_job_id(
    cur,
    routine_code: str,
    routine_label: Optional[str],
    statuses: Iterable[str],
    *,
    include_routine_column: bool,
) -> Optional[int]:
    status_list = [s for s in statuses]
    binds: Dict[str, Any] = {
        "routine_code": routine_code.upper(),
        "routine_label": (routine_label or "").strip().upper(),
    }
    for idx, status in enumerate(status_list):
        binds[f"status_{idx}"] = status
    status_clause = ", ".join(f":status_{idx}" for idx in range(len(status_list)))
    routine_checks = [
        "UPPER(TRIM(routine_code)) = :routine_code",
        "(routine_code IS NULL AND UPPER(TRIM(routine_label)) = :routine_label)",
        "UPPER(TRIM(routine_label)) = :routine_label",
    ]
    if include_routine_column:
        routine_checks.append("UPPER(TRIM(routine)) = :routine_code")
        routine_checks.append("UPPER(TRIM(routine)) = :routine_label")
    cur.execute(
        f"""
        SELECT job_id FROM (
            SELECT job_id
              FROM proc_gemini_jobs
             WHERE UPPER(TRIM(status)) IN ({status_clause})
               AND ({' OR '.join(routine_checks)})
             ORDER BY created_at NULLS LAST, job_id
        ) WHERE ROWNUM = 1
        """,
        binds,
    )
    row = cur.fetchone()
    if row:
        return int(row[0])
    return None


def pick_job(conn, routine_code: str, routine_label: Optional[str]) -> Optional[JobRecord]:
    cur = conn.cursor()
    columns = _get_table_columns(conn, "PROC_GEMINI_JOBS")
    include_routine = "ROUTINE" in columns

    job_id = _select_job_id(
        cur, routine_code, routine_label, ["PENDING"], include_routine_column=include_routine
    )
    if not job_id:
        job_id = _select_job_id(
            cur, routine_code, routine_label, ["IN_PROGRESS"], include_routine_column=include_routine
        )
        if job_id:
            return fetch_job(conn, job_id)
        return None

    cur.execute(
        """
        UPDATE proc_gemini_jobs
           SET status = 'IN_PROGRESS',
               updated_at = SYSTIMESTAMP
         WHERE job_id = :job_id
           AND UPPER(TRIM(status)) = 'PENDING'
        """,
        {"job_id": job_id},
    )
    if cur.rowcount != 1:
        LOGGER.warning("claim job_id=%s not claimed rowcount=%s", job_id, cur.rowcount)
        conn.rollback()
        return None
    conn.commit()
    return fetch_job(conn, job_id)


def pick_newest_pending_job(conn, routine_code: str, routine_label: Optional[str]) -> Optional[JobRecord]:
    cur = conn.cursor()
    columns = _get_table_columns(conn, "PROC_GEMINI_JOBS")
    include_routine = "ROUTINE" in columns
    status_list = ["PENDING"]
    binds: Dict[str, Any] = {
        "routine_code": routine_code.upper(),
        "routine_label": (routine_label or "").strip().upper(),
    }
    for idx, status in enumerate(status_list):
        binds[f"status_{idx}"] = status
    status_clause = ", ".join(f":status_{idx}" for idx in range(len(status_list)))
    routine_checks = [
        "UPPER(TRIM(routine_code)) = :routine_code",
        "(routine_code IS NULL AND UPPER(TRIM(routine_label)) = :routine_label)",
        "UPPER(TRIM(routine_label)) = :routine_label",
    ]
    if include_routine:
        routine_checks.append("UPPER(TRIM(routine)) = :routine_code")
        routine_checks.append("UPPER(TRIM(routine)) = :routine_label")
    cur.execute(
        f"""
        SELECT job_id FROM (
            SELECT job_id
              FROM proc_gemini_jobs
             WHERE status IN ({status_clause})
               AND ({' OR '.join(routine_checks)})
             ORDER BY created_at DESC NULLS LAST, job_id DESC
        ) WHERE ROWNUM = 1
        """,
        binds,
    )
    row = cur.fetchone()
    if not row:
        return None
    job_id = int(row[0])
    cur.execute(
        """
        UPDATE proc_gemini_jobs
           SET status = 'IN_PROGRESS',
               updated_at = SYSTIMESTAMP
         WHERE job_id = :job_id
           AND UPPER(TRIM(status)) = 'PENDING'
        """,
        {"job_id": job_id},
    )
    if cur.rowcount != 1:
        conn.rollback()
        return None
    conn.commit()
    return fetch_job(conn, job_id)


def update_job_status(conn, job_id: int, status: str, *, result_text: Optional[str] = None, error_text: Optional[str] = None) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE proc_gemini_jobs
           SET status = :status,
               result_text = :result_text,
               error_text = :error_text,
               updated_at = SYSTIMESTAMP
         WHERE job_id = :job_id
        """,
        {
            "status": status,
            "result_text": result_text,
            "error_text": error_text,
            "job_id": job_id,
        },
    )
    conn.commit()


def claim_job(conn, job_id: int) -> bool:
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE proc_gemini_jobs
           SET status = 'IN_PROGRESS',
               updated_at = SYSTIMESTAMP
         WHERE job_id = :job_id
           AND UPPER(TRIM(status)) = 'PENDING'
        """,
        {"job_id": job_id},
    )
    if cur.rowcount != 1:
        conn.rollback()
        return False
    conn.commit()
    return True


def current_schema(conn) -> Optional[str]:
    cur = conn.cursor()
    cur.execute("SELECT SYS_CONTEXT('USERENV','CURRENT_SCHEMA') FROM dual")
    row = cur.fetchone()
    return row[0] if row else None


def status_counts_jobs(conn) -> Dict[str, int]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT UPPER(TRIM(status)) AS status, COUNT(*)
          FROM proc_gemini_jobs
         GROUP BY UPPER(TRIM(status))
        """
    )
    return {row[0]: int(row[1]) for row in cur.fetchall() if row[0]}


def status_counts_approvals(conn) -> Dict[str, int]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT UPPER(TRIM(status)) AS status, COUNT(*)
          FROM proc_gemini_approvals
         GROUP BY UPPER(TRIM(status))
        """
    )
    return {row[0]: int(row[1]) for row in cur.fetchall() if row[0]}


def fetch_recent_jobs(conn, limit: int = 5) -> list[dict[str, Any]]:
    columns = _get_table_columns(conn, "PROC_GEMINI_JOBS")
    routine_col = "ROUTINE" if "ROUTINE" in columns else None
    routine_select = ", routine" if routine_col else ""
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT job_id, routine_code, routine_label{routine_select}, status, created_at
          FROM proc_gemini_jobs
         ORDER BY created_at DESC NULLS LAST, job_id DESC
         FETCH FIRST :limit ROWS ONLY
        """,
        {"limit": limit},
    )
    rows = cur.fetchall()
    payload = []
    for row in rows:
        routine_value = row[3] if routine_col else None
        offset = 1 if routine_col else 0
        payload.append(
            {
                "job_id": row[0],
                "routine_code": row[1],
                "routine_label": row[2],
                "routine": routine_value,
                "status": row[3 + offset],
                "created_at": row[4 + offset],
            }
        )
    return payload


def fetch_recent_approvals(conn, limit: int = 5) -> list[dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT approval_id, status, source_job_id, created_at
          FROM proc_gemini_approvals
         ORDER BY created_at DESC, approval_id DESC
         FETCH FIRST :limit ROWS ONLY
        """,
        {"limit": limit},
    )
    rows = cur.fetchall()
    return [
        {
            "approval_id": row[0],
            "status": row[1],
            "source_job_id": row[2],
            "created_at": row[3],
        }
        for row in rows
    ]


def find_approvals_by_status(conn, status: str, limit: int = 10) -> list[ApprovalRecord]:
    cur = conn.cursor()
    columns = _get_table_columns(conn, "PROC_GEMINI_APPROVALS")
    include_comments = "GEMINI_COMMENTS" in columns
    select_comments = ", gemini_comments" if include_comments else ""
    cur.execute(
        f"""
        SELECT approval_id, source_job_id, request_type, title, proposed_text, details
               {select_comments},
               file_name, file_mime, file_blob, status, created_at, decided_at,
               decided_by, decision_notes
          FROM proc_gemini_approvals
         WHERE UPPER(TRIM(status)) = :status
         ORDER BY created_at DESC, approval_id DESC
         FETCH FIRST :limit ROWS ONLY
        """,
        {"status": status.upper(), "limit": limit},
    )
    rows = cur.fetchall()
    approvals = []
    for row in rows:
        approvals.append(
            ApprovalRecord(
                approval_id=int(row[0]),
                source_job_id=int(row[1]),
                request_type=row[2],
                title=row[3],
                proposed_text=_read_lob(row[4]),
                details=_read_lob(row[5]),
                gemini_comments=_read_lob(row[6]) if include_comments else None,
                file_name=row[7] if include_comments else row[6],
                file_mime=row[8] if include_comments else row[7],
                file_blob=_read_lob(row[9] if include_comments else row[8]),
                status=row[10] if include_comments else row[9],
                created_at=row[11] if include_comments else row[10],
                decided_at=row[12] if include_comments else row[11],
                decided_by=row[13] if include_comments else row[12],
                decision_notes=_read_lob(row[14] if include_comments else row[13]),
            )
        )
    return approvals


def count_pending_jobs(conn) -> int:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COUNT(*)
          FROM proc_gemini_jobs
         WHERE UPPER(TRIM(status)) = 'PENDING'
        """
    )
    row = cur.fetchone()
    return int(row[0]) if row else 0


def count_pending_approvals(conn) -> int:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COUNT(*)
          FROM proc_gemini_approvals
         WHERE UPPER(TRIM(status)) = 'PENDING'
        """
    )
    row = cur.fetchone()
    return int(row[0]) if row else 0


def fetch_pending_jobs(conn, limit: int = 5) -> list[dict[str, Any]]:
    columns = _get_table_columns(conn, "PROC_GEMINI_JOBS")
    routine_col = "ROUTINE" if "ROUTINE" in columns else None
    routine_select = ", routine" if routine_col else ""
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT job_id, routine_code, routine_label{routine_select}, status, created_at, file_name
          FROM proc_gemini_jobs
         WHERE UPPER(TRIM(status)) = 'PENDING'
         ORDER BY job_id DESC
         FETCH FIRST :limit ROWS ONLY
        """,
        {"limit": limit},
    )
    rows = cur.fetchall()
    payload = []
    for row in rows:
        routine_value = row[3] if routine_col else None
        offset = 1 if routine_col else 0
        payload.append(
            {
                "job_id": row[0],
                "routine_code": row[1],
                "routine_label": row[2],
                "routine": routine_value,
                "status": row[3 + offset],
                "created_at": row[4 + offset],
                "file_name": row[5 + offset],
            }
        )
    return payload


def insert_approval(conn, payload: Dict[str, Any]) -> int:
    cur = conn.cursor()
    columns = _get_table_columns(conn, "PROC_GEMINI_APPROVALS")
    include_comments = "GEMINI_COMMENTS" in columns

    gemini_comments = payload.pop("gemini_comments", None)
    if gemini_comments and not include_comments:
        append_learned_note(
            LearnedNote(
                "Approval table missing GEMINI_COMMENTS column",
                context="stored in DETAILS instead",
            )
        )
    column_list = [
        "source_job_id",
        "request_type",
        "title",
        "proposed_text",
        "details",
    ]
    values_list = [
        ":source_job_id",
        ":request_type",
        ":title",
        ":proposed_text",
        ":details",
    ]
    if include_comments:
        column_list.append("gemini_comments")
        values_list.append(":gemini_comments")
        payload["gemini_comments"] = gemini_comments

    column_list.extend(["file_name", "file_mime", "file_blob", "status", "created_at"])
    values_list.extend([":file_name", ":file_mime", ":file_blob", ":status", "SYSTIMESTAMP"])

    approval_id = cur.var(int)
    cur.execute(
        f"""
        INSERT INTO proc_gemini_approvals (
            {", ".join(column_list)}
        ) VALUES (
            {", ".join(values_list)}
        ) RETURNING approval_id INTO :approval_id
        """,
        {**payload, "approval_id": approval_id},
    )
    conn.commit()
    value = approval_id.getvalue()
    if isinstance(value, list) and value:
        value = value[0]
    return int(value)


def fetch_latest_approval(conn, job_id: int, request_type: str) -> Optional[ApprovalRecord]:
    cur = conn.cursor()
    columns = _get_table_columns(conn, "PROC_GEMINI_APPROVALS")
    include_comments = "GEMINI_COMMENTS" in columns
    select_comments = ", gemini_comments" if include_comments else ""
    cur.execute(
        f"""
        SELECT approval_id, source_job_id, request_type, title, proposed_text, details
               {select_comments},
               file_name, file_mime, file_blob, status, created_at, decided_at,
               decided_by, decision_notes
          FROM proc_gemini_approvals
         WHERE source_job_id = :job_id
           AND request_type = :request_type
         ORDER BY created_at DESC, approval_id DESC
        """,
        {"job_id": job_id, "request_type": request_type},
    )
    row = cur.fetchone()
    if not row:
        return None
    return ApprovalRecord(
        approval_id=int(row[0]),
        source_job_id=int(row[1]),
        request_type=row[2],
        title=row[3],
        proposed_text=_read_lob(row[4]),
        details=_read_lob(row[5]),
        gemini_comments=_read_lob(row[6]) if include_comments else None,
        file_name=row[7] if include_comments else row[6],
        file_mime=row[8] if include_comments else row[7],
        file_blob=_read_lob(row[9] if include_comments else row[8]),
        status=row[10] if include_comments else row[9],
        created_at=row[11] if include_comments else row[10],
        decided_at=row[12] if include_comments else row[11],
        decided_by=row[13] if include_comments else row[12],
        decision_notes=_read_lob(row[14] if include_comments else row[13]),
    )


def append_decision_notes(conn, approval_id: int, note: str) -> None:
    cur = conn.cursor()
    columns = _get_table_columns(conn, "PROC_GEMINI_APPROVALS")
    updates = []
    if "APPLIED_AT" in columns:
        updates.append("applied_at = COALESCE(applied_at, SYSTIMESTAMP)")
    if "DECISION_NOTES" in columns:
        updates.append(
            """
            decision_notes =
                CASE
                    WHEN decision_notes IS NULL THEN TO_CLOB(:note)
                    ELSE decision_notes || TO_CLOB(CHR(10)) || TO_CLOB(:note)
                END
            """
        )
    elif "DETAILS" in columns:
        updates.append(
            """
            details =
                CASE
                    WHEN details IS NULL THEN TO_CLOB(:note)
                    ELSE details || TO_CLOB(CHR(10)) || TO_CLOB(:note)
                END
            """
        )
    if not updates:
        return
    cur.execute(
        f"""
        UPDATE proc_gemini_approvals
           SET {", ".join(updates)}
         WHERE approval_id = :approval_id
        """,
        {"note": note, "approval_id": approval_id},
    )
    conn.commit()
