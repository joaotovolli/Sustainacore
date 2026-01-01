"""Entry point for the Gemini Jobs Worker."""
from __future__ import annotations

import argparse
import logging
import sys
import time
from typing import Optional

from . import config
from .approval import approval_is_applied, mark_applied
from .gemini_cli import log_startup_config
from .learned_notes import LearnedNote, append_learned_note
from .oracle import (
    ApprovalRecord,
    JobRecord,
    claim_job,
    count_pending_approvals,
    count_pending_jobs,
    current_schema,
    fetch_job,
    fetch_latest_approval,
    fetch_pending_jobs,
    get_connection,
    init_env,
    insert_approval,
    pick_job,
    pick_newest_pending_job,
    update_job_status,
)
from .routines.rag_ingest import (
    EmbeddingDimMismatch,
    apply_payload,
    build_approval_payload,
)

LOGGER = logging.getLogger("gemini_jobs_worker")


def _resolve_routine_code(job: JobRecord) -> Optional[str]:
    for candidate in (job.routine_code, job.routine_value, job.routine_label):
        if not candidate:
            continue
        key = candidate.strip()
        if not key:
            continue
        upper = key.upper()
        if upper in config.ROUTINE_LABEL_MAP.values():
            return upper
        for label, code in config.ROUTINE_LABEL_MAP.items():
            if label.strip().lower() == key.strip().lower():
                return code
        if upper in config.ROUTINE_LABEL_MAP.values():
            return upper
    return None


def _log_startup_diagnostics() -> None:
    init_env()
    with get_connection() as conn:
        schema = current_schema(conn)
        pending_jobs = count_pending_jobs(conn)
        pending_approvals = count_pending_approvals(conn)
    LOGGER.info("startup utc=%s", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
    LOGGER.info("oracle_schema=%s", schema)
    LOGGER.info("pending_jobs=%s pending_approvals=%s", pending_jobs, pending_approvals)
    LOGGER.info(
        "routine_mapping codes=%s labels=%s",
        sorted(set(config.ROUTINE_LABEL_MAP.values())),
        sorted(config.ROUTINE_LABEL_MAP.keys()),
    )


def _log_poll_state(conn) -> None:
    pending = count_pending_jobs(conn)
    jobs = fetch_pending_jobs(conn, limit=5)
    job_ids = [str(item.get("job_id")) for item in jobs]
    LOGGER.info("poll pending_jobs=%s job_ids=%s", pending, ",".join(job_ids))
    for item in jobs:
        routine_code = item.get("routine_code")
        routine_label = item.get("routine_label")
        routine_value = item.get("routine")
        LOGGER.info(
            "pending job_id=%s routine_code=%s routine_label=%s routine=%s status=%s file_name=%s",
            item.get("job_id"),
            routine_code,
            routine_label,
            routine_value,
            item.get("status"),
            item.get("file_name"),
        )
        normalized = _resolve_routine_code(
            JobRecord(
                job_id=int(item.get("job_id")),
                routine_code=routine_code,
                routine_label=routine_label,
                routine_value=routine_value,
                content_text=None,
                instructions=None,
                file_name=item.get("file_name"),
                file_mime=None,
                file_blob=None,
                status=item.get("status"),
                created_at=item.get("created_at"),
                updated_at=None,
                result_text=None,
                error_text=None,
            )
        )
        if not normalized:
            LOGGER.warning(
                "skip job_id=%s reason=routine_mismatch routine_code=%s routine_label=%s routine=%s",
                item.get("job_id"),
                routine_code,
                routine_label,
                routine_value,
            )


def _ensure_approval(conn, job: JobRecord, *, force_new: bool = False) -> ApprovalRecord:
    approval = fetch_latest_approval(conn, job.job_id, "ADD_VECTORS")
    if approval and not force_new:
        status = (approval.status or "").upper()
        if status != "REJECTED":
            return approval

    payload = build_approval_payload(job.job_id, job.file_blob or b"")
    if payload.quality_errors:
        update_job_status(
            conn,
            job.job_id,
            "FAILED",
            error_text="; ".join(payload.quality_errors)[:400],
        )
        raise RuntimeError("payload_quality_failed")
    title = f"RAG ingest: {job.file_name or 'attachment'} (JOB {job.job_id})"
    approval_id = insert_approval(
        conn,
        {
            "source_job_id": job.job_id,
            "request_type": "ADD_VECTORS",
            "title": title,
            "proposed_text": payload.proposed_text,
            "details": payload.details,
            "gemini_comments": payload.comments,
            "file_name": payload.file_name,
            "file_mime": payload.file_mime,
            "file_blob": payload.payload_bytes,
            "status": "PENDING",
        },
    )
    LOGGER.info(
        "Created approval approval_id=%s job_id=%s payload_file=%s",
        approval_id,
        job.job_id,
        payload.file_name,
    )
    approval = fetch_latest_approval(conn, job.job_id, "ADD_VECTORS")
    if approval and approval.approval_id == approval_id:
        return approval
    raise RuntimeError("approval_create_failed")


def _handle_rag_ingest(conn, job: JobRecord, *, dry_run: bool, create_only: bool) -> None:
    approval = _ensure_approval(conn, job, force_new=create_only)
    status = (approval.status or "").upper()

    if dry_run or create_only:
        LOGGER.info("Dry-run mode: approval created or confirmed, no insert applied.")
        update_job_status(
            conn,
            job.job_id,
            "IN_PROGRESS",
            result_text=f"Awaiting approval {approval.approval_id}.",
        )
        return

    if status == "PENDING":
        update_job_status(
            conn,
            job.job_id,
            "IN_PROGRESS",
            result_text=f"Awaiting approval {approval.approval_id}.",
        )
        return

    if status == "REJECTED":
        update_job_status(
            conn,
            job.job_id,
            "DONE",
            result_text=f"Rejected approval {approval.approval_id}.",
        )
        return

    if status != "APPROVED":
        update_job_status(
            conn,
            job.job_id,
            "IN_PROGRESS",
            result_text=f"Awaiting valid approval state for {approval.approval_id}.",
        )
        return

    if approval_is_applied(approval):
        update_job_status(
            conn,
            job.job_id,
            "DONE",
            result_text=f"Approval {approval.approval_id} already applied.",
        )
        return

    if not approval.file_blob:
        update_job_status(
            conn,
            job.job_id,
            "FAILED",
            error_text="Approval payload missing file blob.",
        )
        return

    try:
        stats = apply_payload(conn, approval.file_blob)
    except EmbeddingDimMismatch as exc:
        append_learned_note(
            LearnedNote(
                "Embedding dimension mismatch during apply",
                context=f"expected={exc.expected} actual={exc.actual}",
            )
        )
        mismatch_text = (
            f"Embedding dimension mismatch (expected {exc.expected}, got {exc.actual}). "
            "No inserts applied."
        )
        insert_approval(
            conn,
            {
                "source_job_id": job.job_id,
                "request_type": "OTHER",
                "title": f"Embedding dimension mismatch (JOB {job.job_id})",
                "proposed_text": mismatch_text,
                "details": mismatch_text,
                "gemini_comments": mismatch_text,
                "file_name": approval.file_name,
                "file_mime": approval.file_mime,
                "file_blob": approval.file_blob,
                "status": "PENDING",
            },
        )
        update_job_status(conn, job.job_id, "FAILED", error_text=mismatch_text)
        return
    except Exception as exc:
        append_learned_note(
            LearnedNote("Oracle insert failure", context=str(exc)[:200])
        )
        update_job_status(conn, job.job_id, "FAILED", error_text=str(exc)[:400])
        return

    mark_applied(conn, approval)
    update_job_status(
        conn,
        job.job_id,
        "DONE",
        result_text=(
            f"Applied approval {approval.approval_id}: inserted={stats.inserted}, "
            f"skipped_existing={stats.skipped_existing}."
        ),
    )


def _process_job(conn, job: JobRecord, *, dry_run: bool) -> None:
    routine_code = _resolve_routine_code(job)
    if not routine_code:
        update_job_status(
            conn,
            job.job_id,
            "FAILED",
            error_text="Unsupported routine code.",
        )
        return
    if routine_code not in config.SUPPORTED_ROUTINES:
        update_job_status(
            conn,
            job.job_id,
            "FAILED",
            error_text=f"Routine not supported by worker: {routine_code}.",
        )
        return

    if routine_code == "RAG_INGEST":
        _handle_rag_ingest(conn, job, dry_run=dry_run, create_only=False)
        return


def run_once(*, job_id: Optional[int], dry_run: bool, create_only: bool) -> bool:
    init_env()
    with get_connection() as conn:
        _log_poll_state(conn)
        if job_id is not None:
            job = fetch_job(conn, job_id)
        else:
            routine_label = "Text to be transformed and added to RAG Vectors"
            job = pick_job(conn, "RAG_INGEST", routine_label)
        if not job:
            LOGGER.info("No matching jobs found.")
            return False
        if not job.file_blob:
            LOGGER.warning("skip job_id=%s reason=file_missing", job.job_id)
            update_job_status(
                conn,
                job.job_id,
                "FAILED",
                error_text="Missing FILE_BLOB attachment.",
            )
            return True
        if create_only:
            _handle_rag_ingest(conn, job, dry_run=dry_run, create_only=True)
        else:
            _process_job(conn, job, dry_run=dry_run)
        return True


def run_loop(*, interval_seconds: int, job_id: Optional[int], dry_run: bool) -> None:
    idle_seconds = 0
    interval_seconds = config.FAST_POLL_SECONDS
    while True:
        job_found = run_once(job_id=job_id, dry_run=dry_run, create_only=False)
        if job_found:
            idle_seconds = 0
            time.sleep(0)
            continue

        idle_seconds += interval_seconds
        if idle_seconds <= config.FAST_POLL_WINDOW_SECONDS:
            interval_seconds = config.FAST_POLL_SECONDS
        elif idle_seconds <= config.FAST_POLL_WINDOW_SECONDS * 2:
            interval_seconds = config.MEDIUM_POLL_SECONDS
        else:
            interval_seconds = config.DEFAULT_POLL_SECONDS
        time.sleep(interval_seconds)


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Gemini Jobs Worker")
    parser.add_argument("--once", action="store_true", help="Run a single poll cycle.")
    parser.add_argument("--loop", action="store_true", help="Run in a polling loop.")
    parser.add_argument("--interval", type=int, default=config.DEFAULT_POLL_SECONDS)
    parser.add_argument("--job-id", type=int, help="Process a specific job id.")
    parser.add_argument("--dry-run", action="store_true", help="Create approval only; skip inserts.")
    parser.add_argument(
        "--create-approval-only",
        action="store_true",
        help="Create approval payload and stop (no inserts).",
    )
    parser.add_argument("--kick-job", type=int, help="Claim and process a specific job id.")
    parser.add_argument(
        "--supervise-first-run",
        action="store_true",
        help="Create approval, then wait for decision and apply if approved.",
    )
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args(argv)


def kick_job(job_id: int) -> int:
    init_env()
    with get_connection() as conn:
        job = fetch_job(conn, job_id)
        if not job:
            LOGGER.error("kick job_id=%s not found", job_id)
            return 1
        normalized = _resolve_routine_code(job)
        LOGGER.info(
            "kick job_id=%s routine_code=%s routine_label=%s routine=%s normalized=%s",
            job.job_id,
            job.routine_code,
            job.routine_label,
            job.routine_value,
            normalized,
        )
        claimed = claim_job(conn, job.job_id)
        if not claimed:
            LOGGER.warning("kick job_id=%s not claimed (status not pending?)", job.job_id)
        if not job.file_blob:
            update_job_status(
                conn,
                job.job_id,
                "FAILED",
                error_text="Missing FILE_BLOB attachment.",
            )
            return 1
        if normalized == "RAG_INGEST":
            _handle_rag_ingest(conn, job, dry_run=False, create_only=True)
            update_job_status(
                conn,
                job.job_id,
                "IN_PROGRESS",
                result_text="Approval created; awaiting decision.",
            )
            return 0
        update_job_status(
            conn,
            job.job_id,
            "FAILED",
            error_text=f"Routine not supported: {normalized or 'UNKNOWN'}",
        )
        return 1


def supervise_first_run() -> int:
    init_env()
    routine_label = "Text to be transformed and added to RAG Vectors"
    with get_connection() as conn:
        job = pick_newest_pending_job(conn, "RAG_INGEST", routine_label)
        if not job:
            job = pick_job(conn, "RAG_INGEST", routine_label)
            if not job:
                LOGGER.info("No pending RAG_INGEST jobs found.")
                return 0
        approval = _ensure_approval(conn, job)
        update_job_status(
            conn,
            job.job_id,
            "IN_PROGRESS",
            result_text=f"Approval {approval.approval_id} created (dry-run).",
        )
        LOGGER.info("Dry-run complete: approval_id=%s job_id=%s", approval.approval_id, job.job_id)

    while True:
        with get_connection() as conn:
            approval = fetch_latest_approval(conn, job.job_id, "ADD_VECTORS")
            if not approval:
                LOGGER.info("Approval not found yet; waiting.")
                time.sleep(10)
                continue
            status = (approval.status or "").upper()
            if status == "APPROVED":
                LOGGER.info("Approval approved; applying inserts.")
                _handle_rag_ingest(conn, job, dry_run=False)
                return 0
            if status == "REJECTED":
                update_job_status(
                    conn,
                    job.job_id,
                    "DONE",
                    result_text=f"Rejected approval {approval.approval_id}.",
                )
                LOGGER.info("Approval rejected; job marked DONE.")
                return 0
        time.sleep(10)


def main(argv: Optional[list[str]] = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO))
    log_startup_config()
    _log_startup_diagnostics()

    try:
        if args.loop and args.once:
            LOGGER.error("Choose either --loop or --once.")
            return 2

        if args.kick_job:
            return kick_job(args.kick_job)

        if args.supervise_first_run:
            return supervise_first_run()

        if args.loop:
            run_loop(interval_seconds=args.interval, job_id=args.job_id, dry_run=args.dry_run)
            return 0

        if args.once:
            run_once(job_id=args.job_id, dry_run=args.dry_run, create_only=args.create_approval_only)
            return 0

        run_loop(interval_seconds=args.interval, job_id=args.job_id, dry_run=args.dry_run)
        return 0
    except Exception as exc:
        append_learned_note(LearnedNote("Worker crash", context=str(exc)[:200]))
        raise


if __name__ == "__main__":
    sys.exit(main())
