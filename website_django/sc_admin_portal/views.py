from __future__ import annotations

import logging

from django.contrib import messages
from django.http import HttpResponse
from django.shortcuts import redirect, render
from django.views.decorators.cache import never_cache

from sc_admin_portal.auth import get_admin_email, portal_not_found, require_sc_admin
from sc_admin_portal import oracle_proc

logger = logging.getLogger(__name__)

ROUTINE_CHOICES = [
    ("NEWS_PUBLISH", "Text to publish news on Sustainacore.org"),
    ("RAG_INGEST", "Text to be transformed and added to RAG Vectors"),
    ("INDEX_REBALANCE", "Data to be added to Index Rebalance"),
]


def _routine_label(code: str) -> str | None:
    for key, label in ROUTINE_CHOICES:
        if key == code:
            return label
    return None


@never_cache
@require_sc_admin
def dashboard(request):
    error = ""
    success = ""
    warning = ""
    diagnostics: list[str] = []
    job_id = None
    if request.method == "POST" and request.POST.get("action") == "submit_job":
        routine_code = (request.POST.get("routine_code") or "").strip()
        routine_label = _routine_label(routine_code)
        content_text = (request.POST.get("content_text") or "").strip() or None
        instructions = (request.POST.get("instructions") or "").strip()
        upload = request.FILES.get("file")
        file_name = upload.name if upload else None
        file_mime = upload.content_type if upload else None
        file_blob = upload.read() if upload else None
        if not routine_label:
            error = "Please select a routine."
        elif not instructions:
            error = "Instructions are required."
        else:
            try:
                job_id = oracle_proc.insert_job(
                    routine_code=routine_code,
                    routine_label=routine_label,
                    content_text=content_text,
                    instructions=instructions,
                    file_name=file_name,
                    file_mime=file_mime,
                    file_blob=file_blob,
                )
                success = "Job submitted."
            except Exception:
                logger.exception("Admin portal job submit failed for routine=%s", routine_code)
                error = "Could not submit the job. Please try again."
    try:
        recent_jobs = oracle_proc.list_recent_jobs(limit=10)
    except Exception as exc:
        if job_id:
            logger.exception("Admin portal refresh failed after job submit job_id=%s", job_id)
            warning = "Job created, but jobs refresh failed. Please reload."
            diagnostics.append(f"jobs_refresh_failed: {exc}")
        else:
            logger.exception("Admin portal failed to load recent jobs")
            warning = "Jobs failed to load. Please reload."
            diagnostics.append(f"jobs_load_failed: {exc}")
        recent_jobs = []
    try:
        pending_approvals = oracle_proc.list_pending_approvals(limit=50)
    except Exception as exc:
        logger.exception("Admin portal failed to load pending approvals")
        if not warning:
            warning = "Approvals failed to load. Please reload."
        diagnostics.append(f"approvals_load_failed: {exc}")
        pending_approvals = []
    try:
        recent_decisions = oracle_proc.list_recent_decisions(limit=50)
    except Exception as exc:
        logger.exception("Admin portal failed to load approval history")
        if not warning:
            warning = "Approval history failed to load. Please reload."
        diagnostics.append(f"decisions_load_failed: {exc}")
        recent_decisions = []
    selected_approval = None
    approval_id = request.GET.get("approval_id")
    if approval_id:
        try:
            selected_approval = oracle_proc.get_approval(int(approval_id))
        except ValueError:
            selected_approval = None
        if approval_id and not selected_approval:
            return portal_not_found()
    return render(
        request,
        "sc_admin_portal/admin_portal.html",
        {
            "error": error,
            "success": success,
            "warning": warning,
            "diagnostics": diagnostics,
            "routine_choices": ROUTINE_CHOICES,
            "recent_jobs": recent_jobs,
            "pending_approvals": pending_approvals,
            "recent_decisions": recent_decisions,
            "selected_approval": selected_approval,
        },
    )


@never_cache
@require_sc_admin
def approve_approval(request, approval_id: int):
    if request.method != "POST":
        return portal_not_found()
    decision_notes = (request.POST.get("decision_notes") or "").strip() or None
    decided_by = (request.user.email or get_admin_email())
    try:
        updated = oracle_proc.decide_approval(
            approval_id=approval_id,
            status="APPROVED",
            decided_by=decided_by,
            decision_notes=decision_notes,
        )
    except Exception:
        schema = oracle_proc.get_current_schema() or "unknown"
        logger.exception(
            "Admin portal decision failed action=approve approval_id=%s schema=%s",
            approval_id,
            schema,
        )
        messages.error(request, "Approval failed (code: DECIDE_APPROVE_ORACLE). Check logs.")
        return redirect("sc_admin_portal:dashboard")
    if updated == 0:
        messages.warning(request, "Approval already decided or not found.")
    else:
        messages.success(request, "Approval recorded.")
    return redirect("sc_admin_portal:dashboard")


@never_cache
@require_sc_admin
def reject_approval(request, approval_id: int):
    if request.method != "POST":
        return portal_not_found()
    decision_notes = (request.POST.get("decision_notes") or "").strip() or None
    decided_by = (request.user.email or get_admin_email())
    try:
        updated = oracle_proc.decide_approval(
            approval_id=approval_id,
            status="REJECTED",
            decided_by=decided_by,
            decision_notes=decision_notes,
        )
    except Exception:
        schema = oracle_proc.get_current_schema() or "unknown"
        logger.exception(
            "Admin portal decision failed action=reject approval_id=%s schema=%s",
            approval_id,
            schema,
        )
        messages.error(request, "Rejection failed (code: DECIDE_REJECT_ORACLE). Check logs.")
        return redirect("sc_admin_portal:dashboard")
    if updated == 0:
        messages.warning(request, "Approval already decided or not found.")
    else:
        messages.success(request, "Rejection recorded.")
    return redirect("sc_admin_portal:dashboard")


@never_cache
@require_sc_admin
def job_file(request, job_id: int):
    payload = oracle_proc.get_job_file(job_id)
    if not payload:
        return portal_not_found()
    response = HttpResponse(payload["file_blob"], content_type=payload["file_mime"] or "application/octet-stream")
    filename = payload["file_name"] or f"job-{job_id}"
    response["Content-Disposition"] = f'attachment; filename="{filename}"'
    return response


@never_cache
@require_sc_admin
def approval_file(request, approval_id: int):
    payload = oracle_proc.get_approval_file(approval_id)
    if not payload:
        return portal_not_found()
    response = HttpResponse(payload["file_blob"], content_type=payload["file_mime"] or "application/octet-stream")
    filename = payload["file_name"] or f"approval-{approval_id}"
    response["Content-Disposition"] = f'attachment; filename="{filename}"'
    return response
