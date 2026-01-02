from __future__ import annotations

import json
import logging
import os
from datetime import datetime, time

from django.contrib import messages
from django.http import HttpResponse
from django.shortcuts import redirect, render
from django.views.decorators.cache import never_cache

from core.auth import get_auth_email
from sc_admin_portal.auth import get_admin_email, portal_not_found, require_sc_admin
from sc_admin_portal import oracle_proc

logger = logging.getLogger(__name__)
_RUNNING_COMMIT: str | None = None

ROUTINE_CHOICES = [
    ("NEWS_PUBLISH", "Text to publish news on Sustainacore.org"),
    ("RAG_INGEST", "Text to be transformed and added to RAG Vectors"),
    ("INDEX_REBALANCE", "Data to be added to Index Rebalance"),
]

RESEARCH_REQUEST_CHOICES = [
    ("REBALANCE", "REBALANCE"),
    ("WEEKLY", "WEEKLY"),
    ("PERIOD_CLOSE", "PERIOD_CLOSE"),
    ("ANOMALY", "ANOMALY"),
    ("COMPANY_SPOTLIGHT", "COMPANY_SPOTLIGHT"),
]


def _routine_label(code: str) -> str | None:
    for key, label in ROUTINE_CHOICES:
        if key == code:
            return label
    return None


def _routine_code_from_request_type(request_type: str | None) -> str:
    mapping = {
        "PUBLISH_NEWS": "NEWS_PUBLISH",
        "ADD_VECTORS": "RAG_INGEST",
        "APPLY_REBALANCE": "INDEX_REBALANCE",
    }
    if not request_type:
        return "OTHER"
    return mapping.get(request_type.strip().upper(), "OTHER")


def _resolve_decided_by(request) -> str:
    decided_by = (getattr(request.user, "email", "") or "").strip()
    if not decided_by:
        decided_by = (get_auth_email(request) or "").strip()
    if not decided_by:
        decided_by = get_admin_email()
    logger.info("Admin portal decided_by resolved=%s", decided_by)
    return decided_by


def _parse_date(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.time() == time.min:
        return parsed
    return parsed


def _load_running_commit() -> str:
    global _RUNNING_COMMIT
    if _RUNNING_COMMIT:
        return _RUNNING_COMMIT
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    git_dir = os.path.join(os.path.dirname(repo_root), ".git")
    head_path = os.path.join(git_dir, "HEAD")
    commit = "unknown"
    try:
        with open(head_path, "r", encoding="utf-8") as handle:
            head = handle.read().strip()
        if head.startswith("ref:"):
            ref_path = head.split(" ", 1)[1].strip()
            ref_file = os.path.join(git_dir, ref_path)
            with open(ref_file, "r", encoding="utf-8") as handle:
                commit = handle.read().strip()
        else:
            commit = head
    except OSError:
        commit = "unknown"
    _RUNNING_COMMIT = commit
    return commit


def _research_request_type(details: str | None) -> str:
    if not details:
        return "REBALANCE"
    raw = details.strip()
    if not raw.startswith("{"):
        return "REBALANCE"
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return "REBALANCE"
    report_type = str(payload.get("report_type") or payload.get("request_type") or "").strip()
    if not report_type:
        return "REBALANCE"
    return report_type.upper()


@never_cache
@require_sc_admin
def dashboard(request):
    error = ""
    success = ""
    warning = ""
    diagnostics: list[str] = []
    job_id = None
    created_request_id = request.GET.get("created_request_id")
    if created_request_id:
        success = f"Research request created: {created_request_id}."
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
    if request.method == "POST" and request.POST.get("action") == "create_research_request":
        request_type = (request.POST.get("request_type") or "").strip().upper()
        company_ticker = (request.POST.get("company_ticker") or "").strip() or None
        window_start = _parse_date(request.POST.get("window_start"))
        window_end = _parse_date(request.POST.get("window_end"))
        editor_notes = (request.POST.get("editor_notes") or "").strip() or None
        if request_type not in {choice[0] for choice in RESEARCH_REQUEST_CHOICES}:
            error = "Please select a valid research request type."
        else:
            try:
                created_by = _resolve_decided_by(request)
                request_id = oracle_proc.create_research_request(
                    request_type=request_type,
                    company_ticker=company_ticker,
                    window_start=window_start,
                    window_end=window_end,
                    editor_notes=editor_notes,
                    source_approval_id=None,
                    created_by=created_by,
                )
                return redirect(f"{request.path}?created_request_id={request_id}")
            except Exception:
                logger.exception("Admin portal research request create failed type=%s", request_type)
                error = "Could not create research request. Please try again."
    show_all_jobs = request.GET.get("show_all_jobs") == "1"
    try:
        recent_jobs = oracle_proc.list_recent_jobs(limit=10, include_handed_off=show_all_jobs)
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
        pending_count = len(pending_approvals)
    except Exception as exc:
        logger.exception("Admin portal failed to load pending approvals")
        if not warning:
            warning = "Approvals failed to load. Please reload."
        diagnostics.append(f"approvals_load_failed: {exc}")
        pending_approvals = []
        pending_count = None
    try:
        recent_decisions = oracle_proc.list_recent_decisions(limit=50)
    except Exception as exc:
        logger.exception("Admin portal failed to load approval history")
        if not warning:
            warning = "Approval history failed to load. Please reload."
        diagnostics.append(f"decisions_load_failed: {exc}")
        recent_decisions = []
    try:
        recent_research_requests = oracle_proc.list_recent_research_requests(limit=10)
    except Exception as exc:
        logger.exception("Admin portal failed to load research requests")
        if not warning:
            warning = "Research requests failed to load. Please reload."
        diagnostics.append(f"research_requests_load_failed: {exc}")
        recent_research_requests = []
    default_tab = "approvals" if pending_count and pending_count > 0 else "create-research"
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
            "research_request_choices": RESEARCH_REQUEST_CHOICES,
            "recent_jobs": recent_jobs,
            "show_all_jobs": show_all_jobs,
            "pending_approvals": pending_approvals,
            "pending_count": pending_count,
            "recent_decisions": recent_decisions,
            "recent_research_requests": recent_research_requests,
            "default_tab": default_tab,
            "running_commit": _load_running_commit(),
            "selected_approval": selected_approval,
        },
    )


@never_cache
@require_sc_admin
def approve_approval(request, approval_id: int):
    if request.method != "POST":
        return portal_not_found()
    decision_notes = (request.POST.get("decision_notes") or "").strip() or None
    decided_by = _resolve_decided_by(request)
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
    decided_by = _resolve_decided_by(request)
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
def resubmit_approval(request, approval_id: int):
    if request.method != "POST":
        messages.warning(request, "Resubmit requires a POST. No changes were made.")
        return redirect("sc_admin_portal:dashboard")
    approval = oracle_proc.get_approval(approval_id)
    if not approval:
        return portal_not_found()
    request_type = (approval.get("request_type") or "").strip().upper()
    if request_type.startswith("RESEARCH"):
        editor_notes = (request.POST.get("new_instructions") or "").strip() or None
        logger.info(
            "Admin portal resubmit action=resubmit approval_id=%s request_type=%s method=%s",
            approval_id,
            request_type,
            request.method,
        )
        try:
            decided_by = _resolve_decided_by(request)
            report_type = _research_request_type(approval.get("details"))
            request_id = oracle_proc.create_research_request(
                request_type=report_type,
                company_ticker=None,
                window_start=None,
                window_end=None,
                editor_notes=editor_notes,
                source_approval_id=approval_id,
                created_by=decided_by,
            )
            note = f"Superseded by request_id={request_id}"
            updated = oracle_proc.decide_approval(
                approval_id=approval_id,
                status="REJECTED",
                decided_by=decided_by,
                decision_notes=note,
            )
            logger.info(
                "Admin portal resubmit action=created approval_id=%s request_type=%s request_id=%s",
                approval_id,
                request_type,
                request_id,
            )
            messages.success(
                request,
                f"Resubmitted: created research request {request_id}. A new draft will appear shortly.",
            )
            if updated == 0:
                messages.warning(request, "Approval already decided or not found.")
        except Exception:
            schema = oracle_proc.get_current_schema() or "unknown"
            logger.exception(
                "Admin portal resubmit failed action=resubmit approval_id=%s request_type=%s schema=%s",
                approval_id,
                request_type,
                schema,
            )
            messages.error(request, "Resubmit failed (code: RESUBMIT_RESEARCH). Check logs.")
        return redirect("sc_admin_portal:dashboard")
    job = None
    if approval.get("source_job_id"):
        job = oracle_proc.get_job(int(approval["source_job_id"]))
    new_instructions = (request.POST.get("new_instructions") or "").strip()
    if not new_instructions:
        new_instructions = (job or {}).get("instructions") or approval.get("details") or approval.get("proposed_text") or ""
    if not new_instructions:
        messages.error(request, "Instructions are required to resubmit.")
        return redirect("sc_admin_portal:dashboard")
    routine_code = (job or {}).get("routine_code") or _routine_code_from_request_type(approval.get("request_type"))
    routine_label = (job or {}).get("routine_label") or (approval.get("title") or "Gemini approval")
    content_text = (job or {}).get("content_text") or approval.get("proposed_text")
    file_name = (job or {}).get("file_name") or approval.get("file_name")
    file_mime = (job or {}).get("file_mime") or approval.get("file_mime")
    file_blob = (job or {}).get("file_blob") or approval.get("file_blob")
    try:
        new_job_id = oracle_proc.insert_job(
            routine_code=routine_code or "OTHER",
            routine_label=routine_label,
            content_text=content_text,
            instructions=new_instructions,
            file_name=file_name,
            file_mime=file_mime,
            file_blob=file_blob,
        )
        note = f"Superseded by job_id={new_job_id} (from approval_id={approval_id})"
        decided_by = _resolve_decided_by(request)
        updated = oracle_proc.decide_approval(
            approval_id=approval_id,
            status="REJECTED",
            decided_by=decided_by,
            decision_notes=note,
        )
        if job and job.get("job_id"):
            oracle_proc.update_job_superseded(job["job_id"], new_job_id)
        if updated == 0:
            messages.warning(request, "Approval already decided or not found.")
        else:
            messages.success(request, f"Resubmitted as job {new_job_id}.")
    except Exception:
        schema = oracle_proc.get_current_schema() or "unknown"
        logger.exception(
            "Admin portal resubmit failed approval_id=%s schema=%s",
            approval_id,
            schema,
        )
        messages.error(request, "Resubmit failed (code: RESUBMIT_APPROVAL). Check logs.")
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
