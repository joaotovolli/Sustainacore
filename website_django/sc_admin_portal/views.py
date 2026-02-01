from __future__ import annotations

import json
import logging
import os
import re
import tempfile
from datetime import datetime, time

from django.contrib import messages
from django.http import HttpResponse, JsonResponse
from django.shortcuts import redirect, render
from django.views.decorators.cache import never_cache
from django.views.decorators.http import require_POST

from core.auth import get_auth_email
from sc_admin_portal.auth import get_admin_email, portal_not_found, require_sc_admin
from sc_admin_portal import oracle_proc
from sc_admin_portal.docx_import import build_news_body_from_docx
from sc_admin_portal.news_storage import (
    NewsStorageError,
    create_news_asset,
    create_news_post,
    delete_news_item,
    get_news_item_preview,
    update_news_item,
)

logger = logging.getLogger(__name__)
_RUNNING_COMMIT: str | None = None

ROUTINE_CHOICES = [
    ("NEWS_PUBLISH", "Text to publish news on Sustainacore.org"),
    ("RAG_INGEST", "Text to be transformed and added to RAG Vectors"),
    ("INDEX_REBALANCE", "Data to be added to Index Rebalance"),
]

_NEWS_UPLOAD_MAX_BYTES = 6 * 1024 * 1024
_RESEARCH_SETTINGS_DEFAULTS = {
    "SCHEDULE_ENABLED": "Y",
    "DEV_NOOP": "N",
    "SAVER_PROFILE": "MEDIUM",
    "MAX_CONTEXT_PCT": "50",
}
_RESEARCH_SAVER_PROFILES = {"MEDIUM", "LOW", "MINIMAL"}
_RESEARCH_CONTEXT_MIN = 5
_RESEARCH_CONTEXT_MAX = 95

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


def _parse_news_id(raw_value: str | None) -> int | None:
    if not raw_value:
        return None
    raw = raw_value.strip()
    match = re.search(r"(\d+)$", raw)
    if not match:
        return None
    try:
        parsed = int(match.group(1))
    except ValueError:
        return None
    return parsed if parsed > 0 else None


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
    publish_form = {"headline": "", "tags": "", "body_html": ""}
    news_error = ""
    news_success = ""
    news_item = None
    news_docx_used = False
    news_docx_name = ""
    news_docx_size = 0
    news_docx_images_found = 0
    news_docx_images_uploaded = 0
    news_preview_html = ""
    publish_tab_active = False
    manage_tab_active = False
    manage_news_error = ""
    manage_news_success = ""
    manage_news_item = None
    manage_news_id = ""
    manage_news_title = ""
    manage_news_body = ""
    manage_news_docx_name = ""
    manage_news_docx_size = 0
    manage_news_docx_images_found = 0
    manage_news_docx_images_uploaded = 0
    manage_news_preview_html = ""
    research_tab_active = False
    research_settings_error = ""
    research_settings_warning = ""
    research_settings_success = ""
    research_form = {}
    research_settings: dict[str, str] = {}
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
    if request.method == "POST" and request.POST.get("action") == "publish_news":
        publish_tab_active = True
        headline = (request.POST.get("headline") or "").strip()
        tags = (request.POST.get("tags") or "").strip()
        body_html = (request.POST.get("body_html") or "").strip()
        confirm_no_images = request.POST.get("confirm_no_images") == "on"
        publish_form = {"headline": headline, "tags": tags, "body_html": body_html}
        docx_upload = request.FILES.get("docx_file")
        if docx_upload:
            news_docx_used = True
            news_docx_name = docx_upload.name or ""
            news_docx_size = docx_upload.size or 0
        if docx_upload:
            if docx_upload.size and docx_upload.size > _NEWS_UPLOAD_MAX_BYTES:
                news_error = "DOCX exceeds upload limit."
            elif not (docx_upload.name or "").lower().endswith(".docx"):
                news_error = "Only .docx uploads are supported for news import."
            else:
                stats: dict[str, int] = {}
                temp_path = None
                try:
                    with tempfile.NamedTemporaryFile(suffix=".docx", delete=False) as tmp_file:
                        temp_path = tmp_file.name
                        for chunk in docx_upload.chunks():
                            tmp_file.write(chunk)

                    def upload_asset(file_name: str | None, mime_type: str | None, file_bytes: bytes) -> int:
                        return create_news_asset(
                            news_id=None,
                            file_name=file_name,
                            mime_type=mime_type,
                            file_bytes=file_bytes,
                        )

                    docx_headline, docx_body = build_news_body_from_docx(
                        temp_path,
                        asset_uploader=upload_asset,
                        stats=stats,
                    )
                except NewsStorageError as exc:
                    news_error = str(exc)
                    docx_body = ""
                    docx_headline = ""
                except Exception:
                    logger.exception("Admin portal DOCX import failed")
                    news_error = "Unable to import DOCX. Please try again."
                    docx_body = ""
                    docx_headline = ""
                finally:
                    if temp_path and os.path.exists(temp_path):
                        os.unlink(temp_path)

                images_found = stats.get("images_found", 0)
                images_uploaded = stats.get("images_uploaded", 0)
                news_docx_images_found = images_found
                news_docx_images_uploaded = images_uploaded
                logger.info(
                    "Admin news DOCX import images_found=%s images_uploaded=%s storage=news_assets file=%s",
                    images_found,
                    images_uploaded,
                    docx_upload.name,
                )
                if images_found and not images_uploaded:
                    news_error = "DOCX contains images but none could be extracted."
                elif docx_body:
                    body_html = docx_body
                    if docx_headline and not headline:
                        headline = docx_headline
                    publish_form = {"headline": headline, "tags": tags, "body_html": body_html}
                    news_preview_html = body_html
                else:
                    news_error = "No body content found in DOCX."
        else:
            logger.info(
                "Admin news publish without DOCX confirm_no_images=%s headline=%s",
                confirm_no_images,
                bool(headline),
            )

        if not news_error:
            if not docx_upload and not confirm_no_images:
                news_error = "Confirm that no images are included when publishing without a DOCX file."
            if not headline:
                news_error = "Headline is required."
            elif not body_html:
                news_error = "Body is required."

        if not news_error:
            try:
                news_item = create_news_post(headline=headline, tags=tags, body_html=body_html)
                messages.success(request, "News post published.")
                return redirect("news_detail", news_item["id"])
            except ValueError as exc:
                news_error = str(exc)
            except NewsStorageError as exc:
                news_error = str(exc)
            except Exception:
                logger.exception("Admin portal news publish failed")
                news_error = "Could not publish news. Please try again."
    if request.method == "POST" and request.POST.get("action") == "lookup_news_item":
        manage_tab_active = True
        manage_news_id = (request.POST.get("manage_news_id") or "").strip()
        parsed_id = _parse_news_id(manage_news_id)
        if not parsed_id:
            manage_news_error = "Enter a valid news id."
        else:
            try:
                manage_news_item = get_news_item_preview(news_id=parsed_id)
            except Exception:
                logger.exception("Admin portal news lookup failed id=%s", manage_news_id)
                manage_news_error = "Unable to load news item."
            if not manage_news_item and not manage_news_error:
                manage_news_error = "News item not found."

    if request.method == "POST" and request.POST.get("action") == "edit_news_item":
        manage_tab_active = True
        manage_news_id = (request.POST.get("manage_news_id") or "").strip()
        manage_news_title = (request.POST.get("manage_news_title") or "").strip()
        manage_news_body = (request.POST.get("manage_news_body") or "").strip()
        confirm_edit = request.POST.get("confirm_edit") == "on"
        parsed_id = _parse_news_id(manage_news_id)
        docx_upload = request.FILES.get("manage_docx_file")

        if docx_upload:
            manage_news_docx_name = docx_upload.name or ""
            manage_news_docx_size = docx_upload.size or 0

        if not parsed_id:
            manage_news_error = "Enter a valid news id."
        elif not confirm_edit:
            manage_news_error = "Confirm you want to overwrite the public news content."

        if not manage_news_error and docx_upload:
            if docx_upload.size and docx_upload.size > _NEWS_UPLOAD_MAX_BYTES:
                manage_news_error = "DOCX exceeds upload limit."
            elif not (docx_upload.name or "").lower().endswith(".docx"):
                manage_news_error = "Only .docx uploads are supported."
            else:
                stats: dict[str, int] = {}
                temp_path = None
                try:
                    with tempfile.NamedTemporaryFile(suffix=".docx", delete=False) as tmp_file:
                        temp_path = tmp_file.name
                        for chunk in docx_upload.chunks():
                            tmp_file.write(chunk)

                    def upload_asset(file_name: str | None, mime_type: str | None, file_bytes: bytes) -> int:
                        return create_news_asset(
                            news_id=parsed_id,
                            file_name=file_name,
                            mime_type=mime_type,
                            file_bytes=file_bytes,
                        )

                    docx_headline, docx_body = build_news_body_from_docx(
                        temp_path,
                        asset_uploader=upload_asset,
                        stats=stats,
                    )
                except NewsStorageError as exc:
                    manage_news_error = str(exc)
                    docx_body = ""
                    docx_headline = ""
                except Exception:
                    logger.exception("Admin portal DOCX edit failed")
                    manage_news_error = "Unable to import DOCX."
                    docx_body = ""
                    docx_headline = ""
                finally:
                    if temp_path and os.path.exists(temp_path):
                        os.unlink(temp_path)

                images_found = stats.get("images_found", 0)
                images_uploaded = stats.get("images_uploaded", 0)
                manage_news_docx_images_found = images_found
                manage_news_docx_images_uploaded = images_uploaded
                logger.info(
                    "Admin news edit DOCX images_found=%s images_uploaded=%s file=%s",
                    images_found,
                    images_uploaded,
                    docx_upload.name,
                )
                if images_found and not images_uploaded:
                    manage_news_error = "DOCX contains images but none could be extracted."
                elif docx_body:
                    manage_news_body = docx_body
                    manage_news_preview_html = docx_body
                    if docx_headline and not manage_news_title:
                        manage_news_title = docx_headline
                else:
                    manage_news_error = "No body content found in DOCX."

        if not manage_news_error:
            headline = manage_news_title or None
            body_html = manage_news_body or None
            if not headline and not body_html:
                manage_news_error = "Provide a headline, body text, or DOCX."
            else:
                try:
                    update_news_item(
                        news_id=parsed_id,
                        headline=headline,
                        body_html=body_html,
                    )
                    manage_news_success = f"Updated NEWS_ITEMS:{parsed_id}."
                    manage_news_item = get_news_item_preview(news_id=parsed_id)
                except (NewsStorageError, ValueError) as exc:
                    manage_news_error = str(exc)
                except Exception:
                    logger.exception("Admin portal news update failed id=%s", manage_news_id)
                    manage_news_error = "Unable to update news item."

    if request.method == "POST" and request.POST.get("action") == "delete_news_item":
        manage_tab_active = True
        manage_news_id = (request.POST.get("manage_news_id") or "").strip()
        confirm_news_id = (request.POST.get("confirm_news_id") or "").strip()
        confirm_delete = request.POST.get("confirm_delete") == "on"
        parsed_id = _parse_news_id(manage_news_id)
        parsed_confirm = _parse_news_id(confirm_news_id)
        if not parsed_id:
            manage_news_error = "Enter a valid news id."
        elif parsed_confirm != parsed_id:
            manage_news_error = "Confirmation id must match."
        elif not confirm_delete:
            manage_news_error = "Confirm you want to permanently delete the news item."
        else:
            try:
                manage_news_item = get_news_item_preview(news_id=parsed_id)
                if not manage_news_item:
                    manage_news_error = "News item not found."
                else:
                    result = delete_news_item(news_id=parsed_id)
                    manage_news_success = f"Deleted NEWS_ITEMS:{parsed_id}."
                    manage_news_item = None
                    logger.info(
                        "Admin portal news delete id=%s assets=%s",
                        parsed_id,
                        len(result.get("asset_ids", [])),
                    )
            except NewsStorageError as exc:
                manage_news_error = str(exc)
            except Exception:
                logger.exception("Admin portal news delete failed id=%s", manage_news_id)
                manage_news_error = "Unable to delete news item."
    if request.method == "POST" and request.POST.get("action") == "save_research_settings":
        research_tab_active = True
        schedule_enabled = request.POST.get("schedule_enabled") == "on"
        dev_noop = request.POST.get("dev_noop") == "on"
        saver_profile = (request.POST.get("saver_profile") or "").strip().upper()
        max_context_raw = (request.POST.get("max_context_pct") or "").strip()
        if not saver_profile:
            saver_profile = _RESEARCH_SETTINGS_DEFAULTS["SAVER_PROFILE"]
        if saver_profile not in _RESEARCH_SAVER_PROFILES:
            research_settings_warning = f"Unknown saver profile '{saver_profile}' saved."
        try:
            max_context_pct = int(max_context_raw) if max_context_raw else int(
                _RESEARCH_SETTINGS_DEFAULTS["MAX_CONTEXT_PCT"]
            )
        except ValueError:
            max_context_pct = int(_RESEARCH_SETTINGS_DEFAULTS["MAX_CONTEXT_PCT"])
        max_context_pct = max(_RESEARCH_CONTEXT_MIN, min(_RESEARCH_CONTEXT_MAX, max_context_pct))
        research_form = {
            "schedule_enabled": "Y" if schedule_enabled else "N",
            "dev_noop": "Y" if dev_noop else "N",
            "saver_profile": saver_profile,
            "max_context_pct": str(max_context_pct),
        }
        try:
            oracle_proc.set_research_settings(
                {
                    "SCHEDULE_ENABLED": research_form["schedule_enabled"],
                    "DEV_NOOP": research_form["dev_noop"],
                    "SAVER_PROFILE": research_form["saver_profile"],
                    "MAX_CONTEXT_PCT": research_form["max_context_pct"],
                },
                updated_by=_resolve_decided_by(request),
            )
            params = "tab=research-settings&settings_saved=1"
            if research_settings_warning:
                params += "&settings_warning=unknown_profile"
            return redirect(f"{request.path}?{params}")
        except Exception as exc:
            logger.exception("Admin portal research settings save failed")
            research_settings_error = "SETTINGS_SAVE_FAILED"
            diagnostics.append(f"research_settings_save_failed: {exc}")
    show_all_jobs = request.GET.get("show_all_jobs") == "1"
    settings_saved = request.GET.get("settings_saved") == "1"
    if settings_saved:
        research_settings_success = "Settings saved."
    if request.GET.get("settings_warning") == "unknown_profile":
        research_settings_warning = "Saver profile is not recognized, but it was saved."
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
        research_settings = oracle_proc.get_research_settings()
    except Exception as exc:
        logger.exception("Admin portal failed to load research settings")
        research_settings = {}
        research_settings_error = "SETTINGS_LOAD_FAILED"
        diagnostics.append(f"research_settings_load_failed: {exc}")
    try:
        pending_approvals = oracle_proc.list_pending_approvals(limit=50)
        pending_count = len(pending_approvals)
        pending_type_counts = {}
        for approval in pending_approvals:
            request_type = (approval.get("request_type") or "UNKNOWN").strip()
            pending_type_counts[request_type] = pending_type_counts.get(request_type, 0) + 1
        newest_pending_id = pending_approvals[0]["approval_id"] if pending_approvals else None
    except Exception as exc:
        logger.exception("Admin portal failed to load pending approvals")
        if not warning:
            warning = "Approvals failed to load. Please reload."
        diagnostics.append(f"approvals_load_failed: {exc}")
        pending_approvals = []
        pending_count = None
        pending_type_counts = {}
        newest_pending_id = None
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
        research_requests_error = ""
    except Exception as exc:
        logger.exception("Admin portal failed to load research requests")
        if not warning:
            warning = "Research requests failed to load. Please reload."
        diagnostics.append(f"research_requests_load_failed: {exc}")
        recent_research_requests = []
        research_requests_error = "Manual requests failed to load."
    default_tab = "approvals" if pending_count and pending_count > 0 else "create-research"
    if publish_tab_active:
        default_tab = "publish-news"
    if manage_tab_active:
        default_tab = "manage-news"
    if research_tab_active:
        default_tab = "research-settings"
    requested_tab = (request.GET.get("tab") or "").strip()
    if requested_tab:
        default_tab = requested_tab

    if not research_form:
        merged_settings = {**_RESEARCH_SETTINGS_DEFAULTS, **research_settings}
        research_form = {
            "schedule_enabled": merged_settings.get("SCHEDULE_ENABLED", "Y"),
            "dev_noop": merged_settings.get("DEV_NOOP", "N"),
            "saver_profile": (merged_settings.get("SAVER_PROFILE") or "").strip().upper(),
            "max_context_pct": merged_settings.get("MAX_CONTEXT_PCT", "50"),
        }
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
            "pending_type_counts": pending_type_counts,
            "newest_pending_id": newest_pending_id,
            "recent_decisions": recent_decisions,
            "recent_research_requests": recent_research_requests,
            "research_requests_error": research_requests_error,
            "default_tab": default_tab,
            "running_commit": _load_running_commit(),
            "now_utc": datetime.utcnow(),
            "selected_approval": selected_approval,
            "news_form": publish_form,
            "news_error": news_error,
            "news_success": news_success,
            "news_item": news_item,
            "news_upload_max_mb": _NEWS_UPLOAD_MAX_BYTES // (1024 * 1024),
            "news_docx_used": news_docx_used,
            "news_docx_name": news_docx_name,
            "news_docx_size": news_docx_size,
            "news_docx_images_found": news_docx_images_found,
            "news_docx_images_uploaded": news_docx_images_uploaded,
            "news_preview_html": news_preview_html,
            "manage_news_error": manage_news_error,
            "manage_news_success": manage_news_success,
            "manage_news_item": manage_news_item,
            "manage_news_id": manage_news_id,
            "manage_news_title": manage_news_title,
            "manage_news_body": manage_news_body,
            "manage_news_docx_name": manage_news_docx_name,
            "manage_news_docx_size": manage_news_docx_size,
            "manage_news_docx_images_found": manage_news_docx_images_found,
            "manage_news_docx_images_uploaded": manage_news_docx_images_uploaded,
            "manage_news_preview_html": manage_news_preview_html,
            "research_settings": research_settings,
            "research_settings_form": research_form,
            "research_settings_error": research_settings_error,
            "research_settings_warning": research_settings_warning,
            "research_settings_success": research_settings_success,
            "research_saver_profiles": sorted(_RESEARCH_SAVER_PROFILES),
            "research_context_min": _RESEARCH_CONTEXT_MIN,
            "research_context_max": _RESEARCH_CONTEXT_MAX,
        },
    )


@never_cache
@require_sc_admin
@require_POST
def news_asset_upload(request):
    upload = request.FILES.get("file")
    if not upload:
        return JsonResponse(
            {"error": "missing_file", "message": "No file uploaded."}, status=400
        )

    if upload.size and upload.size > _NEWS_UPLOAD_MAX_BYTES:
        return JsonResponse(
            {"error": "file_too_large", "message": "File exceeds upload limit."},
            status=400,
        )

    content_type = (upload.content_type or "").lower()
    if not content_type.startswith("image/"):
        return JsonResponse(
            {"error": "invalid_type", "message": "Only image uploads are allowed."},
            status=400,
        )

    news_id = request.POST.get("news_id")
    news_id_value = None
    if news_id:
        try:
            news_id_value = int(news_id)
        except ValueError:
            return JsonResponse(
                {"error": "invalid_news_id", "message": "Invalid news identifier."},
                status=400,
            )

    try:
        asset_id = create_news_asset(
            news_id=news_id_value,
            file_name=upload.name,
            mime_type=content_type,
            file_bytes=upload.read(),
        )
    except NewsStorageError as exc:
        return JsonResponse(
            {"error": exc.code or "storage_error", "message": str(exc)},
            status=503,
        )
    except Exception:
        logger.exception("Admin portal news asset upload failed")
        return JsonResponse(
            {"error": "upload_failed", "message": "Unable to upload image."},
            status=500,
        )

    asset_url = request.build_absolute_uri(f"/news/assets/{asset_id}/")
    return JsonResponse(
        {"location": asset_url, "asset_id": asset_id}
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
    approval_file = None
    if not (job or {}).get("file_blob"):
        approval_file = oracle_proc.get_approval_file(approval_id)
    routine_code = (job or {}).get("routine_code") or _routine_code_from_request_type(approval.get("request_type"))
    routine_label = (job or {}).get("routine_label") or (approval.get("title") or "Gemini approval")
    content_text = (job or {}).get("content_text") or approval.get("proposed_text")
    file_name = (job or {}).get("file_name") or (approval_file or {}).get("file_name") or approval.get("file_name")
    file_mime = (job or {}).get("file_mime") or (approval_file or {}).get("file_mime") or approval.get("file_mime")
    file_blob = (job or {}).get("file_blob") or (approval_file or {}).get("file_blob")
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


@never_cache
@require_sc_admin
def retry_research_request(request, request_id: int):
    if request.method != "POST":
        return portal_not_found()
    note = f"admin_retry_now {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC"
    try:
        outcome = oracle_proc.retry_research_request(request_id, note)
    except Exception:
        logger.exception("Admin portal retry-now failed request_id=%s", request_id)
        messages.error(request, "Retry now failed (code: RETRY_NOW). Check logs.")
        return redirect("sc_admin_portal:dashboard")
    if outcome == "updated":
        messages.success(request, f"Retry scheduled for request {request_id}.")
    elif outcome == "already_ready":
        messages.info(request, "Already eligible to run.")
    elif outcome == "not_pending":
        messages.warning(request, "Request is not pending.")
    elif outcome == "not_found":
        messages.warning(request, "Request not found.")
    else:
        messages.warning(request, "Retry not applied.")
    return redirect("sc_admin_portal:dashboard")
