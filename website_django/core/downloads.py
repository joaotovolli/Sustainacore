from __future__ import annotations

from functools import wraps
from urllib.parse import urlencode

from django.http import JsonResponse
from django.shortcuts import redirect

from core.analytics import log_event
from core.auth import is_logged_in
from telemetry.consent import get_consent_from_request
from telemetry.logger import record_event
from telemetry.utils import ensure_session_key


def _is_ajax(request) -> bool:
    header = request.headers.get("x-requested-with", "")
    if header.lower() == "xmlhttprequest":
        return True
    accept = request.headers.get("accept", "")
    return "application/json" in accept


def require_login_for_download(view):
    @wraps(view)
    def _wrapped(request, *args, **kwargs):
        request._telemetry_skip = True
        if not is_logged_in(request):
            _record_download_event(
                request,
                success=False,
                gated=True,
            )
            consent = get_consent_from_request(request)
            if consent.analytics:
                log_event(
                    "download_blocked",
                    request,
                    {"download": request.path, "reason": "login_required"},
                )
            if _is_ajax(request):
                return JsonResponse({"detail": "Login required.", "login_required": True}, status=401)
            next_url = request.META.get("HTTP_REFERER") or "/"
            query = urlencode(
                {"download_login": "1", "download_url": request.get_full_path()}
            )
            separator = "&" if "?" in next_url else "?"
            return redirect(f"{next_url}{separator}{query}")

        consent = get_consent_from_request(request)
        if consent.analytics:
            log_event("download_click", request, {"download": request.get_full_path()})
        response = view(request, *args, **kwargs)
        if getattr(response, "status_code", 500) < 400:
            if consent.analytics:
                log_event("download_ok", request, {"download": request.get_full_path()})
            _record_download_event(
                request,
                success=True,
                gated=False,
                status_code=getattr(response, "status_code", None),
            )
        else:
            _record_download_event(
                request,
                success=False,
                gated=False,
                status_code=getattr(response, "status_code", None),
            )
        return response

    return _wrapped


def _record_download_event(
    request,
    *,
    success: bool,
    gated: bool,
    status_code: int | None = None,
) -> None:
    consent = get_consent_from_request(request)
    event_name = "blocked_login" if gated else ("ok" if success else "error")
    session_key = ensure_session_key(request)
    try:
        record_event(
            event_type="download",
            request=request,
            consent=consent,
            path=request.path,
            http_method=request.method,
            status_code=status_code,
            event_name=event_name,
            session_key=session_key,
        )
    except Exception:
        return
