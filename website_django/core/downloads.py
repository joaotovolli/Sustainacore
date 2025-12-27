from __future__ import annotations

from functools import wraps
from urllib.parse import urlencode

from django.http import JsonResponse
from django.shortcuts import redirect

from core.analytics import log_event
from core.auth import is_logged_in


def _is_ajax(request) -> bool:
    header = request.headers.get("x-requested-with", "")
    if header.lower() == "xmlhttprequest":
        return True
    accept = request.headers.get("accept", "")
    return "application/json" in accept


def require_login_for_download(view):
    @wraps(view)
    def _wrapped(request, *args, **kwargs):
        if not is_logged_in(request):
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

        log_event("download_click", request, {"download": request.get_full_path()})
        response = view(request, *args, **kwargs)
        if getattr(response, "status_code", 500) < 400:
            log_event("download_ok", request, {"download": request.get_full_path()})
        return response

    return _wrapped
