from __future__ import annotations

import time
from typing import Iterable

from telemetry.consent import get_consent_from_request
from telemetry.logger import record_event, touch_session


EXCLUDED_PREFIXES: Iterable[str] = (
    "/static/",
    "/media/",
    "/admin/",
    "/telemetry/",
    "/favicon.ico",
)

API_PREFIXES: Iterable[str] = (
    "/api/",
    "/ask2/api/",
)


class TelemetryMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        start = time.monotonic()
        response = self.get_response(request)
        duration_ms = int((time.monotonic() - start) * 1000)

        path = getattr(request, "path", "") or ""
        if self._should_skip(path, request):
            return response

        consent = get_consent_from_request(request)
        event_type = self._resolve_event_type(request, response, path)
        if not event_type:
            return response

        try:
            session_key = getattr(request.session, "session_key", None)
        except Exception:
            session_key = None

        try:
            touch_session(request=request, session_key=session_key)
        except Exception:
            pass

        try:
            record_event(
                event_type=event_type,
                request=request,
                consent=consent,
                path=path,
                query_string=request.META.get("QUERY_STRING") or None,
                http_method=request.method,
                status_code=getattr(response, "status_code", None),
                response_ms=duration_ms,
                session_key=session_key,
            )
        except Exception:
            pass

        return response

    @staticmethod
    def _should_skip(path: str, request) -> bool:
        if any(path.startswith(prefix) for prefix in EXCLUDED_PREFIXES):
            return True
        if path == "/api/ux-event/":
            return True
        if getattr(request, "_telemetry_skip", False):
            return True
        return False

    @staticmethod
    def _resolve_event_type(request, response, path: str) -> str | None:
        status_code = getattr(response, "status_code", 200)
        content_type = (response.get("Content-Type", "") or "").lower()

        if any(path.startswith(prefix) for prefix in API_PREFIXES):
            return "api_call"

        if status_code >= 500:
            return "error"

        if request.method == "GET" and "text/html" in content_type and status_code < 400:
            return "page_view"

        return None
