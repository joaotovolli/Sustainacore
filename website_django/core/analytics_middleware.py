from __future__ import annotations

from django.utils.deprecation import MiddlewareMixin

from core.analytics import ensure_anon_cookie, log_event
from telemetry.consent import get_consent_from_request


class AnonCookieMiddleware(MiddlewareMixin):
    def process_response(self, request, response):
        consent = get_consent_from_request(request)
        if consent.analytics:
            ensure_anon_cookie(request, response)
        return response


class PageViewAnalyticsMiddleware(MiddlewareMixin):
    def process_response(self, request, response):
        if request.method != "GET":
            return response
        if request.path.startswith("/static/") or request.path.startswith("/admin/"):
            return response
        content_type = response.get("Content-Type", "")
        if "text/html" not in content_type:
            return response
        if getattr(response, "status_code", 200) >= 400:
            return response
        consent = get_consent_from_request(request)
        if consent.analytics:
            ensure_anon_cookie(request, response)
            log_event("page_view", request, {"path": request.path})
        return response
