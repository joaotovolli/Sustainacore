from __future__ import annotations

import logging
import time
from typing import Iterable

from django.db import connections, transaction

from core.analytics import ensure_anon_cookie
from telemetry.consent import get_consent_from_request
from telemetry.logger import _log_db_health_once, record_event, touch_session
from telemetry.utils import (
    ensure_session_key,
    get_geo_fields,
    is_bot_user_agent,
    resolve_user_id,
)


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
        self._logged_active = False

    def __call__(self, request):
        from django.conf import settings

        if not getattr(settings, "TELEMETRY_WRITE_ENABLED", True):
            return self.get_response(request)
        _log_db_health_once()
        if not self._logged_active:
            logging.getLogger(__name__).info("telemetry.middleware_active")
            self._logged_active = True
        start = time.monotonic()
        response = self.get_response(request)
        duration_ms = int((time.monotonic() - start) * 1000)

        path = getattr(request, "path", "") or ""
        if self._should_skip(path, request):
            return response

        consent = get_consent_from_request(request)
        user_agent = request.META.get("HTTP_USER_AGENT", "") or None
        is_bot = is_bot_user_agent(user_agent)
        event_type = self._resolve_event_type(request, response, path)
        if not event_type:
            return response
        anon_id = None
        session_key = None
        user_id = None
        country_code = None
        region_code = None
        if consent.analytics and not is_bot:
            anon_id = ensure_anon_cookie(request, response)
            session_key = ensure_session_key(request)
            user_id = resolve_user_id(request, consent_analytics=True, anon_id=anon_id)
            country_code, region_code = get_geo_fields(request)

        def _write_events():
            try:
                touch_session(
                    request=request,
                    session_key=session_key,
                    user_id=user_id,
                    country_code=country_code,
                    region_code=region_code,
                )
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
                    user_id=user_id,
                    country_code=country_code,
                    region_code=region_code,
                    payload={"bot": True} if is_bot else None,
                )
            except Exception:
                pass

        try:
            from django.conf import settings

            alias = getattr(settings, "TELEMETRY_DB_ALIAS", "default")
            conn = connections[alias]
            if conn.in_atomic_block:
                transaction.on_commit(_write_events)
            else:
                _write_events()
        except Exception:
            _write_events()

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
