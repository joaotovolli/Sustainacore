from __future__ import annotations

import hashlib
import json
import os
import uuid
from typing import Any, Dict, Optional

from django.utils.timezone import now

from core.oracle_db import get_connection
from telemetry.consent import get_consent_from_request


EVENT_TYPES = {
    "page_view",
    "download_click",
    "download_blocked",
    "download_ok",
    "auth_request_code",
    "auth_verify_ok",
    "auth_verify_fail",
    "table_preview_rendered",
    "table_unlock_click",
    "table_unlock_success",
    "news_list_error",
    "news_detail_error",
}

ANON_COOKIE = "sc_anon"


def _hash_ip(ip: str) -> str:
    salt = os.getenv("ANALYTICS_IP_SALT", "dev-salt")
    payload = f"{salt}:{ip}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _client_ip(request) -> str:
    forwarded = request.META.get("HTTP_X_FORWARDED_FOR")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.META.get("REMOTE_ADDR", "") or ""


def ensure_anon_cookie(request, response) -> str:
    existing = request.COOKIES.get(ANON_COOKIE)
    if existing:
        return existing
    anon_id = uuid.uuid4().hex
    request.COOKIES[ANON_COOKIE] = anon_id
    response.set_cookie(
        ANON_COOKIE,
        anon_id,
        max_age=365 * 24 * 60 * 60,
        httponly=True,
        samesite="Lax",
        path="/",
    )
    return anon_id


def log_event(
    event_type: str,
    request,
    metadata: Optional[Dict[str, Any]] = None,
) -> None:
    if event_type not in EVENT_TYPES:
        return
    try:
        consent = get_consent_from_request(request)
        if not consent.analytics:
            return
    except Exception:
        return
    try:
        session_id = request.COOKIES.get(ANON_COOKIE, "")
        user_email = (request.session.get("auth_email") or "").strip() or None
        path = request.get_full_path()
        referrer = request.META.get("HTTP_REFERER", "")
        user_agent = request.META.get("HTTP_USER_AGENT", "")
        ip_hash = _hash_ip(_client_ip(request)) if _client_ip(request) else None
        metadata_json = json.dumps(metadata or {}, ensure_ascii=True)

        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO U_UX_EVENTS (
                        event_ts,
                        session_id,
                        user_email,
                        event_type,
                        path,
                        referrer,
                        user_agent,
                        ip_hash,
                        metadata_json
                    )
                    VALUES (:event_ts, :session_id, :user_email, :event_type, :path,
                            :referrer, :user_agent, :ip_hash, :metadata_json)
                    """,
                    {
                        "event_ts": now(),
                        "session_id": session_id,
                        "user_email": user_email,
                        "event_type": event_type,
                        "path": path[:512],
                        "referrer": (referrer or "")[:512],
                        "user_agent": (user_agent or "")[:512],
                        "ip_hash": ip_hash,
                        "metadata_json": metadata_json,
                    },
                )
                conn.commit()
    except Exception:
        # Analytics must never break the user flow.
        return
