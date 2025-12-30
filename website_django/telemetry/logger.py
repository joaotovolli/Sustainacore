from __future__ import annotations

import json
from typing import Any, Dict, Optional

from django.utils.timezone import now

from telemetry.consent import ConsentState
from telemetry.models import WebConsent, WebEvent, WebSession
from telemetry.utils import get_ip_fields


def _safe_json(payload: Optional[Dict[str, Any]]) -> str | None:
    if payload is None:
        return None
    try:
        return json.dumps(payload, ensure_ascii=True)
    except (TypeError, ValueError):
        return None


def record_consent(
    *,
    consent: ConsentState,
    request,
    user_id: Optional[int] = None,
) -> None:
    ip_trunc, ip_hash = get_ip_fields(request)
    user_agent = request.META.get("HTTP_USER_AGENT", "") or None
    WebConsent.objects.create(
        user_id=user_id,
        consent_analytics="Y" if consent.analytics else "N",
        consent_functional="Y" if consent.functional else "N",
        consent_policy_version=consent.policy_version,
        source=consent.source,
        user_agent=user_agent[:512] if user_agent else None,
        ip_trunc=ip_trunc,
        ip_hash=ip_hash,
    )


def record_event(
    *,
    event_type: str,
    request,
    consent: ConsentState,
    path: str,
    query_string: str | None = None,
    http_method: str | None = None,
    status_code: int | None = None,
    response_ms: int | None = None,
    payload: Optional[Dict[str, Any]] = None,
    user_id: Optional[int] = None,
    session_key: Optional[str] = None,
    country_code: Optional[str] = None,
    region_code: Optional[str] = None,
) -> None:
    ip_trunc, ip_hash = get_ip_fields(request)
    user_agent = request.META.get("HTTP_USER_AGENT", "") or None
    referrer = request.META.get("HTTP_REFERER", "") or None
    WebEvent.objects.create(
        event_ts=now(),
        user_id=user_id,
        session_key=session_key,
        consent_analytics_effective="Y" if consent.analytics else "N",
        event_type=event_type,
        path=path[:512],
        query_string=query_string,
        http_method=http_method,
        status_code=status_code,
        response_ms=response_ms,
        referrer=referrer[:512] if referrer else None,
        user_agent=user_agent[:512] if user_agent else None,
        ip_trunc=ip_trunc,
        ip_hash=ip_hash,
        country_code=country_code,
        region_code=region_code,
        payload_json=_safe_json(payload),
    )


def touch_session(
    *,
    request,
    session_key: Optional[str],
    user_id: Optional[int] = None,
    country_code: Optional[str] = None,
    region_code: Optional[str] = None,
) -> None:
    if not session_key:
        return
    ip_trunc, ip_hash = get_ip_fields(request)
    user_agent = request.META.get("HTTP_USER_AGENT", "") or None
    timestamp = now()
    existing = WebSession.objects.filter(session_key=session_key).first()
    if existing:
        existing.last_seen_ts = timestamp
        if user_id:
            existing.user_id = user_id
        existing.ip_hash = ip_hash or existing.ip_hash
        existing.user_agent = user_agent[:512] if user_agent else existing.user_agent
        if country_code:
            existing.country_code = country_code
        if region_code:
            existing.region_code = region_code
        existing.save(update_fields=["last_seen_ts", "user_id", "ip_hash", "user_agent", "country_code", "region_code"])
        return
    WebSession.objects.create(
        session_key=session_key,
        user_id=user_id,
        first_seen_ts=timestamp,
        last_seen_ts=timestamp,
        country_code=country_code,
        region_code=region_code,
        user_agent=user_agent[:512] if user_agent else None,
        ip_hash=ip_hash,
    )
