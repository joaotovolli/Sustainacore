from __future__ import annotations

import json
from urllib.parse import urlparse, quote

from django.conf import settings
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.views.decorators.http import require_POST
from django.views.decorators.csrf import csrf_exempt
from django.utils.timezone import now

from core.analytics import ensure_anon_cookie
from telemetry.consent import CONSENT_COOKIE, ConsentState, get_consent_from_request, serialize_consent
from telemetry.logger import record_consent, record_event
from telemetry.models import WebEvent
from telemetry.utils import (
    ensure_session_key,
    get_geo_fields,
    is_bot_user_agent,
    resolve_user_id,
)


ALLOWED_UI_EVENTS = {
    "filter_applied",
    "search_submitted",
    "download_click",
    "ask2_opened",
    "tab_changed",
}


def _load_json(request: HttpRequest) -> dict:
    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except (json.JSONDecodeError, UnicodeDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _event_path(payload: dict, fallback: str) -> str:
    referrer = payload.get("page") or payload.get("referrer") or ""
    if isinstance(referrer, str) and referrer:
        parsed = urlparse(referrer)
        return parsed.path or fallback
    return fallback


def _is_same_origin(request: HttpRequest) -> bool:
    origin = request.META.get("HTTP_ORIGIN") or request.META.get("HTTP_REFERER")
    if not origin:
        return True
    parsed = urlparse(origin)
    return parsed.netloc == request.get_host()


@csrf_exempt
@require_POST
def consent(request: HttpRequest) -> JsonResponse:
    if not _is_same_origin(request):
        return JsonResponse({"ok": False}, status=403)
    payload = _load_json(request)
    analytics = bool(payload.get("analytics"))
    functional = bool(payload.get("functional"))
    source = str(payload.get("source") or "banner")

    consent_state = ConsentState(
        analytics=analytics,
        functional=functional,
        policy_version=settings.TELEMETRY_POLICY_VERSION,
        source=source,
    )
    cookie_value = quote(serialize_consent(consent_state))
    response = JsonResponse({"ok": True})
    user_agent = request.META.get("HTTP_USER_AGENT", "") or None
    is_bot = is_bot_user_agent(user_agent)
    anon_id = None
    user_id = None
    if consent_state.analytics and not is_bot:
        anon_id = ensure_anon_cookie(request, response)
        user_id = resolve_user_id(request, consent_analytics=True, anon_id=anon_id)
    response.set_cookie(
        CONSENT_COOKIE,
        cookie_value,
        max_age=settings.TELEMETRY_CONSENT_MAX_AGE_DAYS * 24 * 60 * 60,
        httponly=False,
        samesite="Lax",
        secure=not settings.DEBUG,
        path="/",
    )
    try:
        record_consent(consent=consent_state, request=request, user_id=user_id)
    except Exception:
        pass
    return response


@csrf_exempt
@require_POST
def telemetry_event(request: HttpRequest) -> HttpResponse:
    if not _is_same_origin(request):
        return HttpResponse(status=403)
    consent = get_consent_from_request(request)
    if not consent.analytics:
        return HttpResponse(status=204)

    payload = _load_json(request)
    event_name = payload.get("event_name") or payload.get("event") or payload.get("name")
    if not isinstance(event_name, str):
        return HttpResponse(status=204)
    event_name = event_name.strip()
    if not event_name or event_name not in ALLOWED_UI_EVENTS:
        return HttpResponse(status=204)

    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
    response = HttpResponse(status=204)
    user_agent = request.META.get("HTTP_USER_AGENT", "") or None
    is_bot = is_bot_user_agent(user_agent)
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
    try:
        record_event(
            event_type="ui_event",
            request=request,
            consent=consent,
            path=_event_path(metadata, request.path),
            query_string=None,
            http_method=request.method,
            status_code=204,
            session_key=session_key,
            user_id=user_id,
            country_code=country_code,
            region_code=region_code,
            payload={
                "event_name": event_name,
                "metadata": metadata,
                **({"bot": True} if is_bot else {}),
            },
        )
    except Exception:
        pass
    return response


def telemetry_health(request: HttpRequest) -> JsonResponse:
    if not getattr(request.user, "is_staff", False):
        return JsonResponse({"detail": "forbidden"}, status=403)
    db_alias = getattr(settings, "TELEMETRY_DB_ALIAS", "default")
    engine = settings.DATABASES.get(db_alias, {}).get("ENGINE")
    last_event = (
        WebEvent.objects.using(db_alias)
        .order_by("-event_ts")
        .values_list("event_ts", flat=True)
        .first()
    )
    payload = {
        "ok": True,
        "engine": engine,
        "db_alias": db_alias,
        "last_event_ts": last_event.isoformat() if last_event else None,
        "checked_at": now().isoformat(),
    }
    return JsonResponse(payload)

# Create your views here.
