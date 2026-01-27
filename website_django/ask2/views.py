from __future__ import annotations

import json
import time
from datetime import datetime
from typing import Any, Dict
from urllib.parse import urlparse

from django.conf import settings
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt

from . import client
from telemetry.consent import get_consent_from_request
from telemetry.logger import record_event
from telemetry.ask2_logging import (
    ASK2_CONVERSATION_COOKIE,
    get_or_create_conversation_id,
    log_ask2_exchange,
)


def ask2_page(request: HttpRequest) -> HttpResponse:
    """Render the Ask2 chat page with the shared site template."""
    return render(
        request,
        "ask2.html",
        {
            "year": datetime.now().year,
        },
    )


def _origin_allowed(value: str) -> bool:
    if not value:
        return False
    parsed = urlparse(value)
    if not parsed.scheme or not parsed.hostname:
        return False
    host = parsed.hostname.lower()
    if parsed.scheme == "https" and host in {
        "sustainacore.org",
        "www.sustainacore.org",
        "preview.sustainacore.org",
    }:
        return True
    if parsed.scheme == "http" and host in {"127.0.0.1", "localhost"}:
        return True
    return False


@csrf_exempt
def ask2_api(request: HttpRequest) -> JsonResponse:
    """Proxy Ask2 chat requests to the VM1 backend."""
    if request.method != "POST":
        return JsonResponse({"error": "method_not_allowed", "message": "Use POST."}, status=405)

    origin = (request.META.get("HTTP_ORIGIN") or "").strip()
    referer = (request.META.get("HTTP_REFERER") or "").strip()
    if origin or referer:
        if not (_origin_allowed(origin) or _origin_allowed(referer)):
            return JsonResponse({"error": "origin_not_allowed", "message": "Origin not allowed."}, status=403)

    start = time.monotonic()
    user_message = ""
    if request.content_type and "application/json" in request.content_type:
        try:
            payload = json.loads(request.body or b"{}")
        except json.JSONDecodeError:
            return JsonResponse({"error": "invalid_payload", "message": "Invalid JSON payload."}, status=400)
        user_message = (payload.get("message") or payload.get("user_message") or "").strip()
    else:
        user_message = (
            request.POST.get("message")
            or request.POST.get("user_message")
            or ""
        ).strip()

    if not user_message:
        return JsonResponse({"error": "missing_message", "message": "Message is required."}, status=400)

    conversation_id = get_or_create_conversation_id(request)
    result = client.ask2_query(user_message)

    has_error = bool(result.get("error"))
    status_code = 200 if not has_error else 502
    response_data: Dict[str, Any] = {
        "session_id": result.get("session_id"),
        "reply": result.get("reply")
        or result.get("answer")
        or result.get("content"),
        "answer": result.get("answer"),
        "content": result.get("content"),
        "message": result.get("message"),
        "sources": result.get("sources"),
    }
    if has_error:
        response_data["error"] = result.get("error")

    latency_ms = int((time.monotonic() - start) * 1000)
    reply_text = (
        response_data.get("reply")
        or response_data.get("answer")
        or response_data.get("content")
        or response_data.get("message")
        or ""
    )
    payload = {
        "latency_ms": latency_ms,
        "success": status_code < 400,
        "model": result.get("model"),
        "prompt_chars": len(user_message),
        "response_chars": len(reply_text or ""),
    }
    if settings.TELEMETRY_STORE_ASK2_TEXT:
        payload["prompt_text"] = user_message
        payload["response_text"] = reply_text
    try:
        consent = get_consent_from_request(request)
        try:
            session_key = getattr(request.session, "session_key", None)
        except Exception:
            session_key = None
        record_event(
            event_type="ask2_chat",
            request=request,
            consent=consent,
            path=request.path,
            query_string=request.META.get("QUERY_STRING") or None,
            http_method=request.method,
            status_code=status_code,
            response_ms=latency_ms,
            payload=payload,
            session_key=session_key,
        )
    except Exception:
        pass

    try:
        log_ask2_exchange(
            request=request,
            conversation_id=conversation_id,
            prompt_text=user_message,
            reply_text=reply_text or "",
            latency_ms=latency_ms,
            model_name=result.get("model"),
            tokens_in=result.get("tokens_in"),
            tokens_out=result.get("tokens_out"),
            request_id=result.get("request_id"),
            status="ok" if status_code < 400 else "error",
            error_class=type(result.get("error")).__name__ if result.get("error") else None,
            error_msg=str(result.get("error")) if result.get("error") else None,
            path_first=request.path,
        )
    except Exception:
        pass

    response = JsonResponse(response_data, status=status_code)
    response.set_cookie(
        ASK2_CONVERSATION_COOKIE,
        str(conversation_id),
        max_age=365 * 24 * 60 * 60,
        httponly=False,
        samesite="Lax",
        secure=not settings.DEBUG,
        path="/",
    )
    return response
