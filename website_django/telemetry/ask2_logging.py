from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime
from typing import Any, Dict, Optional

from django.conf import settings
from django.db import transaction
from django.utils.timezone import now

from telemetry.consent import get_consent_from_request
from telemetry.models import WebAsk2Conversation, WebAsk2Message
from telemetry.utils import get_ip_fields

logger = logging.getLogger(__name__)

ASK2_CONVERSATION_COOKIE = "ask2_conversation_id"
ASK2_MAX_CONTENT = 20000


def _truncate(value: str, max_len: int = ASK2_MAX_CONTENT) -> str:
    if len(value) <= max_len:
        return value
    return value[:max_len]


def get_or_create_conversation_id(request) -> uuid.UUID:
    header_value = request.headers.get("x-ask2-conversation-id", "").strip()
    if header_value:
        try:
            return uuid.UUID(header_value)
        except ValueError:
            pass
    cookie_value = request.COOKIES.get(ASK2_CONVERSATION_COOKIE, "")
    if cookie_value:
        try:
            return uuid.UUID(cookie_value)
        except ValueError:
            pass
    return uuid.uuid4()


def ensure_session_key(request) -> Optional[str]:
    try:
        if not request.session.session_key:
            request.session.save()
        return request.session.session_key
    except Exception:
        return None


def log_ask2_exchange(
    *,
    request,
    conversation_id: uuid.UUID,
    prompt_text: str,
    reply_text: str,
    latency_ms: Optional[int],
    model_name: Optional[str],
    tokens_in: Optional[int],
    tokens_out: Optional[int],
    request_id: Optional[str],
    status: str,
    error_class: Optional[str] = None,
    error_msg: Optional[str] = None,
    path_first: Optional[str] = None,
) -> None:
    db_alias = getattr(settings, "TELEMETRY_DB_ALIAS", "default")
    ip_prefix, ip_hash = get_ip_fields(request)
    user_agent = (request.META.get("HTTP_USER_AGENT") or "")[:512] or None
    session_key = ensure_session_key(request)
    user_id = request.user.id if getattr(request.user, "is_authenticated", False) else None
    consent = get_consent_from_request(request)
    timestamp = now()

    def _write():
        conversation, created = WebAsk2Conversation.objects.using(db_alias).get_or_create(
            conversation_id=conversation_id,
            defaults={
                "created_at": timestamp,
                "last_message_at": timestamp,
                "user_id": user_id,
                "session_key": session_key,
                "ip_hash": ip_hash,
                "ip_prefix": ip_prefix,
                "user_agent": user_agent,
                "path_first": path_first,
                "consent_analytics_effective": "Y" if consent.analytics else "N",
            },
        )
        if not created:
            conversation.last_message_at = timestamp
            if user_id:
                conversation.user_id = user_id
            if session_key:
                conversation.session_key = session_key
            if ip_hash:
                conversation.ip_hash = ip_hash
            if ip_prefix:
                conversation.ip_prefix = ip_prefix
            if user_agent:
                conversation.user_agent = user_agent
            if not conversation.path_first and path_first:
                conversation.path_first = path_first
            conversation.save(
                using=db_alias,
                update_fields=[
                    "last_message_at",
                    "user_id",
                    "session_key",
                    "ip_hash",
                    "ip_prefix",
                    "user_agent",
                    "path_first",
                ],
            )

        WebAsk2Message.objects.using(db_alias).create(
            conversation=conversation,
            created_at=timestamp,
            role="user",
            content=_truncate(prompt_text),
            content_len=len(prompt_text),
            model_name=model_name,
            latency_ms=latency_ms,
            tokens_in=tokens_in,
            tokens_out=tokens_out,
            request_id=request_id,
            status=status,
        )
        WebAsk2Message.objects.using(db_alias).create(
            conversation=conversation,
            created_at=timestamp,
            role="assistant",
            content=_truncate(reply_text),
            content_len=len(reply_text),
            model_name=model_name,
            latency_ms=latency_ms,
            tokens_in=tokens_in,
            tokens_out=tokens_out,
            request_id=request_id,
            status=status,
            error_class=error_class,
            error_msg=_truncate(error_msg or "", max_len=512) if error_msg else None,
        )

    try:
        with transaction.atomic(using=db_alias):
            _write()
    except Exception as exc:
        logger.error(
            "ask2_log_failed conversation_id=%s err=%s",
            conversation_id,
            exc.__class__.__name__,
        )
