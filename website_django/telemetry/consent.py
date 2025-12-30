from __future__ import annotations

import json
from urllib.parse import unquote
from dataclasses import dataclass
from typing import Optional

from django.conf import settings


CONSENT_COOKIE = "sc_consent"


@dataclass
class ConsentState:
    analytics: bool = False
    functional: bool = False
    policy_version: str = ""
    source: str = "banner"


def parse_consent_cookie(raw: str) -> Optional[ConsentState]:
    if not raw:
        return None
    raw = unquote(raw)
    if raw.startswith('"') and raw.endswith('"'):
        raw = raw[1:-1]
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError:
            return None
    if not isinstance(payload, dict):
        return None
    return ConsentState(
        analytics=bool(payload.get("analytics")),
        functional=bool(payload.get("functional")),
        policy_version=str(payload.get("policy_version") or ""),
        source=str(payload.get("source") or "banner"),
    )


def serialize_consent(consent: ConsentState) -> str:
    payload = {
        "analytics": bool(consent.analytics),
        "functional": bool(consent.functional),
        "policy_version": consent.policy_version,
        "source": consent.source,
    }
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=True)


def get_consent_from_request(request) -> ConsentState:
    raw = request.COOKIES.get(CONSENT_COOKIE, "")
    parsed = parse_consent_cookie(raw)
    current_version = getattr(settings, "TELEMETRY_POLICY_VERSION", "")
    if parsed and parsed.policy_version == current_version:
        return parsed
    return ConsentState(
        analytics=False,
        functional=False,
        policy_version=current_version,
        source="unknown",
    )
