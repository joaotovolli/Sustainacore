from __future__ import annotations

from django.conf import settings
import base64
import json

COOKIE_NAME = "sc_session"
MAX_COOKIE_AGE = 30 * 24 * 60 * 60


def get_auth_token(request) -> str | None:
    return request.COOKIES.get(COOKIE_NAME)


def _b64url_decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(value + padding)


def _extract_email_from_claims(claims: dict) -> str | None:
    for key in ("email", "sub", "user", "upn"):
        value = claims.get(key)
        if isinstance(value, str) and "@" in value:
            return value.strip()
    return None


def get_email_from_token(token: str | None) -> str | None:
    if not token or token.count(".") < 2:
        return None
    parts = token.split(".")
    if len(parts) < 2:
        return None
    try:
        payload = _b64url_decode(parts[1])
        claims = json.loads(payload.decode("utf-8"))
    except (ValueError, json.JSONDecodeError, UnicodeDecodeError):
        return None
    if not isinstance(claims, dict):
        return None
    return _extract_email_from_claims(claims)


def get_auth_email(request) -> str | None:
    email = (request.session.get("auth_email") or "").strip()
    return email or None


def is_logged_in(request) -> bool:
    token = get_auth_token(request)
    email = get_auth_email(request)
    return bool(token and email)


def get_masked_email_from_session(request) -> str | None:
    email = (request.session.get("auth_email") or "").strip()
    if not email or "@" not in email:
        return None
    name, domain = email.split("@", 1)
    if len(name) <= 1:
        masked = "*"
    else:
        masked = f"{name[0]}***"
    return f"{masked}@{domain}"


def get_localpart_from_session(request) -> str | None:
    email = (request.session.get("auth_email") or "").strip()
    if not email or "@" not in email:
        return None
    return email.split("@", 1)[0]


def apply_auth_cookie(response, token: str, expires_in_seconds: int | None) -> None:
    if expires_in_seconds is None:
        max_age = 3600
    else:
        max_age = min(int(expires_in_seconds), MAX_COOKIE_AGE)
    response.set_cookie(
        COOKIE_NAME,
        token,
        max_age=max_age,
        httponly=True,
        secure=not settings.DEBUG,
        samesite="Lax",
        path="/",
    )


def clear_auth_cookie(response) -> None:
    response.delete_cookie(
        COOKIE_NAME,
        path="/",
        samesite="Lax",
    )
