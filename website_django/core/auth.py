from __future__ import annotations

from django.conf import settings

COOKIE_NAME = "sc_session"
MAX_COOKIE_AGE = 30 * 24 * 60 * 60


def get_auth_token(request) -> str | None:
    return request.COOKIES.get(COOKIE_NAME)


def is_logged_in(request) -> bool:
    return bool(get_auth_token(request))


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
