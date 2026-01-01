import os
from functools import wraps

from django.http import HttpResponseNotFound

ADMIN_EMAIL_ENV = "SC_ADMIN_EMAIL"
DEFAULT_ADMIN_EMAIL = "joaotovolli@hotmail.com"
X_ROBOTS_TAG_VALUE = "noindex, nofollow"


def get_admin_email() -> str:
    return os.getenv(ADMIN_EMAIL_ENV, DEFAULT_ADMIN_EMAIL).strip().lower()


def portal_not_found() -> HttpResponseNotFound:
    response = HttpResponseNotFound("Not Found")
    response["X-Robots-Tag"] = X_ROBOTS_TAG_VALUE
    return response


def require_sc_admin(view_func):
    @wraps(view_func)
    def _wrapped(request, *args, **kwargs):
        if not getattr(request.user, "is_authenticated", False):
            return portal_not_found()
        email = (getattr(request.user, "email", "") or "").strip().lower()
        if email != get_admin_email():
            return portal_not_found()
        response = view_func(request, *args, **kwargs)
        response["X-Robots-Tag"] = X_ROBOTS_TAG_VALUE
        return response

    return _wrapped
