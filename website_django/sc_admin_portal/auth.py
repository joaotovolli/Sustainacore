import os
from functools import wraps

from django.contrib.auth import get_user_model, login
from django.http import HttpResponseNotFound

from core.auth import get_auth_email, is_logged_in

ADMIN_EMAIL_ENV = "SC_ADMIN_EMAIL"
DEFAULT_ADMIN_EMAIL = "joaotovolli@hotmail.com"
X_ROBOTS_TAG_VALUE = "noindex, nofollow"


def get_admin_email() -> str:
    return os.getenv(ADMIN_EMAIL_ENV, DEFAULT_ADMIN_EMAIL).strip().lower()


def portal_not_found() -> HttpResponseNotFound:
    response = HttpResponseNotFound("Not Found")
    response["X-Robots-Tag"] = X_ROBOTS_TAG_VALUE
    return response


def _sync_session_user(request, admin_email: str):
    if not is_logged_in(request):
        return None
    email = (get_auth_email(request) or "").strip().lower()
    if email != admin_email:
        return None
    User = get_user_model()
    user = User.objects.filter(email__iexact=email).first()
    if not user:
        user = User.objects.create_user(username=email, email=email)
    if not user.is_active:
        return None
    if user.email.strip().lower() != email:
        user.email = email
        user.save(update_fields=["email"])
    login(request, user, backend="django.contrib.auth.backends.ModelBackend")
    return user


def require_sc_admin(view_func):
    @wraps(view_func)
    def _wrapped(request, *args, **kwargs):
        if not getattr(request.user, "is_authenticated", False):
            _sync_session_user(request, get_admin_email())
        if not getattr(request.user, "is_authenticated", False):
            return portal_not_found()
        email = (getattr(request.user, "email", "") or "").strip().lower()
        if email != get_admin_email():
            return portal_not_found()
        response = view_func(request, *args, **kwargs)
        response["X-Robots-Tag"] = X_ROBOTS_TAG_VALUE
        return response

    return _wrapped
