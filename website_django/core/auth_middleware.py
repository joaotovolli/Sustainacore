from __future__ import annotations

from django.utils.deprecation import MiddlewareMixin

from core.auth import COOKIE_NAME, get_auth_token, get_email_from_token


class AuthCookieMiddleware(MiddlewareMixin):
    def process_request(self, request):
        token = get_auth_token(request)
        if not token:
            return None
        if request.session.get("auth_email"):
            return None
        email = get_email_from_token(token)
        if email:
            request.session["auth_email"] = email
            request._auth_cookie_invalid = False
        else:
            request._auth_cookie_invalid = True

    def process_response(self, request, response):
        if getattr(request, "_auth_cookie_invalid", False):
            response.delete_cookie(COOKIE_NAME, path="/", samesite="Lax")
            request.session.pop("auth_email", None)
        return response
