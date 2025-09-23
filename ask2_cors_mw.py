# ask2_cors_mw.py
# WSGI middleware to handle CORS preflight and mirror CORS headers on actual responses.
# Designed to wrap a Flask app.wsgi_app. Idempotent and dependency-free.

from typing import Iterable, List, Tuple

class CORSReflectMiddleware:
    def __init__(self, app, allowed_origins=None):
        self.app = app
        self.allowed = set(allowed_origins or [])

    def _is_allowed(self, origin: str) -> bool:
        return bool(origin) and origin in self.allowed

    def __call__(self, environ, start_response):
        method = environ.get('REQUEST_METHOD', 'GET').upper()
        origin = environ.get('HTTP_ORIGIN', '')
        acrh = environ.get('HTTP_ACCESS_CONTROL_REQUEST_HEADERS', '')

        def cors_start_response(status: str, headers: List[Tuple[str, str]], exc_info=None):
            # Mirror CORS headers on actual responses
            if self._is_allowed(origin):
                # Ensure we don't duplicate headers
                names = {k.lower() for k, _ in headers}
                if 'access-control-allow-origin' not in names:
                    headers.append(('Access-Control-Allow-Origin', origin))
                if 'access-control-allow-credentials' not in names:
                    headers.append(('Access-Control-Allow-Credentials', 'true'))
                if 'vary' in names:
                    # Append to existing Vary
                    new_headers = []
                    for k, v in headers:
                        if k.lower() == 'vary':
                            # Ensure Origin, Access-Control-Request-Headers present
                            vary_set = {p.strip() for p in v.split(',') if p.strip()}
                            vary_set.update(['Origin', 'Access-Control-Request-Headers'])
                            v = ', '.join(sorted(vary_set))
                        new_headers.append((k, v))
                    headers = new_headers
                else:
                    headers.append(('Vary', 'Origin, Access-Control-Request-Headers'))
            return start_response(status, headers, exc_info)

        if method == 'OPTIONS':
            # Preflight
            status = '204 No Content'
            headers: List[Tuple[str, str]] = [
                ('Access-Control-Allow-Methods', 'POST, OPTIONS'),
                ('Access-Control-Allow-Headers', acrh),
                ('Access-Control-Max-Age', '86400'),
                ('Vary', 'Origin, Access-Control-Request-Headers'),
            ]
            if self._is_allowed(origin):
                headers.append(('Access-Control-Allow-Origin', origin))
                headers.append(('Access-Control-Allow-Credentials', 'true'))
            start_response(status, headers)
            return [b'']

        # Non-preflight: pass through and inject CORS via wrapped start_response
        return self.app(environ, cors_start_response)
