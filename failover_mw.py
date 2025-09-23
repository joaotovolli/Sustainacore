import json
from typing import Callable

def _collect_body(iterable):
    body = b""
    for chunk in iterable:
        body += chunk
    return body

def _safe_json_loads(b):
    try:
        return json.loads(b.decode("utf-8", "ignore"))
    except Exception:
        return None

def _json_dumps(obj):
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")

def _set_header(headers, name, value):
    out = []
    seen = False
    for k,v in headers:
        if k.lower() == name.lower():
            out.append((k, value))
            seen = True
        else:
            out.append((k, v))
    if not seen:
        out.append((name, value))
    return out

def _synthesize(q: str, sources):
    try:
        import gemini_adapter  # CLI-backed shim already installed
    except Exception:
        return ""
    ctx = []
    for s in (sources or [])[:8]:
        if isinstance(s, dict):
            sn = s.get("snippet") or ""
            if sn:
                ctx.append(sn)
    return (gemini_adapter.generate(q or "", context=ctx) or "").strip()

class FailoverMiddleware:
    def __init__(self, app):
        self.app = app

    def __call__(self, environ, start_response: Callable):
        path = environ.get("PATH_INFO", "")
        # Capture q from query string for synthesis
        q = ""
        try:
            qs = environ.get("QUERY_STRING") or ""
            for part in qs.split("&"):
                if part.startswith("q="):
                    q = part[2:].replace("+"," ")
                    break
        except Exception:
            pass

        status_headers = {}
        def _sr(status, headers, exc_info=None):
            status_headers["status"] = status
            status_headers["headers"] = list(headers)
            # ignore write callable (unused with gunicorn)
            return lambda x: None

        result = self.app(environ, _sr)
        body = _collect_body(result)

        if path.startswith("/ask2"):
            payload = _safe_json_loads(body) or {}
            if isinstance(payload, dict):
                ans = payload.get("answer")
                if isinstance(ans, str) and not ans.strip():
                    syn = _synthesize(q, payload.get("sources"))
                    if syn:
                        payload["answer"] = syn
                        body = _json_dumps(payload)
                        # Fix Content-Length if present
                        status_headers["headers"] = _set_header(status_headers["headers"], "Content-Length", str(len(body)))
                        status_headers["headers"] = _set_header(status_headers["headers"], "Content-Type", "application/json; charset=utf-8")

        write = start_response(status_headers["status"], status_headers["headers"])
        return [body]
