import io, json, os
from smalltalk import smalltalk_response

ALT_KEYS = ("question","q","message","text","prompt","input")


def _flag_enabled(value: str, *, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() not in {"0", "false", "off", "no"}


SMALL_TALK_ENABLED = _flag_enabled(os.getenv("SMALL_TALK"), default=True)

class SmalltalkMiddleware:
    def __init__(self, app):
        self.app = app

    def __call__(self, environ, start_response):
        try:
            path = environ.get("PATH_INFO", "")
            method = environ.get("REQUEST_METHOD", "GET")
            if method in ("POST", "GET"):
                # Read request body (and re-inject if we don't short-circuit)
                try:
                    length = int(environ.get("CONTENT_LENGTH") or "0")
                except ValueError:
                    length = 0
                body = environ.get("wsgi.input").read(length) if length > 0 else b""

                q = ""
                if body:
                    try:
                        data = json.loads(body.decode("utf-8", "ignore"))
                        if isinstance(data, str):
                            q = data.strip()
                        elif isinstance(data, dict):
                            for k in ALT_KEYS:
                                v = data.get(k)
                                if isinstance(v, str) and v.strip():
                                    q = v.strip()
                                    break
                    except Exception:
                        pass

                if not q and method == "GET":
                    from urllib.parse import parse_qs
                    qs = parse_qs(environ.get("QUERY_STRING", ""))
                    for k in ("q","question"):
                        v = (qs.get(k, [""])[0] or "").strip()
                        if v:
                            q = v
                            break

                if q and SMALL_TALK_ENABLED:
                    resp = smalltalk_response(q)
                    if resp:
                        payload = json.dumps(resp).encode("utf-8")
                        start_response("200 OK", [
                            ("Content-Type", "application/json; charset=utf-8"),
                            ("Content-Length", str(len(payload))),
                            ("X-Smalltalk-Handled", path or "/"),
                        ])
                        return [payload]

                # Not smalltalk → restore body and continue
                environ["wsgi.input"] = io.BytesIO(body)
                environ["CONTENT_LENGTH"] = str(len(body))
        except Exception:
            # fail-open to main app
            pass
        return self.app(environ, start_response)
