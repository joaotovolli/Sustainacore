import json
from app import app as downstream
from refinedriver import refine_maybe

def _collect_iter(it):
    try:
        return b"".join(it)
    finally:
        if hasattr(it, "close"):
            it.close()

def app(environ, start_response):
    method = environ.get("REQUEST_METHOD","GET").upper()
    path   = environ.get("PATH_INFO","")

    sh = {}
    def _sr(status, headers, exc_info=None):
        sh["status"]  = status
        sh["headers"] = headers
        return lambda b: None

    body_iter = downstream(environ, _sr)
    body = _collect_iter(body_iter)
    status  = sh.get("status", "200 OK")
    headers = sh.get("headers", [])

    # Intercept only successful JSON responses from POST /ask
    if method == "POST" and path == "/ask" and status.startswith("200"):
        try:
            obj = json.loads(body.decode("utf-8"))
            ans = obj.get("answer")
            ctxs = obj.get("contexts") or obj.get("top_contexts")
            if isinstance(ans, str):
                obj["answer"] = refine_maybe(ans, ctxs)
                new = json.dumps(obj, ensure_ascii=False).encode("utf-8")
                # fix headers
                headers = [(k,v) for (k,v) in headers if k.lower() != "content-length"]
                # ensure content-type
                if not any(k.lower()=="content-type" for k,_ in headers):
                    headers.append(("Content-Type","application/json"))
                headers.append(("Content-Length", str(len(new))))
                start_response(status, headers)
                return [new]
        except Exception:
            # On any error, just passthrough original response
            pass

    # passthrough
    start_response(status, headers)
    return [body]
