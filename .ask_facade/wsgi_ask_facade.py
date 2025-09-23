from importlib import import_module
import json, urllib.request, urllib.error, urllib.parse
from flask import request, jsonify

# import existing app
app_mod = import_module("app")
base_app = getattr(app_mod, "app", None)

def _shape(j):
    if not isinstance(j, dict):
        try: j = json.loads(j)
        except Exception: j = {}
    ans = j.get("answer") or j.get("text") or j.get("summary") or ""
    src = j.get("sources") or j.get("chunks") or j.get("results") or []
    meta = j.get("meta") or {}
    return {"answer": ans, "sources": src, "meta": meta}

def _non_empty_answer(shaped):
    return bool(shaped and isinstance(shaped, dict) and shaped.get("answer"))

def _try_http_json(url, method="POST", body=None, timeout=20):
    try:
        data = None if body is None else (json.dumps(body).encode("utf-8") if isinstance(body, dict) else body)
        req = urllib.request.Request(url, data=data, headers={"Content-Type":"application/json"}, method=method)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            txt = r.read().decode("utf-8","ignore")
            try: j = json.loads(txt)
            except Exception: j = {}
            return _shape(j)
    except Exception:
        return None

def _try_app_ask(q):
    view = getattr(app_mod, "ask", None)
    if not callable(view) or base_app is None:
        return None
    with base_app.test_request_context("/ask", method="POST", json={"q": q}):
        resp = view()
    try:
        if hasattr(resp, "get_json"):
            j = resp.get_json(silent=True) or {}
        elif isinstance(resp, tuple) and resp:
            r0 = resp[0]
            if hasattr(r0, "get_json"):
                j = r0.get_json(silent=True) or {}
            elif isinstance(r0, (bytes, str)):
                j = json.loads(r0 if isinstance(r0, str) else r0.decode())
            else:
                j = {}
        elif isinstance(resp, (bytes, str)):
            j = json.loads(resp if isinstance(resp, str) else resp.decode())
        elif isinstance(resp, dict):
            j = resp
        else:
            j = {}
    except Exception:
        j = {}
    shaped = _shape(j)
    # treat retrieval-only as incomplete (no answer)
    return shaped if _non_empty_answer(shaped) else None

def _try_composer(q):
    # Try composer-first (Gemini drafts)
    for url in ("http://127.0.0.1:8088/compose", "http://127.0.0.1:8088/ask"):
        shaped = _try_http_json(url, method="POST", body={"q": q})
        if _non_empty_answer(shaped):
            return shaped
    # Try GET /ask?q=...
    shaped = _try_http_json(f"http://127.0.0.1:8088/ask?q={urllib.parse.quote(q)}", method="GET", body=None)
    if _non_empty_answer(shaped):
        return shaped
    return None

def _try_same_app_backends(q):
    if not base_app:
        return {"answer":"", "sources":[], "meta":{"error":"no_backend"}}
    try:
        with base_app.test_client() as c:
            payload = {"q": q}
            for path in ("/api/ask", "/v1/ask"):
                r = c.post(path, json=payload)
                if r.status_code == 200:
                    j = r.get_json(silent=True) or {}
                    if not j:
                        try: j = json.loads(r.data.decode("utf-8","ignore"))
                        except Exception: j = {}
                    shaped = _shape(j)
                    if _non_empty_answer(shaped):
                        return shaped
            r = c.get("/ask2", query_string={"q": q})
            if r.status_code == 200:
                j = r.get_json(silent=True) or {}
                if not j:
                    try: j = json.loads(r.data.decode("utf-8","ignore"))
                    except Exception: j = {}
                shaped = _shape(j)
                if _non_empty_answer(shaped):
                    return shaped
            # If none gave an answer, still return retrieval so APEX sees something
            if 'j' in locals():
                return _shape(j)
    except Exception:
        pass
    return {"answer":"", "sources":[], "meta":{"error":"no_backend"}}

# Mount /ask (idempotent)
if base_app and not any(str(r.rule) == "/ask" for r in base_app.url_map.iter_rules()):
    @base_app.route("/ask", methods=["POST"])
    def _ask_facade():
        data = request.get_json(silent=True, force=True) or {}
        q = data.get("q") or data.get("question") or data.get("text") or ""
        # Composer first
        shaped = _try_composer(q)
        # Then in-app ask if composer missing
        if not _non_empty_answer(shaped):
            shaped = _try_app_ask(q) or shaped
        # Then same-app backends, and finally retrieval-only if needed
        if not _non_empty_answer(shaped):
            shaped = _try_same_app_backends(q)
        return jsonify(shaped), 200

app = base_app
