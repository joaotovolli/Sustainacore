from importlib import import_module
import json, urllib.request, urllib.error, urllib.parse
from pathlib import Path
import time
from flask import request, jsonify

# import existing app
app_mod = import_module("app")

# Allow the legacy ``app`` module to expose ``app.<submodule>`` helpers that
# live inside the ``app/`` package directory.
try:
    _APP_PKG_PATH = Path(__file__).resolve().parent.parent / "app"
    if _APP_PKG_PATH.exists():
        app_mod.__path__ = [str(_APP_PKG_PATH)]  # type: ignore[attr-defined]
        if getattr(app_mod, "__spec__", None):
            app_mod.__spec__.submodule_search_locations = [str(_APP_PKG_PATH)]  # type: ignore[attr-defined]
except Exception:
    _APP_PKG_PATH = None

try:
    from app.rag.routing import route_ask2  # type: ignore
except Exception:  # pragma: no cover - safety net for bootstrap issues
    route_ask2 = None
base_app = getattr(app_mod, "app", None)
_MODULE_START_TS = time.time()

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


FALLBACK_MESSAGE = (
    "I couldn’t find a direct answer in the indexed docs. Here are the most relevant sources."
)


def _sanitize_meta_k(value, default=4):
    try:
        k_val = int(value)
    except (TypeError, ValueError):
        k_val = default
    if k_val < 1:
        k_val = 1
    if k_val > 10:
        k_val = 10
    return k_val


def _call_route_ask2(q, k_value):
    k_sanitized = _sanitize_meta_k(k_value)
    if callable(route_ask2):
        try:
            shaped = route_ask2(q, k_sanitized)
            if isinstance(shaped, dict):
                meta = shaped.get("meta")
                if isinstance(meta, dict):
                    meta.setdefault("k", k_sanitized)
                else:
                    shaped["meta"] = {"k": k_sanitized}
                return shaped
        except Exception:
            pass
    return {
        "answer": FALLBACK_MESSAGE,
        "sources": [],
        "meta": {
            "routing": "no_hit",
            "top_score": None,
            "gemini_used": False,
            "k": k_sanitized,
            "error": "router_unavailable",
        },
    }


def _extract_ask2_params():
    if request.method == "POST":
        data = request.get_json(silent=True) or {}
        q = data.get("q") or data.get("question") or data.get("text") or ""
        k_value = data.get("k") or data.get("top_k") or data.get("limit")
    else:
        args = request.args
        q = args.get("q") or args.get("question") or args.get("text") or ""
        k_value = args.get("k") or args.get("top_k") or args.get("limit")
    return q, k_value


def _ask2_facade_handler():
    q, k_value = _extract_ask2_params()
    shaped = _call_route_ask2(q, k_value)
    return jsonify(shaped), 200

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

# Ensure /ask2 is served by the new routing layer (GET and POST compatibility).
if base_app:
    try:
        if "ask2" in base_app.view_functions:
            base_app.view_functions["ask2"] = _ask2_facade_handler
        else:
            base_app.add_url_rule("/ask2", view_func=_ask2_facade_handler, methods=["POST"], endpoint="ask2")
        if not any(str(rule.rule) == "/ask2" and "GET" in (rule.methods or []) for rule in base_app.url_map.iter_rules()):
            base_app.add_url_rule("/ask2", view_func=_ask2_facade_handler, methods=["GET"], endpoint="ask2_facade_get")
    except Exception:
        pass

app = base_app

if app:
    @app.route("/healthz", endpoint="healthz_facade")
    def _healthz_facade():
        return jsonify({"ok": True})

    @app.route("/metrics", endpoint="metrics_facade")
    def _metrics_facade():
        uptime = time.time() - _MODULE_START_TS
        if uptime <= 0:
            uptime = 1e-6
        return jsonify({"uptime": float(uptime)})

