# Calls app.ask inside a Flask request context, normalizes JSON
import json, sys
from importlib import import_module

def _shape(j):
    if not isinstance(j, dict):
        try: j = json.loads(j)
        except Exception: j = {}
    ans = j.get("answer") or j.get("text") or j.get("summary") or ""
    src = j.get("sources") or j.get("chunks") or j.get("results") or []
    meta = j.get("meta") or {}
    return {"answer": ans, "sources": src, "meta": meta}

def answer(q):
    try:
        app_mod = import_module("app")
        flask_app = getattr(app_mod, "app", None)
        view = getattr(app_mod, "ask", None)
        if not callable(view) or flask_app is None:
            return {"answer":"", "sources":[], "meta":{"error":"no_app_ask"}}
        with flask_app.test_request_context("/ask", method="POST", json={"q": q if isinstance(q, str) else (q.get("q") if isinstance(q, dict) else str(q))}):
            resp = view()
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
        return _shape(j)
    except Exception as e:
        print("[route_adapter] error:", e, file=sys.stderr)
        return {"answer":"", "sources":[], "meta":{"error":"adapter_exception"}}
