# v6: Safe /ask facade auto-mount
import os, sys, json, importlib
try:
    from flask import Flask, request, jsonify
except Exception:
    Flask = None

def _log(*a):
    try: print("[ASK_FACADE v6]", *a, file=sys.stderr)
    except Exception: pass

def _has_route(app, rule):
    try: return any(str(r.rule) == rule for r in app.url_map.iter_rules())
    except Exception: return False

def _shape(j):
    if not isinstance(j, dict):
        try: j = json.loads(j)
        except Exception: j = {}
    ans = j.get("answer") or j.get("text") or j.get("summary") or ""
    src = j.get("sources") or j.get("chunks") or j.get("results") or []
    meta = j.get("meta") or {}
    model = os.getenv("GEMINI_MODEL","")
    mi = meta.setdefault("model_info", {})
    if model and not mi.get("llm"): mi["llm"] = model
    return {"answer": ans, "sources": src, "meta": meta}

def _resolve_handler():
    spec = os.getenv("ASK_FACADE_HANDLER","")
    if spec and ":" in spec:
        mn, fn = spec.split(":",1)
        try:
            m = importlib.import_module(mn)
            f = getattr(m, fn, None)
            if callable(f): return f
            _log("handler not callable", spec)
        except Exception as e:
            _log("handler import failed", spec, e)
    return None

def _call_handler(handler, q):
    for payload in (q, {"q": q}, {"question": q}):
        try:
            res = handler(payload) if isinstance(payload, dict) else handler(q)
            if isinstance(res, (bytes, str)):
                try: res = json.loads(res if isinstance(res, str) else res.decode())
                except Exception: res = {"answer": res if isinstance(res, str) else res.decode()}
            if isinstance(res, dict): return _shape(res), 200
        except Exception as e:
            _log("handler call failed", e)
    return None, None

def _try_backends(app, q):
    backends = [p.strip() for p in os.getenv("ASK_FACADE_BACKENDS","/ask2,/api/ask,/v1/ask").split(",") if p.strip()]
    try:
        client = app.test_client()
        payload = json.dumps({"q": q})
        for path in backends:
            rv = client.post(path, data=payload, content_type="application/json")
            if rv.status_code == 405:
                rv = client.get(path, query_string={"q": q})
            if rv.status_code == 200:
                try: j = rv.get_json(silent=True) or json.loads(rv.data.decode("utf-8","ignore"))
                except Exception: j = {}
                return _shape(j), 200
    except Exception as e:
        _log("backend attempts failed", e)
    return {"answer":"", "sources":[], "meta":{"error":"no_backend"}}, 404

if Flask is not None:
    _orig_init = Flask.__init__
    def _patched_init(self, *a, **k):
        _orig_init(self, *a, **k)
        if os.getenv("ASK_FACADE","").lower() != "on": return
        if _has_route(self, "/ask"):
            _log("/ask already present (no-op)"); return
        handler = _resolve_handler()
        _log("mounted /ask; handler:", ("present" if handler else "none"))
        @self.route("/ask", methods=["POST"])
        def _ask_facade():
            try: data = request.get_json(silent=True, force=True) or {}
            except Exception: data = {}
            q = data.get("q") or data.get("question") or data.get("text") or ""
            if handler:
                shaped, code = _call_handler(handler, q)
                if shaped: return jsonify(shaped), code or 200
            shaped, code = _try_backends(self, q)
            return jsonify(shaped), code
    Flask.__init__ = _patched_init
    _log("Flask.__init__ patched to auto-install /ask")
else:
    _log("Flask not importable; facade inactive")
