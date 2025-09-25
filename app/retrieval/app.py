from __future__ import annotations
from fastapi import FastAPI, Query
from typing import Any, Dict
import time

# Router is optional; app must still boot if it’s missing or broken.
try:
    from app.rag.routing import route_ask2 as _route_ask2
except Exception:
    _route_ask2 = None

def _sanitize_k(value: Any, default: int = 4) -> int:
    try:
        k = int(value)
    except Exception:
        k = default
    return max(1, min(10, k))

FALLBACK = (
    "I couldn’t find a grounded answer in the indexed docs yet. "
    "Try adding a company/topic (e.g., Microsoft, TECH100) or a specific field."
)

app = FastAPI(title="SustainaCore Retrieval Facade", version="1.0")

@app.get("/healthz")
def healthz():
    return {"status": "ok", "ts": time.time()}

@app.get("/ask2")
def ask2(q: str = Query("", alias="q"), k: int = Query(4, alias="k")) -> Dict[str, Any]:
    q = (q or "").strip()
    k = _sanitize_k(k)
    if not q:
        return {"answer": "Please provide a question (q).",
                "sources": [], "meta": {"routing": "empty", "k": k, "intent": "EMPTY", "latency_ms": 0, "show_debug_block": False}}

    if _route_ask2 is None:
        return {"answer": FALLBACK, "sources": [],
                "meta": {"routing": "fallback", "k": k, "note": "router_missing", "intent": "FALLBACK", "latency_ms": 0, "show_debug_block": False}}

    try:
        shaped = _route_ask2(q, k)
    except Exception:
        return {"answer": FALLBACK, "sources": [],
                "meta": {"routing": "error_fallback", "k": k, "intent": "FALLBACK", "latency_ms": 0, "show_debug_block": False}}

    # Contract guard: ensure non-empty answer and required keys
    if not isinstance(shaped, dict):
        shaped = {}
    ans = str((shaped.get("answer") or "")).strip()
    if not ans:
        meta = shaped.get("meta") or {}
        if not isinstance(meta, dict):
            meta = {}
        meta.update({"note": "nonempty_guard", "k": k})
        meta.setdefault("intent", "FALLBACK")
        meta.setdefault("latency_ms", 0)
        meta.setdefault("show_debug_block", False)
        shaped = {"answer": FALLBACK, "sources": [], "meta": meta}
    else:
        shaped.setdefault("sources", [])
        shaped.setdefault("meta", {})
        shaped["meta"].setdefault("k", k)
        shaped["meta"].setdefault("intent", "UNKNOWN")
        shaped["meta"].setdefault("latency_ms", 0)
        shaped["meta"].setdefault("show_debug_block", False)
    return shaped
