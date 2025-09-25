import time
from typing import Any, Dict, List, Optional, Set, Tuple

from fastapi import Body, FastAPI, HTTPException, Query, Request

# Router is optional; app must still boot if it’s missing or broken.
try:
    from app.rag.routing import route_ask2 as _route_ask2
except Exception:
    _route_ask2 = None

try:
    from app.retrieval.service import (
        GeminiUnavailableError,
        RateLimitError,
        run_pipeline,
    )
except Exception:  # pragma: no cover - fall back to facade-only mode
    run_pipeline = None

    class RateLimitError(Exception):
        """Placeholder when the service layer failed to import."""

    class GeminiUnavailableError(Exception):
        """Placeholder when the service layer failed to import."""

FALLBACK = (
    "I couldn’t find a grounded answer in the indexed docs yet. "
    "Try adding a company/topic (e.g., Microsoft, TECH100) or a specific field."
)

ASK_EMPTY = "Please provide a question so I can help."  # Friendly guardrail


def _sanitize_k(value: Any, default: int = 4) -> int:
    try:
        k = int(value)
    except Exception:
        k = default
    if k < 1:
        k = 1
    if k > 10:
        k = 10
    return k


def _shape_sources_and_contexts(
    raw_sources: Any, raw_contexts: Any = None
) -> Tuple[List[str], List[Dict[str, Any]]]:
    urls: List[str] = []
    contexts: List[Dict[str, Any]] = []
    seen: Set[str] = set()

    def _clean(value: Any) -> Optional[str]:
        if isinstance(value, str):
            stripped = value.strip()
            if stripped:
                return stripped
        return None

    def _register(url_value: Optional[str], title_value: Optional[str]) -> None:
        url_clean = _clean(url_value)
        if not url_clean or url_clean in seen:
            return
        seen.add(url_clean)
        urls.append(url_clean)
        context: Dict[str, Any] = {"source_url": url_clean}
        title_clean = _clean(title_value)
        if title_clean is not None:
            context["title"] = title_clean
        contexts.append(context)

    if isinstance(raw_contexts, list):
        for item in raw_contexts:
            if not isinstance(item, dict):
                continue
            url_candidate = (
                item.get("source_url")
                or item.get("url")
                or item.get("link")
                or item.get("href")
            )
            title_candidate = item.get("title") or item.get("source_title") or item.get("name")
            _register(url_candidate, title_candidate)

    if isinstance(raw_sources, list):
        for item in raw_sources:
            url_candidate: Optional[str] = None
            title_candidate: Optional[str] = None
            if isinstance(item, dict):
                title_candidate = item.get("title") or item.get("source_title") or item.get("name")
                url_candidate = (
                    item.get("url")
                    or item.get("source_url")
                    or item.get("link")
                    or item.get("href")
                )
            elif isinstance(item, str):
                url_candidate = item
            _register(url_candidate, title_candidate)

    return urls, contexts


def _build_payload(
    *,
    answer: str,
    raw_sources: Any,
    top_k: int,
    note: str,
    meta: Optional[Dict[str, Any]] = None,
    raw_contexts: Any = None,
    limit_sources: Optional[int] = None,
) -> Dict[str, Any]:
    sources, contexts = _shape_sources_and_contexts(raw_sources, raw_contexts)
    if limit_sources is None and note != "ok":
        limit_sources = 3
    if limit_sources is not None:
        sources = sources[:limit_sources]
        contexts = contexts[:limit_sources]
    payload_meta: Dict[str, Any] = {}
    if isinstance(meta, dict):
        payload_meta.update(meta)
    payload_meta["provider"] = payload_meta.get("provider") or "oracle"
    payload_meta["note"] = note
    payload_meta["k"] = top_k
    return {
        "answer": answer,
        "contexts": contexts,
        "sources": sources,
        "meta": payload_meta,
    }

_START_TS = time.time()

app = FastAPI(title="SustainaCore Retrieval Facade", version="1.0")


@app.get("/healthz")
def healthz() -> Dict[str, bool]:
    return {"ok": True}


@app.get("/metrics")
def metrics() -> Dict[str, float]:
    uptime = time.time() - _START_TS
    if uptime < 0:
        uptime = 0.0
    return {"uptime": float(uptime)}


@app.post("/ask")
def ask(
    request: Request,
    payload: Dict[str, Any] = Body(default_factory=dict),
) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        payload = {}

    question_value = payload.get("question")
    if question_value is None:
        question_value = payload.get("q")
    if question_value is None:
        question_value = payload.get("text")
    question_text = question_value.strip() if isinstance(question_value, str) else ""

    raw_top_k = payload.get("top_k")
    if raw_top_k is None:
        raw_top_k = payload.get("k")
    if raw_top_k is None:
        raw_top_k = payload.get("limit")
    top_k = _sanitize_k(raw_top_k)

    forwarded = request.headers.get("x-forwarded-for", "")
    if forwarded:
        client_ip = forwarded.split(",")[0].strip() or "unknown"
    else:
        client_ip = request.client.host if request.client and request.client.host else "unknown"

    if not question_text:
        return _build_payload(
            answer=ASK_EMPTY,
            raw_sources=[],
            raw_contexts=[],
            top_k=top_k,
            note="fallback",
            meta={"reason": "empty_question"},
        )

    if run_pipeline is None:
        return _build_payload(
            answer=FALLBACK,
            raw_sources=[],
            raw_contexts=[],
            top_k=top_k,
            note="fallback",
            meta={"reason": "pipeline_unavailable"},
        )

    try:
        result = run_pipeline(question_text, k=top_k, client_ip=client_ip)
    except RateLimitError as exc:
        raise HTTPException(status_code=429, detail=str(exc))
    except GeminiUnavailableError:
        return _build_payload(
            answer=FALLBACK,
            raw_sources=[],
            raw_contexts=[],
            top_k=top_k,
            note="fallback",
            meta={"reason": "gemini_unavailable"},
        )
    except Exception:
        return _build_payload(
            answer=FALLBACK,
            raw_sources=[],
            raw_contexts=[],
            top_k=top_k,
            note="fallback",
            meta={"reason": "pipeline_error"},
        )

    answer_text = str(result.get("answer") or "").strip()
    raw_sources = result.get("sources") or []
    raw_contexts = result.get("contexts")
    meta = result.get("meta") if isinstance(result.get("meta"), dict) else {}

    if not answer_text:
        fallback_meta = dict(meta)
        fallback_meta["reason"] = "empty_answer"
        return _build_payload(
            answer=FALLBACK,
            raw_sources=raw_sources,
            raw_contexts=raw_contexts,
            top_k=top_k,
            note="fallback",
            meta=fallback_meta,
        )

    return _build_payload(
        answer=answer_text,
        raw_sources=raw_sources,
        raw_contexts=raw_contexts,
        top_k=top_k,
        note="ok",
        meta=meta,
    )

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
