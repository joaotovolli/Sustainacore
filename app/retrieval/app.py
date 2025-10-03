import json
import logging
import os
import subprocess
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from anyio import to_thread
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from app.persona import apply_persona
from app.request_normalizer import BAD_INPUT_ERROR, normalize_request

try:  # Import lazily to avoid hard failures when optional deps are missing.
    from app.retrieval.settings import settings as _SETTINGS
except Exception:  # pragma: no cover - defensive fallback when settings fails.
    _SETTINGS = None

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

PERSONA_ENABLED = os.getenv("PERSONA_V1") == "1"
REQUEST_NORMALIZE_ENABLED = os.getenv("REQUEST_NORMALIZE") == "1"

LOGGER = logging.getLogger("ask2")


def _resolve_git_sha() -> str:
    """Best effort resolution of the git SHA for observability surfaces."""

    env_candidates = [
        os.getenv("SUSTAINACORE_GIT_SHA"),
        os.getenv("GIT_SHA"),
    ]
    for candidate in env_candidates:
        if candidate:
            stripped = candidate.strip()
            if stripped:
                return stripped

    file_candidates = [
        os.getenv("SUSTAINACORE_GIT_SHA_FILE"),
        os.getenv("GIT_SHA_FILE"),
    ]

    repo_root = Path(__file__).resolve().parents[2]
    file_candidates.extend(
        [
            str(repo_root / "config" / "git-sha"),
            str(repo_root / "config" / "git_sha"),
        ]
    )

    for file_candidate in file_candidates:
        if not file_candidate:
            continue
        path = Path(file_candidate)
        if not path.exists():
            continue
        try:
            contents = path.read_text(encoding="utf-8").strip()
        except OSError:
            continue
        if contents:
            return contents

    try:
        output = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=str(repo_root),
            stderr=subprocess.DEVNULL,
        )
        candidate = output.decode("utf-8").strip()
        return candidate or "unknown"
    except Exception:  # pragma: no cover - git unavailable in some deployments
        return "unknown"


def _resolve_provider() -> str:
    """Determine which LLM provider is active for reporting."""

    explicit = os.getenv("ASK2_PROVIDER")
    if explicit:
        explicit = explicit.strip().upper()
        if explicit in {"GEMINI", "OLLAMA"}:
            return explicit

    if _SETTINGS and getattr(_SETTINGS, "gemini_first_enabled", False):
        return "GEMINI"

    if run_pipeline is not None:
        return "GEMINI"

    return "OLLAMA"


def _resolve_model(provider: str) -> str:
    if provider == "GEMINI":
        if _SETTINGS and getattr(_SETTINGS, "gemini_model_answer", None):
            return _SETTINGS.gemini_model_answer
        for name in ("GEMINI_ANSWER_MODEL", "GEMINI_MODEL"):
            value = os.getenv(name)
            if value:
                return value
        return "gemini"

    for name in ("SCAI_OLLAMA_MODEL", "OLLAMA_MODEL", "OLLAMA_EMBED_MODEL"):
        value = os.getenv(name)
        if value:
            return value
    return "ollama"


_GIT_SHA = _resolve_git_sha()
_PROVIDER = _resolve_provider()
_MODEL_NAME = _resolve_model(_PROVIDER)

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

    persona_sources: Optional[List[str]] = None
    if PERSONA_ENABLED and note == "ok":
        answer, persona_sources = apply_persona(answer, contexts)
    payload_meta: Dict[str, Any] = {}
    if isinstance(meta, dict):
        payload_meta.update(meta)
    payload_meta["provider"] = payload_meta.get("provider") or "oracle"
    payload_meta["note"] = note
    payload_meta["k"] = top_k
    sources_payload: List[Any]
    if persona_sources is not None:
        sources_payload = persona_sources
    else:
        sources_payload = sources
    return {
        "answer": answer,
        "contexts": contexts,
        "sources": sources_payload,
        "meta": payload_meta,
    }

_START_TS = time.time()

app = FastAPI(title="SustainaCore Retrieval Facade", version="1.0")


def _finalize_response(payload: Dict[str, Any], started: float) -> JSONResponse:
    answer_value = payload.get("answer")
    if not isinstance(answer_value, str):
        payload["answer"] = "" if answer_value is None else str(answer_value)

    contexts_value = payload.get("contexts")
    if not isinstance(contexts_value, list):
        contexts: List[Dict[str, Any]] = []
    else:
        contexts = contexts_value
    payload["contexts"] = contexts

    sources_value = payload.get("sources")
    if not isinstance(sources_value, list):
        payload["sources"] = []

    if "meta" not in payload or not isinstance(payload["meta"], dict):
        payload["meta"] = {}

    latency_ms = int((time.perf_counter() - started) * 1000)

    top_titles: List[str] = []
    for context in contexts[:3]:
        if not isinstance(context, dict):
            continue
        candidate = context.get("title") or context.get("source_title") or context.get("source_url")
        if isinstance(candidate, str):
            stripped = candidate.strip()
            if stripped:
                top_titles.append(stripped)
    LOGGER.info(
        "ask2.response %s",
        json.dumps(
            {
                "contexts_count": len(contexts),
                "top_titles": top_titles,
                "lat_ms": latency_ms,
                "persona": PERSONA_ENABLED,
                "normalize": REQUEST_NORMALIZE_ENABLED,
            },
            ensure_ascii=False,
        ),
    )
    return JSONResponse(payload, media_type="application/json")


@app.get("/health")
async def health() -> JSONResponse:
    payload = {
        "ok": True,
        "git": _GIT_SHA,
        "provider": _PROVIDER,
        "model": _MODEL_NAME,
        "flags": {
            "PERSONA_V1": PERSONA_ENABLED,
            "REQUEST_NORMALIZE": REQUEST_NORMALIZE_ENABLED,
        },
    }
    return JSONResponse(payload, media_type="application/json")


@app.get("/healthz")
def healthz() -> Dict[str, bool]:
    return {"ok": True}


@app.get("/metrics")
def metrics() -> Dict[str, float]:
    uptime = time.time() - _START_TS
    if uptime < 0:
        uptime = 0.0
    return {"uptime": float(uptime)}


@app.post("/ask2")
async def ask2_post(request: Request) -> JSONResponse:
    started = time.perf_counter()
    raw_body = await request.body()
    content_type = (request.headers.get("content-type") or "").split(";")[0].strip().lower()

    payload: Any
    if content_type == "text/plain":
        decoded = raw_body.decode("utf-8", "ignore") if raw_body else ""
        payload = {"question": decoded}
    elif raw_body:
        try:
            decoded = raw_body.decode("utf-8", "ignore")
            payload = json.loads(decoded or "{}")
        except ValueError:
            payload = {}
    else:
        payload = {}

    effective_payload: Dict[str, Any]
    if REQUEST_NORMALIZE_ENABLED:
        normalized, error = await normalize_request(request, payload, raw_body=raw_body)
        if error:
            top_k = _sanitize_k((normalized or {}).get("top_k"))
            response = _build_payload(
                answer="",
                raw_sources=[],
                raw_contexts=[],
                top_k=top_k,
                note="fallback",
                meta={"reason": "bad_input"},
            )
            response["error"] = BAD_INPUT_ERROR
            response.setdefault("contexts", [])
            response.setdefault("sources", [])
            return _finalize_response(response, started)
        effective_payload = normalized
    elif isinstance(payload, dict):
        effective_payload = dict(payload)
    else:
        effective_payload = {}

    question_value = effective_payload.get("question")
    if question_value is None:
        question_value = effective_payload.get("query")
    if question_value is None:
        question_value = effective_payload.get("q")
    if question_value is None:
        question_value = effective_payload.get("text")
    question_text = question_value.strip() if isinstance(question_value, str) else ""

    raw_top_k = effective_payload.get("top_k")
    if raw_top_k is None:
        raw_top_k = effective_payload.get("topK")
    if raw_top_k is None:
        raw_top_k = effective_payload.get("k")
    if raw_top_k is None:
        raw_top_k = effective_payload.get("limit")
    top_k = _sanitize_k(raw_top_k)

    forwarded = request.headers.get("x-forwarded-for", "")
    if forwarded:
        client_ip = forwarded.split(",")[0].strip() or "unknown"
    else:
        client_ip = request.client.host if request.client and request.client.host else "unknown"

    if not question_text:
        return _finalize_response(
            _build_payload(
                answer=ASK_EMPTY,
                raw_sources=[],
                raw_contexts=[],
                top_k=top_k,
                note="fallback",
                meta={"reason": "empty_question"},
            ),
            started,
        )

    if run_pipeline is None:
        return _finalize_response(
            _build_payload(
                answer=FALLBACK,
                raw_sources=[],
                raw_contexts=[],
                top_k=top_k,
                note="fallback",
                meta={"reason": "pipeline_unavailable"},
            ),
            started,
        )

    try:
        result = await to_thread.run_sync(
            run_pipeline,
            question_text,
            k=top_k,
            client_ip=client_ip,
        )
    except RateLimitError as exc:
        raise HTTPException(status_code=429, detail=str(exc))
    except GeminiUnavailableError:
        return _finalize_response(
            _build_payload(
                answer=FALLBACK,
                raw_sources=[],
                raw_contexts=[],
                top_k=top_k,
                note="fallback",
                meta={"reason": "gemini_unavailable"},
            ),
            started,
        )
    except Exception:
        return _finalize_response(
            _build_payload(
                answer=FALLBACK,
                raw_sources=[],
                raw_contexts=[],
                top_k=top_k,
                note="fallback",
                meta={"reason": "pipeline_error"},
            ),
            started,
        )

    answer_text = str(result.get("answer") or "").strip()
    raw_sources = result.get("sources") or []
    raw_contexts = result.get("contexts")
    meta = result.get("meta") if isinstance(result.get("meta"), dict) else {}

    if not answer_text:
        fallback_meta = dict(meta)
        fallback_meta["reason"] = "empty_answer"
        return _finalize_response(
            _build_payload(
                answer=FALLBACK,
                raw_sources=raw_sources,
                raw_contexts=raw_contexts,
                top_k=top_k,
                note="fallback",
                meta=fallback_meta,
            ),
            started,
        )

    return _finalize_response(
        _build_payload(
            answer=answer_text,
            raw_sources=raw_sources,
            raw_contexts=raw_contexts,
            top_k=top_k,
            note="ok",
            meta=meta,
        ),
        started,
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
