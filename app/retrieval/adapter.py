"""Gemini-first adapter that orchestrates Oracle retrieval for /ask2."""

from __future__ import annotations

import logging
import os
import time
from typing import Any, Dict, List, Tuple

from .gemini_gateway import gateway as gemini_gateway
from .oracle_retriever import RetrievalResult, capability_snapshot, retriever
from .settings import settings

try:
    from .service import (
        GeminiUnavailableError as _ServiceUnavailableError,
        RateLimitError as _ServiceRateLimitError,
        run_pipeline as _service_run_pipeline,
    )
except Exception:  # pragma: no cover - keep legacy path available
    class _ServiceUnavailableError(Exception):
        pass

    class _ServiceRateLimitError(Exception):
        detail = "rate_limited"

    _service_run_pipeline = None

LOGGER = logging.getLogger("app.retrieval.adapter")
_FALLBACK_ANSWER = "Gemini is momentarily unavailable, but the retrieved Sustainacore contexts are attached."


def _contexts_to_facts(contexts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    facts: List[Dict[str, Any]] = []
    for idx, ctx in enumerate(contexts or []):
        if not isinstance(ctx, dict):
            continue
        citation = ctx.get("doc_id") or f"CTX_{idx+1}"
        fact = {
            "citation_id": str(citation),
            "title": ctx.get("title") or "",
            "source_name": ctx.get("source_name") or "",
            "source_url": ctx.get("source_url") or "",
            "snippet": ctx.get("chunk_text") or ctx.get("snippet") or "",
            "score": ctx.get("score"),
        }
        facts.append(fact)
    return facts


def _build_sources_from_contexts(contexts: List[Dict[str, Any]]) -> List[str]:
    sources: List[str] = []
    seen: set[str] = set()
    for ctx in contexts or []:
        if not isinstance(ctx, dict):
            continue
        title = str(ctx.get("title") or "").strip()
        url = str(ctx.get("source_url") or "").strip()
        label = title or url
        if not label:
            continue
        key = (label.lower(), url.lower())
        if key in seen:
            continue
        seen.add(key)
        if url and url not in label:
            sources.append(f"{label} - {url}")
        else:
            sources.append(label)
        if len(sources) >= settings.retriever_fact_cap:
            break
    return sources


def _base_meta(result: RetrievalResult, *, k: int, client_ip: str) -> Dict[str, Any]:
    capability = capability_snapshot()
    capability["text_mode"] = result.mode if result.mode in {"oracle_text", "like"} else capability.get("text_mode")

    meta: Dict[str, Any] = {
        "routing": "gemini_first",
        "retriever": {
            "mode": result.mode,
            "latency_ms": result.latency_ms,
            "note": result.note,
            "returned": len(result.contexts),
        },
        "client_ip": client_ip or "unknown",
        "k": k,
    }
    debug_block = meta.setdefault("debug", {})
    debug_block["capability"] = capability
    return meta


def ask2_pipeline_first(question: str, k: int, *, client_ip: str = "unknown") -> Tuple[Dict[str, Any], int]:
    """Run Oracle retrieval and compose with Gemini once."""

    sanitized_question = (question or "").strip()
    try:
        k_value = int(k)
    except (TypeError, ValueError):
        k_value = 4
    if k_value < 1:
        k_value = 1

    # Prefer the shared service pipeline (intent + planner + composer) when available.
    if _service_run_pipeline is not None:
        try:
            payload = _service_run_pipeline(sanitized_question, k=k_value, client_ip=client_ip or "unknown")
            shaped = payload if isinstance(payload, dict) else {}
            shaped.setdefault("meta", {})
            shaped["meta"].setdefault("routing", "gemini_first")
            contexts = shaped.get("contexts")
            if not isinstance(contexts, list) or not contexts:
                shaped["meta"].setdefault("note", "no_contexts")
            return shaped, 200
        except _ServiceRateLimitError as exc:  # type: ignore[arg-type]
            meta = {
                "routing": "gemini_first",
                "k": k_value,
                "client_ip": client_ip or "unknown",
                "error": getattr(exc, "detail", "rate_limited"),
            }
            message = "You’ve hit the current rate limit. Please retry in a few seconds."
            return {"answer": message, "sources": [], "contexts": [], "meta": meta}, 200
        except _ServiceUnavailableError:
            pass  # fall through to legacy path if the service is disabled
        except Exception as exc:  # pragma: no cover - defensive logging
            LOGGER.exception("gemini_service_pipeline_failed", exc_info=exc)

    retrieval_start = time.perf_counter()
    result = retriever.retrieve(sanitized_question, k_value)
    oracle_latency_ms = int((time.perf_counter() - retrieval_start) * 1000)
    meta = _base_meta(result, k=k_value, client_ip=client_ip)
    meta["retriever"]["latency_ms"] = oracle_latency_ms

    contexts = result.contexts
    if not contexts:
        meta.setdefault("note", "no_contexts")
        payload = {"answer": "", "sources": [], "contexts": [], "meta": meta}
        return payload, 200

    facts = _contexts_to_facts(contexts)
    retriever_payload = {"facts": facts, "context_note": result.note}
    plan_payload = {
        "filters": {},
        "k": k_value,
        "query_variants": [sanitized_question] if sanitized_question else [],
    }

    try:
        compose_start = time.perf_counter()
        composed = gemini_gateway.compose_answer(sanitized_question, retriever_payload, plan_payload, hop_count=1)
    except Exception as exc:
        LOGGER.exception("gemini_compose_failed", exc_info=exc)
        meta["routing"] = "gemini_first_fail"
        meta.setdefault("error", str(exc))
        payload = {
            "answer": _FALLBACK_ANSWER,
            "sources": _build_sources_from_contexts(contexts),
            "contexts": contexts,
            "meta": meta,
        }
        return payload, 200

    answer = str(composed.get("answer") or "").strip()
    sources_raw = composed.get("sources") if isinstance(composed, dict) else None
    sources: List[str] = []
    if isinstance(sources_raw, list):
        for item in sources_raw:
            if isinstance(item, str) and item.strip():
                sources.append(item.strip())
    if not sources:
        sources = _build_sources_from_contexts(contexts)

    gem_meta = gemini_gateway.last_meta
    if gem_meta:
        meta.setdefault("gemini", gem_meta)

    fallback_needed = not answer or answer.lower().startswith("i don't have") or answer.lower().startswith("i’m sorry")
    if fallback_needed and contexts and os.getenv("RETURN_TOP_AS_ANSWER", "0") == "1":
        top_snippet = (contexts[0].get("chunk_text") or contexts[0].get("snippet") or "").strip()
        if top_snippet:
            answer = top_snippet[:500]
            meta.setdefault("note", "vector_snippet_fallback")
    payload = {"answer": answer, "sources": sources, "contexts": contexts, "meta": meta}
    return payload, 200


__all__ = ["ask2_pipeline_first"]
