"""Shared Gemini-first orchestration helpers used by multiple entrypoints."""

from __future__ import annotations

import logging
import re
import time
from collections import deque
from threading import Lock
from typing import Any, Dict, Iterable, List, Optional

from .gemini_gateway import gateway
from .observability import observer
from .oracle_retriever import retriever
from .settings import settings


LOGGER = logging.getLogger("gemini-service")


class RateLimitError(Exception):
    """Raised when the caller exceeds the configured rate limit."""

    def __init__(self, detail: str = "rate_limited") -> None:
        super().__init__(detail)
        self.detail = detail


class GeminiUnavailableError(Exception):
    """Raised when Gemini-first orchestration is disabled or fails hard."""


_RATE_BUCKETS: Dict[str, deque] = {}
_RATE_LOCK = Lock()


def _enforce_rate_limit(ip: str) -> None:
    window = settings.rate_limit_window_seconds
    limit = settings.rate_limit_max_requests
    now = time.time()
    with _RATE_LOCK:
        bucket = _RATE_BUCKETS.setdefault(ip, deque())
        while bucket and now - bucket[0] > window:
            bucket.popleft()
        if len(bucket) >= limit:
            raise RateLimitError()
        bucket.append(now)


def _merge_facts(
    facts_primary: Iterable[Dict[str, Any]], facts_secondary: Iterable[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    combined = list(facts_primary) + list(facts_secondary)
    if not combined:
        return []
    last_index: Dict[str, int] = {}
    keys: List[str] = []
    for idx, fact in enumerate(combined):
        key = (fact.get("url") or fact.get("citation_id") or str(idx)).lower()
        last_index[key] = idx
        keys.append(key)
    merged: List[Dict[str, Any]] = []
    for idx, fact in enumerate(combined):
        key = keys[idx]
        if last_index[key] != idx:
            continue
        merged.append(fact)
        if len(merged) >= settings.retriever_fact_cap:
            break
    return merged


def _facts_to_contexts(facts: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    contexts: List[Dict[str, Any]] = []
    for fact in facts or []:
        if not isinstance(fact, dict):
            continue
        context: Dict[str, Any] = {}
        url_value = fact.get("source_url") or fact.get("url") or fact.get("link")
        if isinstance(url_value, str):
            url_clean = url_value.strip()
            if url_clean:
                context["source_url"] = url_clean
        title_value = fact.get("title") or fact.get("source_title") or fact.get("name")
        if isinstance(title_value, str):
            title_clean = title_value.strip()
            if title_clean:
                context["title"] = title_clean
        snippet_value = fact.get("snippet") or fact.get("text") or fact.get("content")
        if isinstance(snippet_value, str):
            snippet_clean = snippet_value.strip()
            if snippet_clean:
                context["snippet"] = snippet_clean
        citation_value = fact.get("citation_id")
        if isinstance(citation_value, str):
            citation_clean = citation_value.strip()
            if citation_clean:
                context["citation_id"] = citation_clean
        if context:
            contexts.append(context)
        if len(contexts) >= settings.retriever_fact_cap:
            break
    return contexts


def _build_meta(
    *,
    intent: str,
    intent_info: Dict[str, Any],
    latencies: Dict[str, int],
    plan: Optional[Dict[str, Any]],
    context_note: str,
    oracle_result,
    hop_count: int,
    sources_count: int,
    k: int,
    extra_result=None,
    final_facts: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    meta: Dict[str, Any] = {
        "intent": intent,
        "latency_ms": latencies.get("total_ms", 0),
        "latency_breakdown": latencies,
        "gemini_first": True,
        "gemini": {"intent": intent_info},
        "retriever": {
            "context_note": context_note,
            "candidates": oracle_result.candidates,
            "deduped": oracle_result.deduped,
            "facts_returned": len(final_facts or []),
            "latency_ms": oracle_result.latency_ms,
        },
        "hop_count": hop_count,
        "show_debug_block": settings.show_debug_block,
        "k": k,
    }

    if plan is not None:
        meta["plan"] = plan

    if extra_result is not None:
        meta["retriever"]["hop2"] = {
            "context_note": extra_result.context_note,
            "candidates": extra_result.candidates,
            "deduped": extra_result.deduped,
            "latency_ms": extra_result.latency_ms,
        }

    if settings.show_debug_block:
        meta["debug"] = {
            "facts": final_facts or oracle_result.facts,
            "raw_retriever_facts": oracle_result.raw_facts,
            "plan_raw": (plan or {}).get("raw") if isinstance(plan, dict) else None,
        }

    observer.record(
        {
            "event": "ask2",
            "intent": intent,
            "question": intent_info.get("question") if isinstance(intent_info, dict) else None,
            "filters_applied": (plan or {}).get("filters") if isinstance(plan, dict) else {},
            "K_in": (plan or {}).get("k") if isinstance(plan, dict) else settings.oracle_knn_k,
            "K_after_dedup": len(oracle_result.facts),
            "rerank": "gemini",
            "final_sources_count": sources_count,
            "latency_ms": latencies.get("total_ms", 0),
            "latency_breakdown": latencies,
            "gemini": "answer" if sources_count else "plan",
            "hop_count": hop_count,
        },
    )

    return meta


def _record_smalltalk(question: str, intent_info: Dict[str, Any], latencies: Dict[str, int]) -> None:
    total_ms = latencies.get("total_ms", 0)
    observer.record(
        {
            "event": "ask2",
            "intent": "SMALL_TALK",
            "question": question,
            "filters_applied": {},
            "K_in": 0,
            "K_after_dedup": 0,
            "rerank": "gemini",
            "final_sources_count": 0,
            "latency_ms": total_ms,
            "latency_breakdown": latencies,
            "gemini": "smalltalk",
            "hop_count": 0,
        }
    )


def run_pipeline(
    question: str,
    *,
    k: int,
    client_ip: Optional[str] = None,
) -> Dict[str, Any]:
    """Execute the Gemini-first orchestration and return the response payload."""

    sanitized_q = (question or "").strip()
    ip = (client_ip or "unknown").strip() or "unknown"

    if not settings.gemini_first_enabled:
        raise GeminiUnavailableError("gemini_first_disabled")

    if not sanitized_q:
        return {
            "answer": "Please share a SustainaCore question so I can help.",
            "sources": [],
            "contexts": [],
            "meta": {"intent": "EMPTY", "k": k, "show_debug_block": settings.show_debug_block},
        }

    try:
        _enforce_rate_limit(ip)
    except RateLimitError as exc:
        observer.record(
            {
                "event": "rate_limited",
                "ip": ip,
                "intent": "UNKNOWN",
                "question": sanitized_q,
                "final_sources_count": 0,
                "gemini": "none",
            }
        )
        raise exc

    latencies: Dict[str, int] = {}
    t0 = time.time()

    intent_start = time.time()
    intent_info = gateway.classify_intent(sanitized_q)
    latencies["intent_ms"] = int((time.time() - intent_start) * 1000)
    intent = intent_info.get("intent", "INFO_REQUEST")

    if intent == "SMALL_TALK":
        small_start = time.time()
        reply = gateway.compose_small_talk(sanitized_q)
        latencies["gemini_smalltalk_ms"] = int((time.time() - small_start) * 1000)
        total_ms = int((time.time() - t0) * 1000)
        latencies["total_ms"] = total_ms
        _record_smalltalk(sanitized_q, intent_info, latencies)
        return {
            "answer": reply,
            "sources": [],
            "contexts": [],
            "meta": {
                "intent": intent,
                "latency_ms": total_ms,
                "latency_breakdown": latencies,
                "gemini_first": True,
                "gemini": {"intent": intent_info},
                "hop_count": 0,
                "show_debug_block": settings.show_debug_block,
                "k": k,
            },
        }

    plan_start = time.time()
    plan = gateway.plan_retrieval(sanitized_q)
    latencies["plan_ms"] = int((time.time() - plan_start) * 1000)
    plan_k = int(plan.get("k") or settings.oracle_knn_k)
    filters = plan.get("filters") or {}
    variants = plan.get("query_variants") or [sanitized_q]

    retrieval_start = time.time()
    oracle_result = retriever.retrieve(filters, variants, plan_k, hop_count=1)
    latencies["oracle_ms"] = oracle_result.latency_ms
    facts = oracle_result.facts
    context_note = oracle_result.context_note
    hop_count = 1

    extra_result = None
    if settings.allow_hop2 and plan.get("hop2") and len(facts) < settings.retriever_fact_cap:
        hop_plan = plan["hop2"]
        hop_variants = hop_plan.get("query_variants") or []
        hop_filters = hop_plan.get("filters") or {}
        if hop_variants:
            extra_result = retriever.retrieve(hop_filters, hop_variants, plan_k, hop_count=2)
            latencies["oracle_hop2_ms"] = extra_result.latency_ms
            facts = _merge_facts(facts, extra_result.facts)
            hop_count = 2
            context_note = context_note + "\n-- hop2 --\n" + extra_result.context_note

    retrieval_elapsed = int((time.time() - retrieval_start) * 1000)
    latencies["oracle_total_ms"] = retrieval_elapsed

    final_facts = facts[: settings.retriever_fact_cap]
    retriever_payload = {
        "facts": final_facts,
        "context_note": context_note,
    }

    compose_start = time.time()
    composed = gateway.compose_answer(sanitized_q, retriever_payload, plan, hop_count)
    latencies["gemini_compose_ms"] = int((time.time() - compose_start) * 1000)

    answer_text = composed.get("answer", "").strip()
    sources_list = composed.get("sources") or []
    total_ms = int((time.time() - t0) * 1000)
    latencies["total_ms"] = total_ms

    if not answer_text:
        answer_text = "I’m sorry, I couldn’t generate an answer from the retrieved facts."

    answer_text = _strip_sources_block(answer_text)

    if not isinstance(sources_list, list):
        sources_list = []

    contexts = _facts_to_contexts(final_facts)

    meta = _build_meta(
        intent=intent,
        intent_info={**intent_info, "question": sanitized_q},
        latencies=latencies,
        plan=plan,
        context_note=context_note,
        oracle_result=oracle_result,
        hop_count=hop_count,
        sources_count=len(sources_list),
        k=k,
        extra_result=extra_result,
        final_facts=final_facts,
    )
    meta.setdefault("plan", plan)
    meta["gemini"]["compose"] = composed.get("raw")

    compose_metrics = gateway.last_meta
    if compose_metrics:
        meta.setdefault("gemini", {})["compose_meta"] = compose_metrics

    return {
        "answer": answer_text,
        "sources": sources_list[: settings.retriever_fact_cap],
        "contexts": contexts,
        "meta": meta,
    }


_SOURCE_BLOCK_RE = re.compile(r"(?:\r?\n){1,}\s*Sources?:.*$", re.IGNORECASE | re.DOTALL)


def _strip_sources_block(answer: str) -> str:
    if not isinstance(answer, str) or not answer:
        return ""
    return _SOURCE_BLOCK_RE.sub("", answer).strip()


__all__ = [
    "GeminiUnavailableError",
    "RateLimitError",
    "run_pipeline",
]

