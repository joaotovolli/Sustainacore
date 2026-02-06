"""Shared Gemini-first orchestration helpers used by multiple entrypoints."""

from __future__ import annotations

import json
import logging
import os
import re
import time
from collections import deque
from threading import Lock
from types import SimpleNamespace
from typing import Any, Dict, Iterable, List, Optional

from .gemini_gateway import gateway, _format_final_answer, _resolve_source_url
from .observability import observer
from .oracle_retriever import retriever
from .settings import settings
from .quality_guards import (
    infer_source_type_filters,
    is_greeting_or_thanks,
    is_low_information,
    should_abstain,
    clarify_answer,
    smalltalk_answer,
)


LOGGER = logging.getLogger("gemini-service")


def _needs_ranked_results(question: str) -> bool:
    lowered = (question or "").lower()
    return any(term in lowered for term in ("top", "rank", "highest", "lowest", "leaders", "list"))


def _needs_unfiltered_results(question: str) -> bool:
    lowered = (question or "").lower()
    return any(term in lowered for term in ("in the index", "in the tech100", "included", "constituent", "member of"))


class RateLimitError(Exception):
    """Raised when the caller exceeds the configured rate limit."""

    def __init__(self, detail: str = "rate_limited") -> None:
        super().__init__(detail)
        self.detail = detail


class GeminiUnavailableError(Exception):
    """Raised when Gemini-first orchestration is disabled or fails hard."""


_RATE_BUCKETS: Dict[str, deque] = {}
_RATE_LOCK = Lock()
_NO_FACTS_FALLBACK = (
    "**Answer**\n"
    "I could not find enough SustainaCore context to answer this question.\n\n"
    "**Key facts**\n"
    "- No high-confidence facts were retrieved for this query.\n\n"
    "**Sources**\n"
    "1. SustainaCore — https://sustainacore.org"
)


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

def _dedupe_contexts(contexts: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen: set[str] = set()
    deduped: List[Dict[str, Any]] = []
    for ctx in contexts or []:
        if not isinstance(ctx, dict):
            continue
        key = (str(ctx.get("source_url") or "") or str(ctx.get("doc_id") or "")).strip().lower()
        if key and key in seen:
            continue
        if key:
            seen.add(key)
        deduped.append(dict(ctx))
        if len(deduped) >= settings.retriever_max_facts:
            break
    return deduped


def _contexts_to_facts_for_service(contexts: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    facts: List[Dict[str, Any]] = []
    for idx, ctx in enumerate(contexts or []):
        if not isinstance(ctx, dict):
            continue
        facts.append(
            {
                "citation_id": ctx.get("doc_id") or f"CTX_{idx+1}",
                "title": ctx.get("title") or "",
                "source_name": ctx.get("source_name") or "",
                "source_url": ctx.get("source_url") or "",
                "snippet": ctx.get("chunk_text") or ctx.get("snippet") or "",
                "score": ctx.get("score"),
            }
        )
    return facts


def _oracle_search_variants(
    variants: Iterable[str], k: int, filters: Optional[Dict[str, Any]] = None
) -> SimpleNamespace:
    variants = [v for v in variants if isinstance(v, str) and v.strip()]
    total_contexts: List[Dict[str, Any]] = []
    total_latency = 0
    notes: List[str] = []
    mode: Optional[str] = None
    for variant in variants or []:
        result = retriever.retrieve(variant, k, filters=filters)
        total_latency += result.latency_ms
        total_contexts.extend(result.contexts)
        if result.note:
            notes.append(result.note)
        if mode is None:
            mode = result.mode
    deduped = _dedupe_contexts(total_contexts)
    facts = _contexts_to_facts_for_service(deduped)
    context_note = " | ".join(notes) if notes else (mode or "retrieval")
    return SimpleNamespace(
        latency_ms=total_latency,
        context_note=context_note,
        facts=facts,
        candidates=len(total_contexts),
        deduped=len(deduped),
        hop_count=1,
        raw_facts=deduped,
        mode=mode or "none",
    )



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
                context.setdefault("chunk_text", snippet_clean)
        citation_value = fact.get("citation_id")
        if isinstance(citation_value, str):
            citation_clean = citation_value.strip()
            if citation_clean:
                context["citation_id"] = citation_clean
        score_value = fact.get("score")
        try:
            score_float = float(score_value)
        except (TypeError, ValueError):
            score_float = None
        if score_float is not None:
            context["score"] = score_float
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

    if not sanitized_q:
        return {
            "answer": "Please share a SustainaCore question so I can help.",
            "sources": [],
            "contexts": [],
            "meta": {"intent": "EMPTY", "k": k, "show_debug_block": settings.show_debug_block},
        }

    # Deterministic small-talk / low-information bypass.
    # This prevents retrieval from returning irrelevant regulatory/company chunks for greetings.
    if is_greeting_or_thanks(sanitized_q):
        t0 = time.time()
        total_ms = int((time.time() - t0) * 1000)
        latencies = {"total_ms": total_ms}
        intent_info = {"intent": "SMALL_TALK", "confidence": "heuristic"}
        _record_smalltalk(sanitized_q, intent_info, latencies)
        return {
            "answer": smalltalk_answer(sanitized_q),
            "sources": [],
            "contexts": [],
            "meta": {
                "intent": "SMALL_TALK",
                "latency_ms": total_ms,
                "latency_breakdown": latencies,
                "gemini_first": False,
                "gemini": {"intent": intent_info},
                "hop_count": 0,
                "show_debug_block": settings.show_debug_block,
                "k": 0,
                "note": "smalltalk_bypass",
            },
        }

    if is_low_information(sanitized_q):
        t0 = time.time()
        total_ms = int((time.time() - t0) * 1000)
        latencies = {"total_ms": total_ms}
        observer.record(
            {
                "event": "ask2",
                "intent": "LOW_INFO",
                "question": sanitized_q,
                "filters_applied": {},
                "K_in": 0,
                "K_after_dedup": 0,
                "rerank": "none",
                "final_sources_count": 0,
                "latency_ms": total_ms,
                "latency_breakdown": latencies,
                "gemini": "none",
                "hop_count": 0,
            }
        )
        return {
            "answer": clarify_answer(sanitized_q),
            "sources": [],
            "contexts": [],
            "meta": {
                "intent": "LOW_INFO",
                "latency_ms": total_ms,
                "latency_breakdown": latencies,
                "gemini_first": False,
                "hop_count": 0,
                "show_debug_block": settings.show_debug_block,
                "k": 0,
                "note": "low_info_bypass",
            },
        }

    if not settings.gemini_first_enabled:
        raise GeminiUnavailableError("gemini_first_disabled")

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
    plan_k = max(plan_k, k)
    if _needs_ranked_results(sanitized_q):
        plan_k = max(plan_k, settings.retriever_fact_cap)
    filters = plan.get("filters") or {}
    variants = plan.get("query_variants") or [sanitized_q]
    if sanitized_q not in variants:
        variants = [sanitized_q] + list(variants)
    if _needs_ranked_results(sanitized_q):
        filters = {}
    if _needs_unfiltered_results(sanitized_q):
        filters = {}

    # If Gemini plan didn't constrain retrieval, apply a conservative heuristic
    # to reduce domination by large corpora like regulatory content.
    if not filters or not isinstance(filters, dict) or "source_type" not in filters:
        inferred = infer_source_type_filters(sanitized_q)
        if inferred:
            filters = dict(filters) if isinstance(filters, dict) else {}
            filters["source_type"] = inferred

    retrieval_start = time.time()
    oracle_result = _oracle_search_variants(variants, plan_k, filters)
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
            extra_result = _oracle_search_variants(hop_variants, plan_k, hop_filters)
            latencies["oracle_hop2_ms"] = extra_result.latency_ms
            facts = _merge_facts(facts, extra_result.facts)
            hop_count = 2
            context_note = context_note + "\n-- hop2 --\n" + extra_result.context_note

    retrieval_elapsed = int((time.time() - retrieval_start) * 1000)
    latencies["oracle_total_ms"] = retrieval_elapsed

    fact_cap = max(settings.retriever_fact_cap, 3)
    final_facts = facts[:fact_cap]
    raw_contexts = list(getattr(oracle_result, "raw_facts", []) or [])
    if extra_result is not None:
        raw_contexts = _dedupe_contexts(raw_contexts + list(getattr(extra_result, "raw_facts", []) or []))
    retriever_payload = {
        "facts": final_facts,
        "contexts": raw_contexts[:fact_cap],
        "context_note": context_note,
    }

    # Quality guard: abstain when retrieval looks low-confidence.
    # This is critical for nonsense/generic prompts where KNN always returns something.
    # Use the raw contexts (with score/title/snippet) as the signal.
    decision = should_abstain(sanitized_q, raw_contexts[:fact_cap])
    if decision.abstain:
        total_ms = int((time.time() - t0) * 1000)
        latencies["total_ms"] = total_ms
        observer.record(
            {
                "event": "ask2",
                "intent": intent,
                "question": sanitized_q,
                "filters_applied": filters,
                "K_in": plan_k,
                "K_after_dedup": oracle_result.deduped,
                "rerank": "none",
                "final_sources_count": 1,
                "latency_ms": total_ms,
                "latency_breakdown": latencies,
                "gemini": "abstain",
                "hop_count": hop_count,
                "abstain_reason": decision.reason,
                "abstain_best_score": decision.best_score,
                "abstain_overlap": decision.max_token_overlap,
            }
        )
        answer_text = (
            "**Answer**\n"
            "I couldn’t find relevant information on SustainaCore to answer that question yet.\n\n"
            "**Try**\n"
            "- Rephrasing with a ticker (e.g., “AAPL”) or “Tech100”.\n"
            "- Asking about a specific page or topic (regulation, news, or index performance).\n\n"
            "**Sources**\n"
            "1. SustainaCore — https://sustainacore.org\n"
        )
        return {
            "answer": answer_text,
            "sources": ["SustainaCore — https://sustainacore.org"],
            "contexts": [],
            "meta": {
                "intent": intent,
                "latency_ms": total_ms,
                "latency_breakdown": latencies,
                "gemini_first": True,
                "gemini": {"intent": intent_info, "compose": "abstain"},
                "hop_count": 0,
                "show_debug_block": settings.show_debug_block,
                "k": k,
                "note": f"abstain:{decision.reason}",
            },
        }

    compose_start = time.time()
    composed = gateway.compose_answer(sanitized_q, retriever_payload, plan, hop_count)
    latencies["gemini_compose_ms"] = int((time.time() - compose_start) * 1000)

    raw_answer = composed.get("answer")
    if isinstance(raw_answer, str):
        answer_text = raw_answer.strip()
    else:
        answer_text = str(raw_answer).strip() if raw_answer is not None else ""
    sources_list = composed.get("sources") or []
    total_ms = int((time.time() - t0) * 1000)
    latencies["total_ms"] = total_ms

    if os.getenv("ASK2_SYNTH_FALLBACK", "0") not in {"0", "false", "False"}:
        facts_list = retriever_payload.get("facts") or []
        fallback_lines = []
        for fact in facts_list[:4]:
            if not isinstance(fact, dict):
                continue
            title = (fact.get("title") or fact.get("source_name") or "").strip()
            snippet = (fact.get("snippet") or fact.get("chunk_text") or "").strip()
            if title and snippet:
                fallback_lines.append(f"- {title}: {snippet[:180]}")
            elif title:
                fallback_lines.append(f"- {title}")
            elif snippet:
                fallback_lines.append(f"- {snippet[:180]}")
        if (not answer_text or answer_text == "I’m sorry, I couldn’t generate an answer from the retrieved facts.") and fallback_lines:
            answer_text = _format_final_answer("", "", facts_list, None)
    if not answer_text:
        answer_text = "I’m sorry, I couldn’t generate an answer from the retrieved facts."

    answer_text = _strip_sources_block(answer_text)

    if not isinstance(sources_list, list):
        sources_list = []

    contexts = _facts_to_contexts(final_facts)

    def _is_plan_like(text: str) -> bool:
        clean = (text or "").strip()
        if not clean:
            return False
        if "query_variants" in clean and "filters" in clean:
            if clean.startswith("{"):
                try:
                    parsed = json.loads(clean)
                    if isinstance(parsed, dict) and {"query_variants", "k"} & set(parsed.keys()):
                        return True
                except Exception:
                    pass
            return True
        return False

    def _is_unusable(text: str) -> bool:
        lowered = (text or "").strip().lower()
        if not lowered:
            return True
        prefixes = (
            "i’m sorry",
            "i'm sorry",
            "i do not have",
            "i don't have",
        )
        return lowered.startswith(prefixes) or _is_plan_like(lowered)

    def _summarize_facts(facts: Iterable[Dict[str, Any]]) -> str:
        facts_list = [fact for fact in facts if isinstance(fact, dict)]
        if not facts_list:
            return ""
        return _format_final_answer("", "", facts_list, None)

    def _sources_from_facts(facts: Iterable[Dict[str, Any]]) -> List[str]:
        sources: List[str] = []
        seen: set[str] = set()
        for fact in facts or []:
            if not isinstance(fact, dict):
                continue
            title = (fact.get("title") or fact.get("source_name") or "").strip()
            url = (fact.get("source_url") or fact.get("url") or "").strip()
            if url.startswith(("local://", "file://", "internal://")):
                url = ""
            url = _resolve_source_url(title, url)
            if not url:
                continue
            label = title or "SustainaCore"
            key = (label.lower(), url.lower())
            if key in seen:
                continue
            seen.add(key)
            sources.append(f"{label} — {url}")
            if len(sources) >= settings.retriever_fact_cap:
                break
        return sources

    # Final answer selection with safe fallbacks
    facts_summary = _summarize_facts(final_facts)
    if _is_unusable(answer_text):
        answer_text = facts_summary or _NO_FACTS_FALLBACK
    if not isinstance(sources_list, list):
        sources_list = []
    sources_list = [str(item).strip() for item in sources_list if str(item).strip()]
    if not sources_list:
        sources_list = _sources_from_facts(final_facts)

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
    if "**Sources**" in answer:
        return answer
    return _SOURCE_BLOCK_RE.sub("", answer).strip()


__all__ = [
    "GeminiUnavailableError",
    "RateLimitError",
    "run_pipeline",
]
