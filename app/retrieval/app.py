codex/implement-gemini-first-orchestration-for-sustainacore-lrla9o
"""FastAPI facade that exposes the Gemini-first orchestration."""

from __future__ import annotations

import logging
from typing import Any, Dict, List
=======
import logging
import time
from collections import deque
from threading import Lock
from typing import Any, Dict, Iterable, List
main

from fastapi import FastAPI, HTTPException, Query, Request
from pydantic import BaseModel, Field

codex/implement-gemini-first-orchestration-for-sustainacore-lrla9o
from .service import GeminiUnavailableError, RateLimitError, run_pipeline
=======
from .gemini_gateway import gateway
from .observability import observer
from .oracle_retriever import retriever
main
from .settings import settings


LOGGER = logging.getLogger("ask2")
app = FastAPI()


class Answer(BaseModel):
    """Serialized shape returned to APEX callers."""

    answer: str
    sources: List[str] = Field(default_factory=list)
    meta: Dict[str, Any] = Field(default_factory=dict)
 codex/implement-gemini-first-orchestration-for-sustainacore-lrla9o


def _sanitize_k(value: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):  # pragma: no cover - FastAPI enforces type
        parsed = 4
    parsed = max(1, parsed)
    return min(parsed, 10)



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
            raise HTTPException(status_code=429, detail="rate_limited")
        bucket.append(now)


def _merge_facts(facts_primary: Iterable[Dict[str, Any]], facts_secondary: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
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
 main


@app.get("/healthz")
def healthz() -> Dict[str, Any]:
    return {"ok": True, "gemini_first": settings.gemini_first_enabled}


@app.get("/ask2", response_model=Answer)
async def ask2(request: Request, q: str = Query(""), k: int = Query(4)) -> Answer:
    question = (q or "").strip()
    client_ip = request.client.host if request.client else "unknown"
 codex/implement-gemini-first-orchestration-for-sustainacore-lrla9o
    sanitized_k = _sanitize_k(k)

    try:
        payload = run_pipeline(question, k=sanitized_k, client_ip=client_ip)
    except RateLimitError as exc:
        raise HTTPException(status_code=429, detail="rate_limited") from exc
    except GeminiUnavailableError:
        return Answer(
            answer="Gemini-first orchestration is temporarily disabled. Please retry soon.",
            sources=[],
            meta={"intent": "DISABLED", "k": sanitized_k, "show_debug_block": settings.show_debug_block},

    try:
        _enforce_rate_limit(client_ip)
    except HTTPException as exc:
        observer.record(
            {
                "event": "rate_limited",
                "ip": client_ip,
                "intent": "UNKNOWN",
                "question": question,
                "final_sources_count": 0,
                "gemini": "none",
            }
        )
        raise exc

    if not question:
        return Answer(
            answer="Please share a SustainaCore question so I can help.",
            sources=[],
            meta={"intent": "EMPTY", "k": k},
 main
        )
    except Exception as exc:  # pragma: no cover - defensive logging
        LOGGER.exception("Gemini-first pipeline failed", exc_info=exc)
        raise HTTPException(status_code=500, detail="gemini_pipeline_failure") from exc

    answer_text = str(payload.get("answer") or "").strip()
    sources_list = payload.get("sources") or []
    if not isinstance(sources_list, list):
        sources_list = []
    meta = payload.get("meta") or {}
    if not isinstance(meta, dict):
        meta = {}
    meta["k"] = sanitized_k


    if not settings.gemini_first_enabled:
        return Answer(
            answer="Gemini-first orchestration is temporarily disabled. Please retry soon.",
            sources=[],
            meta={"intent": "DISABLED", "k": k},
        )

    latencies: Dict[str, int] = {}
    t0 = time.time()

    intent_start = time.time()
    intent_info = gateway.classify_intent(question)
    latencies["intent_ms"] = int((time.time() - intent_start) * 1000)
    intent = intent_info.get("intent", "INFO_REQUEST")

    if intent == "SMALL_TALK":
        small_start = time.time()
        reply = gateway.compose_small_talk(question)
        latencies["gemini_smalltalk_ms"] = int((time.time() - small_start) * 1000)
        total_ms = int((time.time() - t0) * 1000)
        meta = {
            "intent": intent,
            "latency_ms": total_ms,
            "gemini_first": True,
            "gemini": {"intent": intent_info},
            "latency_breakdown": latencies,
        }
        observer.record(
            {
                "event": "ask2",
                "intent": intent,
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
        return Answer(answer=reply, sources=[], meta=meta)

    plan_start = time.time()
    plan = gateway.plan_retrieval(question)
    latencies["plan_ms"] = int((time.time() - plan_start) * 1000)
    plan_k = int(plan.get("k") or settings.oracle_knn_k)
    filters = plan.get("filters") or {}
    variants = plan.get("query_variants") or [question]

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
            context_note = (
                context_note
                + "\n-- hop2 --\n"
                + extra_result.context_note
            )

    retrieval_elapsed = int((time.time() - retrieval_start) * 1000)
    latencies["oracle_total_ms"] = retrieval_elapsed

    final_facts = facts[: settings.retriever_fact_cap]
    retriever_payload = {
        "facts": final_facts,
        "context_note": context_note,
    }

    compose_start = time.time()
    composed = gateway.compose_answer(question, retriever_payload, plan, hop_count)
    latencies["gemini_compose_ms"] = int((time.time() - compose_start) * 1000)

    answer_text = composed.get("answer", "").strip()
    sources_list = composed.get("sources") or []
    total_ms = int((time.time() - t0) * 1000)
    latencies["total_ms"] = total_ms

    meta: Dict[str, Any] = {
        "intent": intent,
        "plan": plan,
        "latency_ms": total_ms,
        "latency_breakdown": latencies,
        "retriever": {
            "context_note": context_note,
            "candidates": oracle_result.candidates,
            "deduped": oracle_result.deduped,
            "facts_returned": len(final_facts),
            "latency_ms": oracle_result.latency_ms,
        },
        "hop_count": hop_count,
        "gemini": {"intent": intent_info, "compose": composed.get("raw")},
        "show_debug_block": settings.show_debug_block,
    }

    if extra_result is not None:
        meta["retriever"]["hop2"] = {
            "context_note": extra_result.context_note,
            "candidates": extra_result.candidates,
            "deduped": extra_result.deduped,
            "latency_ms": extra_result.latency_ms,
        }
    if settings.show_debug_block:
        meta["debug"] = {
            "facts": retriever_payload["facts"],
            "raw_retriever_facts": oracle_result.raw_facts,
            "plan_raw": plan.get("raw"),
            "gemini_raw": composed.get("raw"),
        }

    observer.record(
        {
            "event": "ask2",
            "intent": intent,
            "question": question,
            "filters_applied": filters,
            "K_in": plan_k,
            "K_after_dedup": len(final_facts),
            "rerank": "gemini",
            "final_sources_count": len(sources_list),
            "latency_ms": total_ms,
            "latency_breakdown": latencies,
            "gemini": "answer",
            "hop_count": hop_count,
        }
    )

 main
    return Answer(answer=answer_text, sources=sources_list, meta=meta)
