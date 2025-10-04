"""Adapter wiring Oracle retrieval and Gemini composition for /ask2."""

from __future__ import annotations

import logging
import os
import re
import time
from typing import Any, Dict, List, Tuple

from app.retrieval.gemini_gateway import gateway as gemini_gateway
from app.retrieval.oracle_retriever import retriever as oracle_retriever

LOGGER = logging.getLogger("sustainacore.ask2.pipeline")
_SOURCE_BLOCK_RE = re.compile(r"(?:\r?\n){1,}\s*Sources?:.*$", re.IGNORECASE | re.DOTALL)


def _strip_sources_block(answer: str) -> str:
    if not isinstance(answer, str):
        return ""
    return _SOURCE_BLOCK_RE.sub("", answer).strip()


def _fs_backfill(question: str, top_k: int) -> List[Dict[str, Any]]:
    if os.getenv("FS_FALLBACK") != "1":
        return []
    try:
        from app.retrieval import fs_retriever  # type: ignore
    except Exception:
        return []
    try:
        return fs_retriever.search(question, top_k=top_k)
    except Exception:
        return []


def _facts_to_contexts(facts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    contexts: List[Dict[str, Any]] = []
    for fact in facts or []:
        if not isinstance(fact, dict):
            continue
        context: Dict[str, Any] = {}
        title = fact.get("title")
        if isinstance(title, str) and title.strip():
            context["title"] = title.strip()
        snippet = fact.get("snippet")
        if isinstance(snippet, str) and snippet.strip():
            context["snippet"] = snippet.strip()
        url = fact.get("url") or fact.get("source_url")
        if isinstance(url, str) and url.strip():
            context["source_url"] = url.strip()
        citation = fact.get("citation_id")
        if isinstance(citation, str) and citation.strip():
            context["citation_id"] = citation.strip()
        score = fact.get("score")
        if isinstance(score, (float, int)):
            context["score"] = float(score)
        if context:
            contexts.append(context)
    return contexts


def _contexts_to_facts(contexts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    facts: List[Dict[str, Any]] = []
    for idx, ctx in enumerate(contexts or []):
        if not isinstance(ctx, dict):
            continue
        facts.append(
            {
                "citation_id": ctx.get("citation_id") or f"FS_{idx+1}",
                "title": (ctx.get("title") or "Untitled excerpt"),
                "source_name": ctx.get("source_name") or "",
                "url": ctx.get("source_url"),
                "snippet": ctx.get("snippet") or "",
                "score": ctx.get("score"),
            }
        )
    return facts


def _build_sources(facts: List[Dict[str, Any]], limit: int) -> List[str]:
    sources: List[str] = []
    for fact in facts:
        if not isinstance(fact, dict):
            continue
        title = (fact.get("title") or "Untitled excerpt").strip()
        source_name = (fact.get("source_name") or "").strip()
        citation = (fact.get("citation_id") or "").strip()
        label_parts = [title]
        if source_name:
            label_parts.append(source_name)
        if citation:
            label_parts.append(f"[{citation}]")
        label = " — ".join(label_parts[:2]) if len(label_parts) >= 2 else label_parts[0]
        if citation and citation not in label:
            label = f"{label} [{citation}]"
        if label:
            sources.append(label)
        if len(sources) >= limit:
            break
    return sources


def _base_meta(routing: str, total_ms: int, oracle_ms: int, gemini_ms: int, result_meta: Dict[str, Any]) -> Dict[str, Any]:
    meta: Dict[str, Any] = {
        "routing": routing,
        "latency_ms": total_ms,
        "latency_breakdown": {
            "oracle_ms": oracle_ms,
            "gemini_ms": gemini_ms,
            "total_ms": total_ms,
        },
    }
    meta.update(result_meta)
    return meta


def ask2_pipeline_first(question: str, k: int, *, client_ip: str = "unknown") -> Tuple[Dict[str, Any], int]:
    sanitized_question = (question or "").strip()
    start = time.perf_counter()

    try:
        retrieval_start = time.perf_counter()
        result = oracle_retriever.retrieve({}, [sanitized_question], k, hop_count=1)
        oracle_ms = int((time.perf_counter() - retrieval_start) * 1000)
    except Exception as exc:
        total_ms = int((time.perf_counter() - start) * 1000)
        meta = _base_meta("gemini_first_fail", total_ms, 0, 0, {"error": str(exc)})
        return {"answer": "", "sources": [], "contexts": [], "meta": meta}, 200

    facts: List[Dict[str, Any]] = list(result.facts)
    if not facts:
        fs_contexts = _fs_backfill(sanitized_question, k)
        if fs_contexts:
            facts = _contexts_to_facts(fs_contexts)
        else:
            total_ms = int((time.perf_counter() - start) * 1000)
            meta = _base_meta(
                "gemini_first",
                total_ms,
                result.latency_ms,
                0,
                {
                    "retriever": {
                        "context_note": result.context_note,
                        "candidates": result.candidates,
                        "deduped": result.deduped,
                        "hop_count": result.hop_count,
                    },
                    "note": "no_contexts",
                },
            )
            return {"answer": "", "sources": [], "contexts": [], "meta": meta}, 200

    contexts = _facts_to_contexts(facts)

    retriever_meta = {
        "retriever": {
            "context_note": result.context_note,
            "candidates": result.candidates,
            "deduped": result.deduped,
            "hop_count": result.hop_count,
            "latency_ms": result.latency_ms,
        }
    }

    retriever_payload = {"facts": facts, "context_note": result.context_note}
    plan = {"filters": {}, "k": k, "query_variants": [sanitized_question] if sanitized_question else []}

    compose_start = time.perf_counter()
    compose = gemini_gateway.compose_answer(sanitized_question, retriever_payload, plan, result.hop_count)
    gemini_ms = int((time.perf_counter() - compose_start) * 1000)
    total_ms = int((time.perf_counter() - start) * 1000)

    gem_meta = gemini_gateway.last_meta
    if gem_meta:
        retriever_meta["gemini"] = gem_meta

    if compose:
        answer_text = _strip_sources_block(compose.get("answer", ""))
        sources = compose.get("sources") if isinstance(compose.get("sources"), list) else []
        meta = _base_meta("gemini_first", total_ms, result.latency_ms, gemini_ms, retriever_meta)
        return {"answer": answer_text, "sources": sources, "contexts": contexts, "meta": meta}, 200

    fallback_sources = _build_sources(facts, limit=3)
    fallback_answer = "I couldn’t generate a Gemini-composed answer right now. Please review the sourced contexts below."
    retriever_meta["note"] = "gemini_compose_failed"
    meta = _base_meta("gemini_first_fail", total_ms, result.latency_ms, gemini_ms, retriever_meta)
    return {"answer": fallback_answer, "sources": fallback_sources, "contexts": contexts, "meta": meta}, 200
