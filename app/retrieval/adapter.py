"""Gemini-first adapter that couples local embeddings with Oracle retrieval."""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Tuple

from app.rag.gemini_cli import (
    GeminiCLIError,
    gemini_call,
    _extract_text as _gemini_extract_text,
)

from . import oracle_retriever

LOGGER = logging.getLogger("app.retrieval.adapter")

# Legacy compatibility hook: older tests monkeypatch run_pipeline
run_pipeline = None  # type: ignore

_DEFAULT_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
_FALLBACK_ANSWER = "Gemini is momentarily unavailable, but I saved the supporting contexts below."


def _strip_sources_block(answer: str) -> str:
    if not isinstance(answer, str):
        return ""
    text = answer.strip()
    if not text:
        return ""
    lowered = text.lower()
    marker = "sources:"
    idx = lowered.find(marker)
    if idx != -1:
        text = text[:idx].rstrip()
    return text


def _build_sources(contexts: List[Dict[str, Any]]) -> List[str]:
    sources: List[str] = []
    seen: set = set()
    for ctx in contexts:
        if not isinstance(ctx, dict):
            continue
        doc_id = str(ctx.get("doc_id") or "").strip()
        title = str(ctx.get("title") or "").strip()
        source_url = str(ctx.get("source_url") or "").strip()
        label = title or doc_id or source_url
        if not label:
            continue
        display = label
        if source_url and source_url not in label:
            display = f"{label} - {source_url}"
        key = (display,)
        if key in seen:
            continue
        seen.add(key)
        sources.append(display)
    return sources


def _compose_with_gemini(question: str, contexts: List[Dict[str, Any]], *, model: str | None = None) -> Dict[str, Any]:
    effective_model = model or _DEFAULT_MODEL
    payload = {
        "question": question.strip(),
        "contexts": contexts,
        "instructions": {
            "style": "≤3 sentences plus optional bullets",
            "citations": "reference contexts by their doc_id where possible",
            "no_inline_sources_heading": True,
        },
    }
    prompt = (
        "You are SustainaCore's Gemini composer.\n"
        "Use ONLY the provided contexts to answer the user's question.\n"
        "Do not invent facts.\n"
        "Return JSON with keys answer (string) and sources (array of short source strings).\n"
        "Do not prefix the answer with \"Sources:\" or similar lists.\n"
        f"Payload: {json.dumps(payload, ensure_ascii=False)}"
    )

    response = gemini_call(prompt, model=effective_model)
    if not response:
        raise GeminiCLIError("gemini_cli_no_output", stderr="")

    try:
        parsed = json.loads(response)
    except json.JSONDecodeError:
        parsed = {"answer": response}

    if isinstance(parsed, dict) and "answer" not in parsed:
        extracted = _gemini_extract_text(parsed)
        if not extracted:
            candidates = parsed.get("candidates")
            if isinstance(candidates, list):
                for cand in candidates:
                    if not isinstance(cand, dict):
                        continue
                    content = cand.get("content")
                    if isinstance(content, dict):
                        parts = content.get("parts")
                        if isinstance(parts, list):
                            for part in parts:
                                text_val = part.get("text") if isinstance(part, dict) else None
                                if isinstance(text_val, str) and text_val.strip():
                                    extracted = text_val
                                    break
                        if extracted:
                            break
                if not extracted:
                    # Fall back to any "response" field the CLI might provide.
                    response_text = parsed.get("response")
                    if isinstance(response_text, str) and response_text.strip():
                        extracted = response_text
        if extracted:
            text = extracted.strip()
            if text.startswith("```") and text.endswith("```"):
                text = text.strip("`").strip()
            if text.lower().startswith("json"):
                text = text[4:].lstrip()
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError:
                parsed = {"answer": text}

    if not isinstance(parsed, dict):
        parsed = {}
    return parsed


def ask2_pipeline_first(question: str, k: int, *, client_ip: str = "unknown") -> Tuple[Dict[str, Any], int]:
    """Run Oracle retrieval first, then compose with Gemini once."""

    contexts = oracle_retriever.retrieve(question, k, filters={})
    base_payload: Dict[str, Any] = {
        "answer": "",
        "sources": [],
        "contexts": contexts,
        "meta": {
            "routing": "gemini_first",
            "retriever": "oracle",
            "client_ip": client_ip or "unknown",
            "k": k,
        },
    }

    if not contexts:
        base_payload["meta"]["note"] = "no_contexts"
        return base_payload, 200

    try:
        composed = _compose_with_gemini(question, contexts)
    except Exception as exc:
        LOGGER.exception("gemini_compose_failed", exc_info=exc)
        payload = dict(base_payload)
        payload["answer"] = _FALLBACK_ANSWER
        payload["sources"] = _build_sources(contexts)
        payload["meta"]["routing"] = "gemini_first_fail"
        payload["meta"]["error"] = getattr(exc, "args", ["compose_failed"])[0]
        return payload, 200

    answer_text = _strip_sources_block(str(composed.get("answer") or ""))
    sources_raw = composed.get("sources")
    if isinstance(sources_raw, list):
        sources_list = []
        for item in sources_raw:
            if isinstance(item, str) and item.strip():
                sources_list.append(item.strip())
        if not sources_list:
            sources_list = _build_sources(contexts)
    else:
        sources_list = _build_sources(contexts)

    payload = dict(base_payload)
    payload["answer"] = answer_text
    payload["sources"] = sources_list
    payload["meta"]["model"] = os.getenv("GEMINI_MODEL", _DEFAULT_MODEL)
    return payload, 200


__all__ = ["ask2_pipeline_first"]
