"""Adapters for APEX chat compatibility."""

from __future__ import annotations

import math
import os
from typing import Any, Dict, Iterable, List, Optional, Tuple

__all__ = [
    "SC_RAG_FAIL_OPEN",
    "SC_RAG_MIN_CONTEXTS",
    "SC_RAG_MIN_SCORE",
    "normalize_response",
]

_UNSURE_PREFIXES = (
    "i'm sorry",
    "i’m sorry",
    "i am sorry",
    "i do not",
    "i don't",
    "i cannot",
    "i can't",
    "unable to",
    "i could not",
    "i couldn’t",
)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    raw = raw.strip().lower()
    if raw in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if raw in {"0", "false", "f", "no", "n", "off"}:
        return False
    return default


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


SC_RAG_MIN_SCORE = _env_float("SC_RAG_MIN_SCORE", 0.55)
SC_RAG_MIN_CONTEXTS = max(1, _env_int("SC_RAG_MIN_CONTEXTS", 3))
SC_RAG_FAIL_OPEN = _env_bool("SC_RAG_FAIL_OPEN", True)


def _normalize_score(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        score = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(score):
        return None
    return max(0.0, min(1.0, score))


def _extract_snippet(entry: Dict[str, Any]) -> str:
    for key in ("chunk_text", "snippet", "text", "content"):
        snippet = entry.get(key)
        if isinstance(snippet, str) and snippet.strip():
            return snippet.strip()
    return ""


def _dedupe_snippets(snippets: Iterable[Dict[str, Any]], limit: int = 5) -> Tuple[List[Dict[str, Any]], Optional[float]]:
    results: List[Dict[str, Any]] = []
    seen: set[Tuple[str, str]] = set()
    max_score: Optional[float] = None

    for entry in snippets:
        if not isinstance(entry, dict):
            continue
        title = str(entry.get("title") or entry.get("source_name") or "").strip()
        url = str(entry.get("source_url") or entry.get("url") or "").strip()
        score = _normalize_score(
            entry.get("score")
            or entry.get("similarity")
            or entry.get("confidence")
        )
        snippet = _extract_snippet(entry)
        key = (title.lower(), url.lower())
        if key in seen:
            continue
        seen.add(key)
        results.append(
            {
                "title": title,
                "url": url,
                "score": score,
                "snippet": snippet,
            }
        )
        if score is not None:
            max_score = max(score, max_score or score)
        if len(results) >= limit:
            break

    return results, max_score


def _synthesize_answer(snippets: List[Dict[str, Any]]) -> str:
    if not snippets:
        return ""
    top = snippets[0]
    snippet_text = top.get("snippet", "").strip()
    if snippet_text:
        sentence = snippet_text.split(". ", 1)[0].strip()
        if len(sentence) > 200:
            sentence = sentence[:197].rstrip() + "…"
        if sentence and not sentence.endswith("."):
            sentence += "."
        return f"{sentence} See sources."
    title = (top.get("title") or "Relevant Sustainacore context").strip()
    return f"{title} — see sources."


def normalize_response(
    payload: Optional[Dict[str, Any]],
    snippets: Optional[Iterable[Dict[str, Any]]],
    provider: str,
    confidence: Optional[float],
) -> Dict[str, Any]:
    base = dict(payload or {})
    snippets_iterable: Iterable[Dict[str, Any]] = snippets if snippets is not None else base.get("contexts", [])  # type: ignore[arg-type]
    normalized_snippets, detected_max_score = _dedupe_snippets(snippets_iterable)

    score_values = [entry["score"] for entry in normalized_snippets if entry.get("score") is not None]
    max_score = None
    if score_values:
        max_score = max(score_values)  # type: ignore[assignment]

    signal = False
    if max_score is not None and max_score >= SC_RAG_MIN_SCORE:
        signal = True
    if not signal and len(normalized_snippets) >= SC_RAG_MIN_CONTEXTS:
        signal = True

    raw_answer = str(
        base.get("answer")
        or base.get("final_answer")
        or base.get("message")
        or ""
    ).strip()

    answer = raw_answer
    if answer:
        lowered = answer.lower()
        if any(lowered.startswith(marker) for marker in _UNSURE_PREFIXES):
            answer = ""
    if not answer and signal:
        answer = _synthesize_answer(normalized_snippets)

    ok = bool(answer)
    answered = bool(answer)
    if signal:
        ok = True
        answered = True

    confidence_candidates = [value for value in (confidence, detected_max_score, max_score) if isinstance(value, (int, float))]
    computed_confidence = confidence_candidates[0] if confidence_candidates else 0.0
    computed_confidence = max(0.0, min(1.0, float(computed_confidence)))
    if signal and computed_confidence < 0.6:
        computed_confidence = max(0.6, computed_confidence, max_score or 0.0)
    computed_confidence = max(0.0, min(1.0, computed_confidence))

    contexts_out = [dict(entry) for entry in normalized_snippets]
    sources_out = [dict(entry) for entry in normalized_snippets]
    citations_out = [dict(entry) for entry in normalized_snippets]

    result = dict(base)
    result.update(
        {
            "answer": answer,
            "final_answer": answer,
            "message": answer,
            "ok": ok,
            "answered": answered,
            "confidence": computed_confidence,
            "provider": provider,
            "error": base.get("error"),
            "contexts": contexts_out,
            "sources": sources_out,
            "citations": citations_out,
        }
    )

    if result["error"] is None and base.get("error") is None:
        result["error"] = None

    if max_score is not None:
        result.setdefault("meta", {})
        if isinstance(result["meta"], dict):
            result["meta"].setdefault("top_score", max_score)

    return result
