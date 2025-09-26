"""Routing helpers for the /ask2 facade."""
from __future__ import annotations

import json
import os
import re
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from .gemini_cli import gemini_call

DEFAULT_K = 4
MAX_K = 10
SMALLTALK_WORD_LIMIT = 8
LOW_OK = float(os.getenv("RAG_LOW_OK", "0.55"))
HIGH_OK = float(os.getenv("RAG_HIGH_OK", "0.70"))
GEMINI_TIMEOUT = float(os.getenv("RAG_GEMINI_TIMEOUT", "8"))
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-1.5-pro")


def _flag_enabled(value: Optional[str], *, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() not in {"0", "false", "off", "no"}


SMALL_TALK_ENABLED = _flag_enabled(os.getenv("SMALL_TALK"), default=True)
INLINE_SOURCES_ENABLED = _flag_enabled(os.getenv("INLINE_SOURCES"), default=False)
SIMILARITY_FLOOR_MODE = (os.getenv("SIMILARITY_FLOOR_MODE") or "monitor").strip().lower()
if SIMILARITY_FLOOR_MODE not in {"monitor", "enforce", "off"}:
    SIMILARITY_FLOOR_MODE = "monitor"


def _parse_floor(value: Optional[str], default: float) -> float:
    try:
        floor = float(value)
    except (TypeError, ValueError):
        floor = default
    if floor < 0.0:
        floor = 0.0
    if floor > 1.0:
        floor = 1.0
    return round(floor, 4)


SIMILARITY_FLOOR = _parse_floor(os.getenv("SIMILARITY_FLOOR"), default=0.35)

SMALLTALK_RE = re.compile(
    r"^\s*(?:hi|hello|hey|hola|howdy|yo|sup|thanks|thank you|thank you so much|thanks a lot|"
    r"thank you very much|good morning|good afternoon|good evening|good day|bye|goodbye|"
    r"see you|see ya|appreciate it|much appreciated|cheers)(?:[\s,.!]+(?:there|team|everyone|folks|all))?[\s,.!]*$",
    re.IGNORECASE,
)
SMALLTALK_WORDS = {
    "hi",
    "hello",
    "hey",
    "hola",
    "howdy",
    "yo",
    "sup",
    "thanks",
    "thank",
    "you",
    "so",
    "much",
    "very",
    "morning",
    "afternoon",
    "evening",
    "day",
    "good",
    "bye",
    "goodbye",
    "see",
    "ya",
    "later",
    "there",
    "team",
    "everyone",
    "folks",
    "all",
    "appreciate",
    "appreciated",
    "it",
    "cheers",
}

NO_HIT_FALLBACK = (
    "I couldn’t find Sustainacore documents for that yet. If you can share the organization or "
    "company name, the ESG or TECH100 topic, and the report or year you care about, I can take "
    "another look."
)
LOW_CONF_FALLBACK = (
    "The Sustainacore matches I found are inconclusive. If you can provide the organization/company," \
    " ESG or TECH100 topic, and the report/year, I can verify the answer."
)
SMALLTALK_FALLBACK = (
    "Hello! I’m the Sustainacore assistant. Let me know any Sustainacore, ESG, or TECH100 "
    "question and I’ll dig in."
)
EMPTY_QUERY_MESSAGE = (
    "Please share a Sustainacore question or topic so I can help."
)

INLINE_SECTION_HEADER_RE = re.compile(r"^(sources?|why\s+this\s+answer)\s*[:：]", re.I)
INLINE_BULLET_RE = re.compile(r"^\s*[-•]", re.UNICODE)

VectorSearchFn = Callable[[str, int], List[Dict[str, Any]]]
GeminiFn = Callable[[str, Optional[float], Optional[str]], Optional[str]]


def _sanitize_k(value: Any) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = DEFAULT_K
    parsed = max(1, parsed)
    return min(parsed, MAX_K)


def _strip(text: Optional[str]) -> str:
    return (text or "").strip()


def _is_smalltalk(query: str) -> bool:
    words = [w.lower() for w in re.split(r"\s+", query.strip()) if w]
    if not words or len(words) > SMALLTALK_WORD_LIMIT:
        return False
    if SMALLTALK_RE.match(query):
        return True
    return all(word.strip(".,!?") in SMALLTALK_WORDS for word in words)


def _format_sources(hits: Sequence[Dict[str, Any]]) -> List[str]:
    formatted: List[str] = []
    for idx, hit in enumerate(hits[:3], start=1):
        title = _strip(hit.get("title") or hit.get("name") or hit.get("id") or f"Document {idx}")
        url = _strip(hit.get("url") or hit.get("link") or hit.get("source_url"))
        if url:
            formatted.append(f"Source {idx}: {title} ({url})")
        else:
            formatted.append(f"Source {idx}: {title}")
    return formatted


def _coerce_score(value: Any) -> Optional[float]:
    try:
        score = float(value)
    except (TypeError, ValueError):
        return None
    if score != score:  # NaN check
        return None
    if score > 1.0:
        if score <= 100:
            score = score / 100.0
        else:
            score = 1.0
    if score < 0.0:
        score = 0.0
    return round(score, 4)


def _normalize_hit(hit: Dict[str, Any]) -> Dict[str, Any]:
    snippet = _strip(
        hit.get("snippet")
        or hit.get("text")
        or hit.get("chunk_text")
        or hit.get("content")
        or hit.get("summary")
    )
    if snippet:
        snippet = re.sub(r"\s+", " ", snippet)[:600]
    score = _coerce_score(hit.get("score") or hit.get("similarity") or hit.get("confidence"))
    if score is None:
        dist = hit.get("dist") or hit.get("distance")
        if dist is not None:
            try:
                score = _coerce_score(1.0 - float(dist))
            except (TypeError, ValueError):
                score = None
    normalized = {
        "title": hit.get("title") or hit.get("name") or hit.get("id") or "Source",
        "url": hit.get("url") or hit.get("link") or hit.get("source_url") or "",
        "snippet": snippet,
        "score": score,
    }
    return normalized


def vector_search(query: str, k: int) -> List[Dict[str, Any]]:
    """Retrieve candidate passages via the existing vector endpoint."""
    query = _strip(query)
    if not query:
        return []
    top_k = _sanitize_k(k)
    url = os.getenv("RAG_VECTOR_URL", "http://127.0.0.1:8080/ask2_direct")
    timeout = float(os.getenv("RAG_VECTOR_TIMEOUT", "6"))
    try:
        import requests  # type: ignore
    except Exception:
        return []
    try:
        resp = requests.get(url, params={"q": query, "k": top_k}, timeout=timeout)
    except Exception:
        return []
    if resp.status_code != 200:
        return []
    try:
        data = resp.json()
    except Exception:
        return []
    candidates: Iterable[Any]
    if isinstance(data, dict):
        candidates = data.get("sources") or data.get("results") or data.get("chunks") or []
    elif isinstance(data, list):
        candidates = data
    else:
        candidates = []
    hits: List[Dict[str, Any]] = []
    for item in candidates:
        if isinstance(item, dict):
            hits.append(_normalize_hit(item))
        elif isinstance(item, str):
            hits.append({"title": item[:80] or "Source", "url": "", "snippet": item[:600], "score": None})
        if len(hits) >= top_k:
            break
    hits.sort(key=lambda h: h.get("score") or 0.0, reverse=True)
    return hits


def _call_gemini(prompt: str, gemini_fn: Optional[GeminiFn]) -> Tuple[str, bool]:
    fn = gemini_fn or gemini_call
    response = fn(prompt, GEMINI_TIMEOUT, GEMINI_MODEL)
    if response:
        text = response.strip()
        if text:
            return text, True
    return "", False


def _build_source_payload(hits: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    payload = []
    for idx, hit in enumerate(hits[:3], start=1):
        payload.append(
            {
                "source": f"Source {idx}",
                "title": _strip(hit.get("title")),
                "url": _strip(hit.get("url")),
                "snippet": _strip(hit.get("snippet")),
                "score": hit.get("score"),
            }
        )
    return payload


def _smalltalk_answer(query: str, gemini_fn: Optional[GeminiFn]) -> Tuple[str, bool]:
    prompt = (
        "You are the friendly Sustainacore assistant. The user greeted you with:\n"
        f"{json.dumps(query)}\n"
        "Reply with one or two short sentences welcoming them and inviting them to ask about "
        "Sustainacore, TECH100, or ESG insights. Keep it warm and professional."
    )
    answer, used = _call_gemini(prompt, gemini_fn)
    if answer:
        return answer, used
    return SMALLTALK_FALLBACK, False


def _no_hit_answer(query: str, gemini_fn: Optional[GeminiFn]) -> Tuple[str, bool]:
    prompt = (
        "You are the Sustainacore assistant. We found no matching Sustainacore content for the "
        "question below.\n"
        "Write a brief reply (2 sentences) saying we do not have grounded information yet and "
        "encourage the user to share the organization/company, the ESG or TECH100 topic, and the "
        "report or year so we can search again. Be helpful and optimistic.\n"
        f"Question: {json.dumps(query)}"
    )
    answer, used = _call_gemini(prompt, gemini_fn)
    if answer:
        return answer, used
    return NO_HIT_FALLBACK, False


def _low_conf_answer(query: str, hits: Sequence[Dict[str, Any]], gemini_fn: Optional[GeminiFn]) -> Tuple[str, bool]:
    payload = _build_source_payload(hits)
    prompt = (
        "You are the Sustainacore assistant. The retrieved passages below were low confidence "
        "matches for the user question.\n"
        "Respond with two concise sentences explaining that the answer is inconclusive, invite the "
        "user to provide the organization/company, ESG or TECH100 topic, and report/year, and "
        "mention that the listed sources may be relevant.\n"
        f"Question: {json.dumps(query)}\n"
        f"Candidate sources: {json.dumps(payload, ensure_ascii=False)}"
    )
    answer, used = _call_gemini(prompt, gemini_fn)
    if answer:
        return answer, used
    candidates = "; ".join(
        f"Source {idx + 1}: {_strip(hit.get('title')) or 'Source'}" for idx, hit in enumerate(hits[:3])
    )
    fallback = (
        f"{LOW_CONF_FALLBACK} Possible matches: {candidates}."
        if candidates
        else LOW_CONF_FALLBACK
    )
    return fallback, False


def _high_conf_prompt(query: str, hits: Sequence[Dict[str, Any]]) -> str:
    lines = [
        "You are the Sustainacore assistant.",
        "Use only the following Sustainacore passages to answer the question.",
        "Cite supporting evidence in-line using [Source N] matching the provided numbering.",
        "Write at most three sentences and stay factual.",
        f"Question: {json.dumps(query)}",
        "Sources:",
    ]
    for idx, hit in enumerate(hits[:3], start=1):
        snippet = _strip(hit.get("snippet"))
        title = _strip(hit.get("title"))
        url = _strip(hit.get("url"))
        entry = {
            "id": f"Source {idx}",
            "title": title,
            "url": url,
            "snippet": snippet,
        }
        lines.append(json.dumps(entry, ensure_ascii=False))
    lines.append("Answer:")
    return "\n".join(lines)


def _high_conf_answer(query: str, hits: Sequence[Dict[str, Any]], gemini_fn: Optional[GeminiFn]) -> Tuple[str, bool]:
    prompt = _high_conf_prompt(query, hits)
    answer, used = _call_gemini(prompt, gemini_fn)
    if answer:
        return answer, used
    snippets = [
        f"{_strip(hit.get('snippet'))} [Source {idx}]" for idx, hit in enumerate(hits[:3], start=1) if _strip(hit.get('snippet'))
    ]
    if not snippets:
        snippets = ["Relevant Sustainacore context is available. [Source 1]"]
    fallback = " ".join(snippets)
    return fallback.strip(), False


def _strip_inline_sections(text: str) -> str:
    if not text:
        return ""
    lines = text.splitlines()
    cleaned: List[str] = []
    skipping = False
    for line in lines:
        stripped = line.strip()
        if INLINE_SECTION_HEADER_RE.match(stripped):
            skipping = True
            continue
        if skipping:
            if not stripped or INLINE_BULLET_RE.match(stripped):
                continue
            skipping = False
        if not skipping:
            cleaned.append(line)
    return "\n".join(cleaned).strip()


def _finalize_answer(answer: str) -> str:
    text = (answer or "").strip()
    if not INLINE_SOURCES_ENABLED:
        text = _strip_inline_sections(text)
    return text


def route_ask2(
    query: str,
    k: Any = None,
    *,
    vector_fn: Optional[VectorSearchFn] = None,
    gemini_fn: Optional[GeminiFn] = None,
) -> Dict[str, Any]:
    """Main routing entry point used by the Flask facade."""

    q = _strip(query)
    sanitized_k = _sanitize_k(k)
    meta: Dict[str, Any] = {
        "routing": "",
        "top_score": None,
        "gemini_used": False,
        "k": sanitized_k,
        "small_talk_enabled": SMALL_TALK_ENABLED,
        "inline_sources_enabled": INLINE_SOURCES_ENABLED,
        "similarity_floor_mode": SIMILARITY_FLOOR_MODE,
        "similarity_floor": SIMILARITY_FLOOR,
    }

    if not q:
        meta.update({"routing": "empty"})
        return {"answer": _finalize_answer(EMPTY_QUERY_MESSAGE), "sources": [], "meta": meta}

    if SMALL_TALK_ENABLED and _is_smalltalk(q):
        answer, used = _smalltalk_answer(q, gemini_fn)
        meta.update({"routing": "smalltalk", "top_score": None, "gemini_used": used})
        return {"answer": _finalize_answer(answer), "sources": [], "meta": meta}

    search_fn = vector_fn or vector_search
    hits = search_fn(q, sanitized_k)
    sources = _format_sources(hits)
    top_score = hits[0].get("score") if hits else None
    meta["top_score"] = top_score

    floor_applicable = SIMILARITY_FLOOR_MODE in {"monitor", "enforce"}
    if floor_applicable and top_score is not None and top_score < SIMILARITY_FLOOR:
        meta.update(
            {
                "similarity_floor_triggered": True,
                "similarity_floor_value": SIMILARITY_FLOOR,
            }
        )
        if SIMILARITY_FLOOR_MODE == "enforce":
            hits = []
            sources = []
            top_score = None
            meta["top_score"] = None

    if not hits:
        answer, used = _no_hit_answer(q, gemini_fn)
        meta.update({"routing": "no_hit", "gemini_used": used})
        return {"answer": _finalize_answer(answer), "sources": [], "meta": meta}

    threshold = top_score or 0.0
    if threshold >= HIGH_OK:
        answer, used = _high_conf_answer(q, hits, gemini_fn)
        meta.update({"routing": "high_conf", "gemini_used": used})
        return {"answer": _finalize_answer(answer), "sources": sources, "meta": meta}

    answer, used = _low_conf_answer(q, hits, gemini_fn)
    meta.update({"routing": "low_conf", "gemini_used": used})
    return {"answer": _finalize_answer(answer), "sources": sources, "meta": meta}
