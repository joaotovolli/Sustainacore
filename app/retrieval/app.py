from __future__ import annotations

import json
import os
import re
import subprocess
from typing import Any, Dict, Iterable, List, Optional, Tuple

from fastapi import FastAPI, Query
from pydantic import BaseModel, Field


app = FastAPI()


@app.get("/healthz")
def healthz() -> Dict[str, bool]:
    return {"ok": True}


class Answer(BaseModel):
    answer: str
    sources: list[str] = Field(default_factory=list)
    meta: dict[str, Any] = Field(default_factory=dict)


Answer.model_rebuild()


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


LOW_OK = _env_float("RAG_LOW_OK", 0.55)
HIGH_OK = _env_float("RAG_HIGH_OK", 0.70)
MAX_SOURCES = 3
SMALLTALK_MAX_WORDS = 8
SMALLTALK_PATTERNS = (
    "hi",
    "hello",
    "hey",
    "thanks",
    "thank you",
    "good morning",
    "good afternoon",
    "good evening",
    "how are you",
)
SUGGESTION_TEMPLATE = (
    "Try sharing the organisation or company name, the TECH100 / ESG topic, and the report or year you're referencing so I can ground the answer."
)
SMALLTALK_FALLBACK = (
    "Hi there! I’m here whenever you need Sustainacore insights. Just share an ESG or AI question to get started."
)
EMPTY_QUERY_MESSAGE = (
    "Ask me something about Sustainacore’s ESG and AI work, and I’ll look up the relevant sources."
)
GEMINI_BIN = os.getenv("GEMINI_BIN", "gemini")
GEMINI_MODEL = os.getenv("RAG_GEMINI_MODEL") or os.getenv("GEMINI_MODEL", "gemini-1.5-pro")
GEMINI_TIMEOUT = _env_float("RAG_GEMINI_TIMEOUT", _env_float("GEMINI_TIMEOUT", 8.0))


def is_smalltalk(text: str) -> bool:
    cleaned = (text or "").strip().lower()
    if not cleaned:
        return False
    if cleaned.count("?") and "how are" not in cleaned:
        return False
    words = re.findall(r"[\w']+", cleaned)
    if not words:
        return False
    if len(words) > SMALLTALK_MAX_WORDS:
        return False
    normalized = " ".join(words)
    return any(pat in normalized for pat in SMALLTALK_PATTERNS)


def _score_from_dist(dist: Any) -> Optional[float]:
    if dist is None:
        return None
    try:
        return 1.0 - float(dist)
    except (TypeError, ValueError):
        return None


def search_vectors(query: str, k: int) -> List[Dict[str, Any]]:
    """Lookup similar chunks for the query.

    This uses the Ollama embedding endpoint + Oracle vector store when available.
    Any failure returns an empty list so routing can fall back safely.
    """

    if not query.strip():
        return []

    try:
        import requests
    except Exception:
        return []

    ollama_url = os.getenv("OLLAMA_URL", "http://127.0.0.1:11434")
    embed_model = os.getenv("OLLAMA_EMBED_MODEL", "all-minilm")

    try:
        resp = requests.post(
            f"{ollama_url}/api/embeddings",
            json={"model": embed_model, "prompt": query},
            timeout=8,
        )
        resp.raise_for_status()
        embedding = resp.json().get("embedding")
        if not isinstance(embedding, list):
            return []
    except Exception:
        return []

    try:
        from db_helper import top_k_by_vector
    except Exception:
        return []

    try:
        rows = top_k_by_vector(embedding, k=max(1, int(k)))
    except Exception:
        return []

    hits: List[Dict[str, Any]] = []
    for row in rows:
        doc_id = str(row.get("doc_id", ""))
        chunk_ix = row.get("chunk_ix")
        identifier = f"{doc_id}-{chunk_ix}" if doc_id else (str(chunk_ix) if chunk_ix is not None else "")
        snippet = (row.get("chunk_text") or row.get("snippet") or "").strip()
        hits.append(
            {
                "id": identifier or doc_id or "chunk",
                "title": (row.get("title") or "").strip(),
                "url": row.get("source_url") or "",
                "snippet": snippet,
                "score": _score_from_dist(row.get("dist")),
            }
        )
    return hits


def gemini_call(
    prompt: str,
    *,
    timeout: float | int = GEMINI_TIMEOUT,
    model: Optional[str] = None,
) -> Optional[str]:
    cmd: List[str] = [GEMINI_BIN]
    chosen_model = model or GEMINI_MODEL
    if chosen_model:
        cmd.extend(["--model", chosen_model])
    cmd.extend(["--json_input", "-p", prompt])
    try:
        completed = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return None
    if completed.returncode != 0:
        return None
    output = completed.stdout.strip()
    if not output:
        return None
    try:
        parsed = json.loads(output)
    except json.JSONDecodeError:
        return output
    if isinstance(parsed, dict):
        for key in ("text", "output", "answer", "content"):
            val = parsed.get(key)
            if isinstance(val, str) and val.strip():
                return val.strip()
    if isinstance(parsed, list):
        parts = [str(item).strip() for item in parsed if isinstance(item, (str, int, float))]
        joined = " ".join(p for p in parts if p)
        if joined.strip():
            return joined.strip()
    return output


def _shape_sources(
    hits: Iterable[Dict[str, Any]],
    limit: int = MAX_SOURCES,
) -> Tuple[List[str], List[Dict[str, str]]]:
    sources: List[str] = []
    contexts: List[Dict[str, str]] = []
    for idx, hit in enumerate(list(hits)[:limit], start=1):
        label = f"Source {idx}"
        title = (hit.get("title") or "").strip()
        identifier = (hit.get("id") or "").strip()
        url = (hit.get("url") or "").strip()
        display_title = title or identifier or url or label
        if url and display_title != url:
            display = f"{display_title} ({url})"
        else:
            display = display_title
        sources.append(f"{label}: {display}")
        contexts.append(
            {
                "label": label,
                "title": display_title,
                "url": url,
                "snippet": (hit.get("snippet") or "").strip(),
            }
        )
    return sources, contexts


def answer_smalltalk(query: str) -> Tuple[str, bool]:
    prompt = (
        "You are the Sustainacore ESG assistant. "
        "Respond to the user’s greeting in one short, friendly sentence. "
        "Acknowledge Sustainacore and invite them to share an ESG or AI question.\n"
        f"User message: {query!r}"
    )
    reply = gemini_call(prompt)
    if reply and reply.strip():
        return reply.strip(), True
    return SMALLTALK_FALLBACK, False


def answer_no_hit() -> Tuple[str, bool]:
    msg = (
        "I couldn’t find grounded Sustainacore material for that yet. "
        f"{SUGGESTION_TEMPLATE}"
    )
    return msg, False


def answer_low_conf(hits: List[Dict[str, Any]]) -> Tuple[str, bool, List[str]]:
    sources, _ = _shape_sources(hits)
    glimpsed_titles = [src.split(": ", 1)[-1] for src in sources]
    if glimpsed_titles:
        overview = ", ".join(glimpsed_titles)
        intro = f"I spotted potential matches ({overview}), but the confidence is below {LOW_OK:.2f}."
    else:
        intro = f"I found some potential matches, but the confidence is below {LOW_OK:.2f}."
    msg = (
        f"{intro} The results are inconclusive right now. {SUGGESTION_TEMPLATE}"
    )
    return msg, False, sources


def answer_confident(query: str, hits: List[Dict[str, Any]]) -> Tuple[str, bool, List[str]]:
    sources, contexts = _shape_sources(hits)
    if not contexts:
        fallback = (
            "I reviewed the Sustainacore corpus, but I need the organisation or timeframe to answer confidently."
        )
        return fallback, False, []

    passages = []
    for ctx in contexts:
        snippet = ctx["snippet"] or "(no snippet provided)"
        passages.append(f"[{ctx['label']}] {ctx['title']}\n{snippet}")
    prompt = (
        "You are the Sustainacore ESG assistant. Use ONLY the passages below to answer the question. "
        "Cite supporting passages using the [Source #] format that matches the labels. "
        "Keep it under 120 words and stay factual. "
        "If the passages do not answer the question, say so and ask for the missing detail.\n\n"
        f"Question: {query}\n\n"
        "Passages:\n"
        + "\n\n".join(passages)
    )
    reply = gemini_call(prompt)
    if reply and reply.strip():
        return reply.strip(), True, sources

    summary_bits = []
    for ctx in contexts:
        snippet = ctx["snippet"]
        if snippet:
            summary_bits.append(snippet[:120].rstrip())
        else:
            summary_bits.append(ctx["title"])
    summary = "; ".join(bit for bit in summary_bits if bit)
    if summary:
        fallback = (
            f"The top Sustainacore passages highlight: {summary}. "
            f"{SUGGESTION_TEMPLATE}"
        )
    else:
        fallback = (
            "I reviewed the Sustainacore passages but could not draft a confident answer. "
            f"{SUGGESTION_TEMPLATE}"
        )
    citations = " ".join(f"[{ctx['label']}]" for ctx in contexts)
    return f"{fallback} {citations}".strip(), False, sources


@app.get("/ask2", response_model=Answer)
def ask2(q: str = Query(""), k: int = Query(4)) -> Answer:
    query = (q or "").strip()
    try:
        k_int = int(k)
    except (TypeError, ValueError):
        k_int = 4
    k_int = max(1, min(k_int, 10))

    if not query:
        return Answer(
            answer=EMPTY_QUERY_MESSAGE,
            sources=[],
            meta={"routing": "empty", "top_score": None, "gemini_used": False, "k": k_int},
        )

    if is_smalltalk(query):
        answer, used_gemini = answer_smalltalk(query)
        return Answer(
            answer=answer,
            sources=[],
            meta={
                "routing": "smalltalk",
                "top_score": None,
                "gemini_used": used_gemini,
                "k": k_int,
            },
        )

    hits = search_vectors(query, k_int)
    if not hits:
        answer, used_gemini = answer_no_hit()
        return Answer(
            answer=answer,
            sources=[],
            meta={
                "routing": "no_hit",
                "top_score": None,
                "gemini_used": used_gemini,
                "k": k_int,
            },
        )

    top_score = hits[0].get("score")

    if top_score is None or top_score < LOW_OK:
        answer, used_gemini, sources = answer_low_conf(hits)
        return Answer(
            answer=answer,
            sources=sources,
            meta={
                "routing": "low_conf",
                "top_score": top_score,
                "gemini_used": used_gemini,
                "k": k_int,
            },
        )

    answer, used_gemini, sources = answer_confident(query, hits)
    return Answer(
        answer=answer,
        sources=sources,
        meta={
            "routing": "high_conf",
            "top_score": top_score,
            "gemini_used": used_gemini,
            "k": k_int,
        },
    )
