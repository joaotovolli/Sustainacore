"""Oracle retriever that embeds questions locally before querying Oracle."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from db_helper import top_k_by_vector

from .embedding_client import embed_text


LOGGER = logging.getLogger("app.oracle_retriever")


def _sanitize_distance(value: Any) -> Optional[float]:
    try:
        dist = float(value)
    except (TypeError, ValueError):
        return None
    if dist != dist:  # NaN guard
        return None
    return round(dist, 6)


def _shape_context(row: Dict[str, Any]) -> Dict[str, Any]:
    doc_id = row.get("doc_id")
    title = row.get("title")
    source_url = row.get("source_url")
    chunk_text = row.get("chunk_text") or row.get("snippet") or row.get("text")
    context = {
        "doc_id": str(doc_id) if doc_id is not None else "",
        "title": title.strip() if isinstance(title, str) else "",
        "source_url": source_url.strip() if isinstance(source_url, str) else "",
        "chunk_text": chunk_text.strip() if isinstance(chunk_text, str) else "",
    }
    dist = _sanitize_distance(row.get("dist"))
    if dist is not None:
        context["dist"] = dist
    chunk_ix = row.get("chunk_ix")
    if isinstance(chunk_ix, (int, str)):
        context["chunk_ix"] = chunk_ix
    return context


def retrieve(question: str, k: int, *, filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """Embed ``question`` locally, then run Oracle KNN to fetch contexts."""

    clean_question = (question or "").strip()
    if not clean_question:
        return []

    try:
        vector = embed_text(clean_question)
    except Exception as exc:
        LOGGER.exception("embedding_failed", exc_info=exc)
        raise

    try:
        k_value = int(k)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        k_value = 5
    if k_value < 1:
        k_value = 1

    try:
        rows = top_k_by_vector(vector, k_value, filters=filters or {})
    except Exception as exc:
        LOGGER.exception("oracle_knn_failed", exc_info=exc)
        raise

    contexts: List[Dict[str, Any]] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        context = _shape_context(row)
        if context["chunk_text"]:
            contexts.append(context)
    return contexts


__all__ = ["retrieve"]
