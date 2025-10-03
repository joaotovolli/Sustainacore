"""Request normalization helpers for /ask2."""

from __future__ import annotations

from typing import Any, Dict, Tuple

from fastapi import Request

BAD_INPUT_ERROR = "bad input"


async def normalize_request(request: Request, payload: Any) -> Tuple[Dict[str, Any], str | None]:
    """Return a normalized payload or an error string."""

    content_type = (request.headers.get("content-type") or "").split(";")[0].strip().lower()
    normalized: Dict[str, Any]

    if content_type == "text/plain":
        body_bytes = await request.body()
        body_text = body_bytes.decode("utf-8", "ignore").strip()
        normalized = {"question": body_text}
    elif isinstance(payload, dict):
        normalized = dict(payload)
    else:
        normalized = {}

    if "query" in normalized and "question" not in normalized:
        normalized["question"] = normalized["query"]

    if "topK" in normalized and "top_k" not in normalized:
        normalized["top_k"] = normalized["topK"]

    # Coerce string numerics to integers when possible.
    for key in ("top_k", "topK", "k"):
        if key in normalized and isinstance(normalized[key], str):
            candidate = normalized[key].strip()
            if candidate.isdigit():
                normalized[key] = int(candidate)
            else:
                try:
                    normalized[key] = int(float(candidate))
                except (TypeError, ValueError):
                    continue

    # Resolve the canonical question field.
    question_value = None
    for key in ("question", "query", "q", "text"):
        if key in normalized and normalized[key] is not None:
            question_value = normalized[key]
            break

    if isinstance(question_value, (int, float)):
        question_value = str(question_value)
    elif isinstance(question_value, bytes):
        question_value = question_value.decode("utf-8", "ignore")

    if not isinstance(question_value, str):
        question_value = ""

    question_value = question_value.strip()
    if not question_value:
        return {"question": question_value}, BAD_INPUT_ERROR

    normalized["question"] = question_value
    return normalized, None


__all__ = ["normalize_request", "BAD_INPUT_ERROR"]
