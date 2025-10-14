"""Local embedding client used by the Oracle retriever."""

from __future__ import annotations

from typing import List

from embedding_client import embed_text as _embed_text


def embed_text(text: str, *, timeout: float = 15.0) -> List[float]:
    """Delegate to the repository-level embedding client (Ollama-backed)."""

    return _embed_text(text, timeout=timeout)


__all__ = ["embed_text"]
