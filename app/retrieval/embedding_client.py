"""Compatibility wrapper exposing the shared embedding client."""

from __future__ import annotations

from typing import Any, List, Optional

from embedding_client import embed_text as _embed_text


def embed_text(text: str, *, timeout: float = 15.0, settings: Optional[Any] = None) -> List[float]:
    """Proxy to the project-wide embedding helper for local vector generation."""

    return _embed_text(text, timeout=timeout, settings=settings)


__all__ = ["embed_text"]
