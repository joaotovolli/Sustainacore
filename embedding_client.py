"""Embedding client helpers shared across entrypoints."""
from __future__ import annotations

import logging
import os
from typing import List

import requests

from embedder_settings import EmbedSettings, get_embed_settings

LOGGER = logging.getLogger("sustainacore.embedding_client")

_OLLAMA_URL = os.getenv("OLLAMA_URL", "http://127.0.0.1:11434").rstrip("/")


def _normalize_vector(vec: List[float], expected_dim: int) -> List[float]:
    if len(vec) == expected_dim:
        return vec
    if len(vec) > expected_dim:
        return vec[:expected_dim]
    return vec + [0.0] * (expected_dim - len(vec))


def embed_text(text: str, *, timeout: float = 15.0, settings: EmbedSettings | None = None) -> List[float]:
    """Embed text using the configured Ollama endpoint."""

    cfg = settings or get_embed_settings()
    prompt = (text or "").strip()
    if not prompt:
        return [0.0] * cfg.expected_dimension

    payload = {"model": cfg.model_name, "prompt": prompt}
    try:
        response = requests.post(f"{_OLLAMA_URL}/api/embeddings", json=payload, timeout=timeout)
        response.raise_for_status()
    except Exception as exc:  # pragma: no cover - network error path
        LOGGER.error("Embedding request failed: %s", exc)
        raise

    data = response.json()
    vector = data.get("embedding")
    if not isinstance(vector, list):
        data_list = data.get("data")
        if isinstance(data_list, list) and data_list:
            vector = data_list[0].get("embedding")
    if not isinstance(vector, list):
        raise ValueError("embedding response missing 'embedding' vector")

    return _normalize_vector([float(x) for x in vector], cfg.expected_dimension)


__all__ = ["embed_text"]
