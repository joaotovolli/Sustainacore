"""Embedding client helpers shared across entrypoints.

Production note:
- VM1 should not depend on an always-on local Ollama daemon for embeddings.
- Prefer in-process embeddings via `fastembed` when available; keep Ollama as a
  compatibility fallback (and for local experimentation).
"""
from __future__ import annotations

import logging
import os
from typing import List

import requests

from embedder_settings import EmbedSettings, get_embed_settings

LOGGER = logging.getLogger("sustainacore.embedding_client")

_OLLAMA_URL = os.getenv("OLLAMA_URL", "http://127.0.0.1:11434").rstrip("/")
_EMBED_PROVIDER = (os.getenv("EMBED_PROVIDER") or os.getenv("EMBEDDING_PROVIDER") or "").strip().lower()

# Lazy, process-wide singleton for fastembed.
_FASTEMBED = None
_FASTEMBED_MODEL = None


def _resolve_fastembed_model(name: str) -> str:
    """Map repo-friendly model names to fastembed model ids.

    `EMBED_MODEL_NAME=all-minilm` is a common shorthand in this repo. fastembed
    model ids vary; use a stable 384D default unless the user provides a
    supported id explicitly.
    """

    raw = (name or "").strip()
    if not raw:
        return "BAAI/bge-small-en-v1.5"
    lowered = raw.lower()
    if lowered in {"all-minilm", "minilm", "all_mini_lm", "all-mini-lm"}:
        return "BAAI/bge-small-en-v1.5"
    return raw


def _fastembed_embedder(model_name: str):
    global _FASTEMBED, _FASTEMBED_MODEL

    if _FASTEMBED is not None and _FASTEMBED_MODEL == model_name:
        return _FASTEMBED

    try:
        from fastembed import TextEmbedding  # type: ignore
    except Exception as exc:  # pragma: no cover - depends on optional dep availability
        raise RuntimeError("fastembed is not available (install requirements.txt)") from exc

    resolved = _resolve_fastembed_model(model_name)
    # Keep embeddings stable on small VMs.
    os.environ.setdefault("OMP_NUM_THREADS", "1")
    os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
    os.environ.setdefault("MKL_NUM_THREADS", "1")

    _FASTEMBED = TextEmbedding(model_name=resolved)
    _FASTEMBED_MODEL = model_name
    return _FASTEMBED


def _normalize_vector(vec: List[float], expected_dim: int) -> List[float]:
    if len(vec) == expected_dim:
        return vec
    if len(vec) > expected_dim:
        return vec[:expected_dim]
    return vec + [0.0] * (expected_dim - len(vec))


def embed_text(text: str, *, timeout: float = 15.0, settings: EmbedSettings | None = None) -> List[float]:
    """Embed text using the configured provider.

    Provider selection:
    - `EMBED_PROVIDER=fastembed` uses in-process ONNX embeddings.
    - `EMBED_PROVIDER=ollama` uses the Ollama HTTP API.

    If `EMBED_PROVIDER` is unset, we default to `fastembed` when available.
    """

    cfg = settings or get_embed_settings()
    prompt = (text or "").strip()
    if not prompt:
        return [0.0] * cfg.expected_dimension

    provider = _EMBED_PROVIDER

    if not provider:
        # Best-effort default: use fastembed if importable, else ollama.
        try:
            _fastembed_embedder(cfg.model_name)
            provider = "fastembed"
        except Exception:
            provider = "ollama"

    if provider == "fastembed":
        embedder = _fastembed_embedder(cfg.model_name)
        vec = None
        for item in embedder.embed([prompt]):
            vec = item
            break
        if vec is None:
            raise RuntimeError("fastembed returned no embedding")
        try:
            vector = [float(x) for x in vec.tolist()]
        except Exception:
            vector = [float(x) for x in list(vec)]
        return _normalize_vector(vector, cfg.expected_dimension)

    if provider != "ollama":
        raise ValueError(f"unknown EMBED_PROVIDER={provider!r}")

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
