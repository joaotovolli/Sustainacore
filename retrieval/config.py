"""Runtime configuration helpers for retrieval."""
from __future__ import annotations

import os
from typing import Dict


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    raw = raw.strip().lower()
    if raw in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if raw in {"0", "false", "f", "no", "n", "off"}:
        return False
    return default


RETRIEVAL_TOP_K = max(8, int(os.getenv("RETRIEVAL_TOP_K", os.getenv("FUSION_TOPK_BASE", "8"))))
SIMILARITY_FLOOR = float(os.getenv("SIMILARITY_FLOOR", "0.58"))
RETRIEVAL_SCOPING_ENABLED = _env_bool("RETRIEVAL_SCOPING", True)
INSUFFICIENT_CONTEXT_MESSAGE = (
    "I’m not confident I have enough Sustainacore context to answer that yet. "
    "Please provide more detail or a specific company/document."
)


def config_snapshot() -> Dict[str, object]:
    return {
        "retrieval_top_k": RETRIEVAL_TOP_K,
        "similarity_floor": SIMILARITY_FLOOR,
        "scoping_enabled": RETRIEVAL_SCOPING_ENABLED,
    }


__all__ = [
    "RETRIEVAL_TOP_K",
    "SIMILARITY_FLOOR",
    "RETRIEVAL_SCOPING_ENABLED",
    "INSUFFICIENT_CONTEXT_MESSAGE",
    "config_snapshot",
]
