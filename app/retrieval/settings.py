"""Centralised configuration for retrieval components."""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field
from typing import Literal

if sys.version_info >= (3, 10):  # pragma: no cover - version guard
    def _settings_dataclass(cls):
        return dataclass(cls, slots=True)
else:  # pragma: no cover - Python 3.9 fallback
    _settings_dataclass = dataclass


def _env_bool(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "t", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):  # pragma: no cover - defensive guard
        return default


def _env_float(name: str, default: float) -> float:
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):  # pragma: no cover - defensive guard
        return default


@_settings_dataclass
class Settings:
    """Derive retrieval configuration from the environment."""

    gemini_first_enabled: bool = field(default_factory=lambda: _env_bool("GEMINI_FIRST_ENABLED", True))
    show_debug_block: bool = field(default_factory=lambda: _env_bool("SHOW_DEBUG_BLOCK", False))
    allow_hop2: bool = field(default_factory=lambda: _env_bool("ALLOW_HOP2", True))

    gemini_bin: str = field(default_factory=lambda: os.environ.get("GEMINI_BIN", "gemini"))
    gemini_model_intent: str = field(
        default_factory=lambda: os.environ.get("GEMINI_INTENT_MODEL", os.environ.get("GEMINI_MODEL", "gemini-1.5-pro"))
    )
    gemini_model_plan: str = field(
        default_factory=lambda: os.environ.get("GEMINI_PLAN_MODEL", os.environ.get("GEMINI_MODEL", "gemini-1.5-pro"))
    )
    gemini_model_answer: str = field(
        default_factory=lambda: os.environ.get("GEMINI_ANSWER_MODEL", os.environ.get("GEMINI_MODEL", "gemini-1.5-pro"))
    )
    gemini_timeout: float = field(default_factory=lambda: _env_float("GEMINI_TIMEOUT", _env_float("RAG_GEMINI_TIMEOUT", 8.0)))

    oracle_table: str = field(default_factory=lambda: os.environ.get("ORACLE_VECTOR_TABLE", "ESG_DOCS"))
    oracle_embedding_column: str = field(default_factory=lambda: os.environ.get("ORACLE_VECTOR_COLUMN", "EMBEDDING"))
    oracle_text_column: str = field(default_factory=lambda: os.environ.get("ORACLE_TEXT_COLUMN", "CHUNK_TEXT"))
    oracle_url_column: str = field(default_factory=lambda: os.environ.get("ORACLE_URL_COLUMN", "SOURCE_URL"))
    oracle_normalized_url_column: str = field(default_factory=lambda: os.environ.get("ORACLE_NORM_URL_COLUMN", "NORMALIZED_URL"))
    oracle_title_column: str = field(default_factory=lambda: os.environ.get("ORACLE_TITLE_COLUMN", "TITLE"))
    oracle_source_column: str = field(default_factory=lambda: os.environ.get("ORACLE_SOURCE_COLUMN", "SOURCE_NAME"))
    oracle_date_column: str = field(default_factory=lambda: os.environ.get("ORACLE_DATE_COLUMN", "PUBLISHED_DATE"))
    oracle_doc_id_column: str = field(default_factory=lambda: os.environ.get("ORACLE_DOC_ID_COLUMN", "DOC_ID"))
    oracle_source_id_column: str = field(default_factory=lambda: os.environ.get("ORACLE_SOURCE_ID_COLUMN", "SOURCE_ID"))
    oracle_chunk_ix_column: str = field(default_factory=lambda: os.environ.get("ORACLE_CHUNK_INDEX_COLUMN", "CHUNK_IX"))
    oracle_scope_filters: tuple[str, ...] = field(
        default_factory=lambda: tuple(
            f.strip().upper()
            for f in os.environ.get(
                "ORACLE_ALLOWED_FILTERS",
                "SOURCE_TYPE,TICKER,DATE_FROM,DATE_TO,DOC_ID,SOURCE_ID",
            ).split(",")
            if f.strip()
        )
    )

    oracle_embed_model: str = field(default_factory=lambda: os.environ.get("ORACLE_EMBED_MODEL", "AI$MINILM_L6_V2"))
    oracle_embed_proc: str = field(default_factory=lambda: os.environ.get("ORACLE_EMBED_PROC", "LOCAL_EMBEDDINGS"))
    oracle_embed_sql: str = field(default_factory=lambda: os.environ.get("ORACLE_EMBED_SQL", "LOCAL_EMBEDDINGS"))
    oracle_knn_metric: Literal["COSINE", "DOT"] = field(
        default_factory=lambda: "DOT"
        if os.environ.get("ORACLE_KNN_METRIC", "COSINE").strip().upper() == "DOT"
        else "COSINE"
    )
    oracle_knn_k: int = field(default_factory=lambda: _env_int("ORACLE_KNN_K", 24))

    rag_prefer_vector: bool = field(default_factory=lambda: _env_bool("RAG_PREFER_VECTOR", True))
    rag_expected_dim: int = field(default_factory=lambda: _env_int("RAG_EXPECTED_DIM", 384))
    rag_text_mode: str = field(
        default_factory=lambda: os.environ.get("RAG_TEXT_MODE", "").strip().lower()
    )

    retriever_max_facts: int = field(default_factory=lambda: _env_int("RETRIEVER_MAX_FACTS", 8))
    retriever_fact_cap: int = field(default_factory=lambda: _env_int("RETRIEVER_FACT_CAP", 6))
    retriever_per_source_cap: int = field(default_factory=lambda: _env_int("RETRIEVER_PER_SOURCE_CAP", 2))

    rate_limit_window_seconds: int = field(default_factory=lambda: _env_int("ASK2_RATE_WINDOW", 10))
    rate_limit_max_requests: int = field(default_factory=lambda: _env_int("ASK2_RATE_MAX", 8))

    latency_budget_ms: int = field(default_factory=lambda: _env_int("ASK2_LATENCY_BUDGET_MS", 4500))


settings = Settings()


__all__ = ["settings", "Settings"]
