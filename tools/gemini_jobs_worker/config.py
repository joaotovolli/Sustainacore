"""Configuration defaults for the Gemini Jobs Worker."""
from __future__ import annotations

import os

ROUTINE_LABEL_MAP = {
    "Text to publish news on Sustainacore.org": "NEWS_PUBLISH",
    "Text to be transformed and added to RAG Vectors": "RAG_INGEST",
    "Data to be added to Index Rebalance": "INDEX_REBALANCE",
}

SUPPORTED_ROUTINES = {"RAG_INGEST"}

DEFAULT_POLL_SECONDS = int(os.getenv("GEMINI_JOBS_POLL_SECONDS", "30"))
FAST_POLL_SECONDS = int(os.getenv("GEMINI_JOBS_FAST_POLL_SECONDS", "5"))
MEDIUM_POLL_SECONDS = int(os.getenv("GEMINI_JOBS_MEDIUM_POLL_SECONDS", "15"))
FAST_POLL_WINDOW_SECONDS = int(os.getenv("GEMINI_JOBS_FAST_WINDOW_SECONDS", "60"))

CHUNK_MAX_CHARS = int(os.getenv("RAG_CHUNK_MAX_CHARS", "1200"))
CHUNK_OVERLAP_CHARS = int(os.getenv("RAG_CHUNK_OVERLAP_CHARS", "120"))

PAYLOAD_MIME = "text/csv"
PAYLOAD_SUFFIX = ".csv"
PAYLOAD_PREFIX = "rag_ingest_payload_"
