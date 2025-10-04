"""File-system backed retriever fallback for /ask2 contexts."""

from __future__ import annotations

import glob
import json
import os
from typing import Any, Dict, List

DATA_DIR = os.environ.get("SC_FS_DATA", "data/fs_corpus")


def _iter_files(directory: str) -> List[str]:
    pattern_jsonl = os.path.join(directory, "*.jsonl")
    pattern_json = os.path.join(directory, "*.json")
    files = glob.glob(pattern_jsonl)
    files.extend(glob.glob(pattern_json))
    return sorted(set(files))


def search(question: str, top_k: int = 6) -> List[Dict[str, Any]]:
    """Return at most ``top_k`` contexts from on-disk corpora.

    This is intentionally simple: it collects JSON/JSONL payloads without
    ranking. The goal is to keep contexts populated when Oracle vector search
    is unavailable.
    """

    del question  # unused placeholder for compatibility

    max_items = max(1, int(top_k))
    results: List[Dict[str, Any]] = []

    for path in _iter_files(DATA_DIR):
        try:
            if path.endswith(".jsonl"):
                with open(path, "r", encoding="utf-8") as handle:
                    for line in handle:
                        if len(results) >= max_items:
                            return results
                        if not line.strip():
                            continue
                        payload = json.loads(line)
                        if isinstance(payload, dict):
                            results.append(payload)
                        elif isinstance(payload, list):
                            results.extend([item for item in payload if isinstance(item, dict)])
            else:
                with open(path, "r", encoding="utf-8") as handle:
                    payload = json.load(handle)
                if isinstance(payload, list):
                    for item in payload:
                        if len(results) >= max_items:
                            return results
                        if isinstance(item, dict):
                            results.append(item)
                elif isinstance(payload, dict):
                    results.append(payload)
        except Exception:
            continue
        if len(results) >= max_items:
            break

    return results[:max_items]
