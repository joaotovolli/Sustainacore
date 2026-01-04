"""Deterministic sanitizer for publish-ready text."""
from __future__ import annotations

import re
from typing import List

_SPACED_DECIMAL = re.compile(r"(\d+)\.\s+(\d+)")
_DOUBLE_SPACE = re.compile(r"\s{2,}")

FORBIDDEN_SNIPPETS = [
    "what it shows",
    "chart above",
    "table above",
    "provides the detailed breakdown",
    "anchors the evidence",
]


def sanitize_text_blocks(paragraphs: List[str]) -> List[str]:
    cleaned: List[str] = []
    for para in paragraphs:
        text = para or ""
        text = _SPACED_DECIMAL.sub(r"\1.\2", text)
        text = _DOUBLE_SPACE.sub(" ", text).strip()
        for snippet in FORBIDDEN_SNIPPETS:
            text = text.replace(snippet, "").replace(snippet.title(), "")
        if text:
            cleaned.append(text)
    return cleaned
