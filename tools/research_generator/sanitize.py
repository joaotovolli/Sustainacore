"""Deterministic sanitizer for publish-ready text."""
from __future__ import annotations

import re
from typing import List, Tuple


_SPACED_DECIMAL = re.compile(r"(\d+)\.\s+(\d+)")
_DOUBLE_SPACE = re.compile(r"\s{2,}")
_DUPLICATE_HEADING = re.compile(r"^(#{1,6}\s+)(.+)$")


FORBIDDEN_SNIPPETS = [
    "anchors the evidence",
    "provides the detailed breakdown",
    "what it shows",
    "chart above",
    "table above",
]


def sanitize_text_blocks(paragraphs: List[str]) -> List[str]:
    cleaned: List[str] = []
    last_heading = None
    for para in paragraphs:
        text = para or ""
        text = _SPACED_DECIMAL.sub(r"\1.\2", text)
        text = _DOUBLE_SPACE.sub(" ", text).strip()
        for snippet in FORBIDDEN_SNIPPETS:
            text = text.replace(snippet, "").replace(snippet.title(), "")
        match = _DUPLICATE_HEADING.match(text)
        if match:
            heading = match.group(2).strip().lower()
            if heading == last_heading:
                continue
            last_heading = heading
        cleaned.append(text)
    return [c for c in cleaned if c]
