"""Boundary-aligned chunking helpers."""
from __future__ import annotations

import re
from typing import List

_SENTENCE_RE = re.compile(r"[^.!?]+[.!?]+|[^.!?]+$")


def _split_sentences(text: str) -> List[str]:
    text = " ".join((text or "").split())
    if not text:
        return []
    return [m.group(0).strip() for m in _SENTENCE_RE.finditer(text) if m.group(0).strip()]


def _split_words(text: str, max_len: int) -> List[str]:
    words = text.split()
    chunks: List[str] = []
    current: List[str] = []
    length = 0
    for word in words:
        add_len = len(word) + (1 if current else 0)
        if current and length + add_len > max_len:
            chunks.append(" ".join(current))
            current = []
            length = 0
        current.append(word)
        length += add_len
    if current:
        chunks.append(" ".join(current))
    return chunks


def chunk_text_boundary(
    text: str,
    *,
    header: str,
    target: int = 1200,
    min_len: int = 800,
    max_len: int = 1400,
    overlap: int = 100,
) -> List[str]:
    header = (header or "").strip()
    base_sentences = _split_sentences(text)
    if not base_sentences:
        return []

    sentences: List[str] = []
    for sentence in base_sentences:
        if len(sentence) > max_len:
            sentences.extend(_split_words(sentence, max_len))
        else:
            sentences.append(sentence)

    chunks: List[str] = []
    current: List[str] = []
    current_len = len(header) + (2 if header else 0)

    def _emit_chunk(local_sentences: List[str]) -> None:
        body = " ".join(local_sentences).strip()
        if not body:
            return
        if header:
            chunk = f"{header}\n{body}"
        else:
            chunk = body
        chunks.append(chunk)

    idx = 0
    while idx < len(sentences):
        sentence = sentences[idx]
        add_len = len(sentence) + (1 if current else 0)
        if current and (current_len + add_len > max_len) and current_len >= min_len:
            _emit_chunk(current)
            if overlap > 0 and current:
                overlap_chars = 0
                overlap_sentences: List[str] = []
                for prev in reversed(current):
                    if overlap_chars + len(prev) + 1 > overlap:
                        break
                    overlap_sentences.insert(0, prev)
                    overlap_chars += len(prev) + 1
                current = overlap_sentences[:]
                current_len = len(header) + (2 if header else 0) + len(" ".join(current))
            else:
                current = []
                current_len = len(header) + (2 if header else 0)
            continue

        current.append(sentence)
        current_len += add_len
        idx += 1

    if current:
        _emit_chunk(current)

    return chunks
