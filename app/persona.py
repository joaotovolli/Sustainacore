"""Persona and response templating helpers for SustainaCore chat answers."""

from __future__ import annotations

import re
from typing import Dict, Iterable, List

FALLBACK_MESSAGE = (
    "I couldn’t find that in SustainaCore’s knowledge base… Try refining your question."
)


def _dedupe_preserve_order(items: Iterable[str]) -> List[str]:
    seen = set()
    result: List[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


def _extract_sentences(answer: str) -> List[str]:
    text = answer.replace("•", " ")
    fragments = re.split(r"[\n\r]+", text)
    sentences: List[str] = []
    for fragment in fragments:
        fragment = fragment.strip()
        if not fragment:
            continue
        parts = re.split(r"(?<=[.!?])\s+", fragment)
        for part in parts:
            candidate = part.strip(" -")
            if candidate:
                sentences.append(candidate)
    return _dedupe_preserve_order(sentences)


def _format_sources(contexts: List[Dict[str, str]]) -> List[str]:
    lines: List[str] = []
    for idx, context in enumerate(contexts, start=1):
        title = context.get("title") or context.get("source_title") or ""
        url = context.get("source_url") or context.get("url") or ""
        display_title = title.strip() if isinstance(title, str) else ""
        display_url = url.strip() if isinstance(url, str) else ""
        if not display_url:
            continue
        if not display_title:
            display_title = display_url
        lines.append(f"{idx}. {display_title} — {display_url}")
    return lines


def apply_persona(answer: str, contexts: List[Dict[str, str]]) -> str:
    """Render the answer using the SustainaCore analyst persona template."""

    if not contexts:
        return FALLBACK_MESSAGE

    cleaned_answer = (answer or "").strip()
    if not cleaned_answer:
        return FALLBACK_MESSAGE

    sentences = _extract_sentences(cleaned_answer)
    if not sentences:
        sentences = [cleaned_answer]

    overview = sentences[0]
    remaining = sentences[1:] or sentences[:1]
    highlights = remaining[:3]

    citation_count = min(len(contexts), max(3, len(highlights))) if contexts else 0
    citation_labels = [f"[{idx}]" for idx in range(1, citation_count + 1)]

    overview_line = overview
    if citation_labels:
        overview_line = overview_line + " " + "".join(citation_labels[: min(3, len(citation_labels))])

    highlight_lines: List[str] = []
    for idx, highlight in enumerate(highlights):
        citation = citation_labels[idx] if idx < len(citation_labels) else citation_labels[-1] if citation_labels else ""
        if citation:
            highlight_lines.append(f"- {highlight} {citation}")
        else:
            highlight_lines.append(f"- {highlight}")

    formatted_sources = _format_sources(contexts)
    if not formatted_sources:
        return FALLBACK_MESSAGE

    lines: List[str] = [overview_line, "", "Highlights:"]
    lines.extend(highlight_lines or ["- See sources for more detail."])
    lines.append("")
    lines.append("Sources:")
    lines.extend(formatted_sources[: max(3, len(formatted_sources))])

    return "\n".join(lines)


__all__ = ["apply_persona", "FALLBACK_MESSAGE"]
