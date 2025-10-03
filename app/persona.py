"""Persona and response templating helpers for SustainaCore chat answers."""

from __future__ import annotations

import re
from typing import Dict, Iterable, List, Tuple

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


def _format_sources(contexts: List[Dict[str, str]]) -> Tuple[List[str], List[str]]:
    json_sources: List[str] = []
    lines: List[str] = []
    for idx, context in enumerate(contexts[:3], start=1):
        title = context.get("title") or context.get("source_title") or ""
        url = context.get("source_url") or context.get("url") or ""
        display_title = title.strip() if isinstance(title, str) else ""
        display_url = url.strip() if isinstance(url, str) else ""
        if not display_title:
            display_title = display_url or f"Source {idx}"
        json_sources.append(display_title)
        if display_url:
            lines.append(f"{idx}. {display_title} — {display_url}")
        else:
            lines.append(f"{idx}. {display_title}")
    return json_sources, lines


def apply_persona(answer: str, contexts: List[Dict[str, str]]) -> Tuple[str, List[str]]:
    """Render the answer using the SustainaCore analyst persona template."""

    cleaned_answer = (answer or "").strip()
    sentences = _extract_sentences(cleaned_answer) if cleaned_answer else []
    if not sentences:
        sentences = [FALLBACK_MESSAGE]

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
        bullet = f"- {highlight.strip()}" if highlight.strip() else "- See sources for more detail."
        if citation:
            bullet = f"{bullet} {citation}"
        highlight_lines.append(bullet)

    json_sources, formatted_sources = _format_sources(contexts)
    if not formatted_sources:
        formatted_sources = ["No sources available."]

    lines: List[str] = [overview_line, "", "Highlights:"]
    lines.extend(highlight_lines or ["- See sources for more detail."])
    lines.append("")
    lines.append("Sources:")
    lines.extend(formatted_sources)

    return "\n".join(lines), json_sources


__all__ = ["apply_persona", "FALLBACK_MESSAGE"]
