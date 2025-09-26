"""Intent detection and scoping helpers for Sustainacore retrieval."""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from .config import RETRIEVAL_SCOPING_ENABLED

_ALIAS = {
    "tech 100": "TECH100",
    "tech-100": "TECH100",
    "ai governance & ethics index": "TECH100",
    "msft": "Microsoft",
    "csco": "Cisco",
    "aapl": "Apple",
    "googl": "Alphabet",
    "goog": "Alphabet",
    "meta": "Meta",
    "ibm": "IBM",
}

_SCOPE_SOURCE_TYPES = {
    "membership": ("membership", "constituent", "tech100_membership"),
    "company_profile": ("company_profile", "profile", "snapshot"),
    "regulatory": ("regulatory", "regulation", "policy"),
    "about_site": ("site", "about", "faq"),
}


@dataclass
class RetrievalScope:
    label: str
    source_types: Tuple[str, ...] = field(default_factory=tuple)
    company: Optional[str] = None
    explicit_docset: Optional[str] = None
    applied_filters: Dict[str, Sequence[str]] = field(default_factory=dict)


def extract_entities(question: str) -> List[str]:
    ents: List[str] = []
    for match in re.finditer(r"\b([A-Z][A-Za-z0-9&\.\-]{1,}(?: [A-Z][A-Za-z0-9&\.\-]{1,}){0,3})\b", question):
        candidate = match.group(1).strip()
        if candidate:
            ents.append(_ALIAS.get(candidate.lower(), candidate))
    if "tech 100" in question.lower() or "tech-100" in question.lower():
        ents.append("TECH100")
    seen = set()
    ordered: List[str] = []
    for ent in ents:
        if ent not in seen:
            seen.add(ent)
            ordered.append(ent)
    return ordered


def detect_intent(question: str, entities: Sequence[str]) -> str:
    text = question.lower()
    if re.search(r"\bmember(ship)?\b|\bpart of\b|\bin (?:the )?tech ?100\b", text):
        return "membership"
    if re.search(r"\bprofile\b|\boverview\b|\bsnapshot\b|\babout\b", text) and entities:
        return "company_profile"
    if re.search(r"\bpolicy\b|\bregulat|\blaw\b|\bact\b|\bdirective\b", text):
        return "regulatory"
    if re.search(r"what is this website|about this website|about sustainacore", text):
        return "about_site"
    return "general"


def _pick_company(entities: Sequence[str]) -> Optional[str]:
    for ent in entities:
        if ent.upper() == "TECH100":
            continue
        if len(ent) <= 2:
            continue
        return ent
    return None


def infer_scope(
    question: str,
    *,
    explicit_filters: Optional[Dict[str, str]] = None,
    entities: Optional[Sequence[str]] = None,
) -> RetrievalScope:
    filters = explicit_filters or {}
    if not RETRIEVAL_SCOPING_ENABLED:
        return RetrievalScope(label="unscoped", applied_filters={})

    normalized_question = (question or "").strip()
    resolved_entities = list(entities) if entities is not None else extract_entities(normalized_question)
    intent = detect_intent(normalized_question, resolved_entities)

    explicit_docset = (filters.get("docset") or filters.get("namespace") or "").strip()
    applied_filters: Dict[str, Sequence[str]] = {}
    label = intent
    source_types: Tuple[str, ...] = ()
    company = filters.get("ticker") or filters.get("company")
    if company:
        company = company.strip()

    if explicit_docset:
        source_types = (explicit_docset,)
        label = f"explicit:{explicit_docset}"
    elif intent in _SCOPE_SOURCE_TYPES:
        source_types = _SCOPE_SOURCE_TYPES[intent]
    else:
        label = "general"

    if not company:
        company = _pick_company(resolved_entities)

    if source_types:
        applied_filters["source_type"] = tuple(source_types)
    if company:
        applied_filters["source_id"] = (company.upper(),)

    return RetrievalScope(
        label=label,
        source_types=tuple(source_types),
        company=company,
        explicit_docset=explicit_docset or None,
        applied_filters=applied_filters,
    )


def dedupe_contexts(chunks: Iterable[Dict[str, object]]) -> List[Dict[str, object]]:
    seen = set()
    deduped: List[Dict[str, object]] = []
    for chunk in chunks:
        title = str(chunk.get("title") or "").strip().lower()
        url = str(chunk.get("source_url") or "").strip().lower()
        key = (title, url)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(chunk)
    return deduped


def compute_similarity(distance: Optional[float]) -> Optional[float]:
    if distance is None:
        return None
    try:
        score = 1.0 - float(distance)
    except (TypeError, ValueError):
        return None
    if score < 0.0:
        score = 0.0
    if score > 1.0:
        score = 1.0
    return round(score, 4)


__all__ = [
    "RetrievalScope",
    "dedupe_contexts",
    "detect_intent",
    "extract_entities",
    "infer_scope",
    "compute_similarity",
]
