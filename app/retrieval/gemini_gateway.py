"""Thin wrapper around the Gemini CLI for the Gemini-first orchestration."""

from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from app.rag.gemini_cli import gemini_call

from .settings import settings


LOGGER = logging.getLogger("gemini-gateway")


def _parse_json(text: Optional[str]) -> Optional[Dict[str, Any]]:
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        LOGGER.warning("Gemini response was not valid JSON: %s", text[:2000])
        return None


class GeminiGateway:
    """Lightweight helper that enforces consistent prompts and parsing."""

    def __init__(self) -> None:
        self._timeout = settings.gemini_timeout

    def _call_json(self, prompt: str, *, model: str) -> Optional[Dict[str, Any]]:
        text = gemini_call(prompt, timeout=self._timeout, model=model)
        return _parse_json(text)

    # ---- public API -----------------------------------------------------

    def classify_intent(self, question: str) -> Dict[str, Any]:
        """Return a structured intent classification."""

        prompt = (
            "You are SustainaCore's Gemini front-door classifier.\n"
            "Decide whether the user input is SMALL_TALK or INFO_REQUEST.\n"
            "SMALL_TALK covers greetings, thanks, compliments, or casual chit-chat.\n"
            "INFO_REQUEST is any request that needs grounded facts about TECH100, ESG, or AI governance.\n"
            "Return ONLY valid compact JSON with keys intent, confidence (0-1), and rationale.\n"
            "Example: {\"intent\": \"SMALL_TALK\", \"confidence\": 0.62, \"rationale\": \"greeting\"}.\n"
            f"User input: {question.strip()}\n"
        )

        payload = self._call_json(prompt, model=settings.gemini_model_intent) or {}
        intent = str(payload.get("intent") or "INFO_REQUEST").strip().upper()
        if intent not in {"SMALL_TALK", "INFO_REQUEST"}:
            intent = "INFO_REQUEST"
        confidence = payload.get("confidence")
        try:
            confidence = float(confidence)
        except (TypeError, ValueError):
            confidence = None
        rationale = str(payload.get("rationale") or "").strip()
        return {"intent": intent, "confidence": confidence, "rationale": rationale, "raw": payload}

    def plan_retrieval(self, question: str) -> Dict[str, Any]:
        """Ask Gemini for a retrieval plan."""

        prompt = (
            "You are the retrieval planner for SustainaCore's Gemini-first pipeline.\n"
            "Craft Oracle 23ai vector search instructions for the question below.\n"
            "Return ONLY JSON with keys: filters (object), query_variants (3-5 strings), k (int), and optional hop2.\n"
            "filters must stick to supported keys: SOURCE_TYPE, TICKER, DATE_FROM, DATE_TO, DOC_ID, SOURCE_ID.\n"
            "Use upper-case keys and lists for multi-value filters.\n"
            "Ensure query_variants preserve key nouns and abbreviations.\n"
            "Set k to 24 unless a smaller slice is strongly justified.\n"
            "hop2 is optional and may include reason, filters, and query_variants when a second retrieval hop is essential.\n"
            "Never include prose outside the JSON payload.\n"
            f"Question: {question.strip()}\n"
        )

        plan = self._call_json(prompt, model=settings.gemini_model_plan) or {}
        variants = plan.get("query_variants")
        if not isinstance(variants, list):
            variants = [question.strip()]
        cleaned_variants: List[str] = []
        for variant in variants:
            if not isinstance(variant, str):
                continue
            variant = variant.strip()
            if not variant:
                continue
            if variant not in cleaned_variants:
                cleaned_variants.append(variant)
            if len(cleaned_variants) >= 5:
                break
        if not cleaned_variants:
            cleaned_variants = [question.strip()]
        filters = plan.get("filters")
        filters = filters if isinstance(filters, dict) else {}
        upper_filters = {}
        for key, value in filters.items():
            if not isinstance(key, str):
                continue
            key_u = key.strip().upper()
            if key_u not in settings.oracle_scope_filters:
                continue
            if isinstance(value, list):
                normalized_list = []
                for item in value:
                    if isinstance(item, str) and item.strip():
                        normalized_list.append(item.strip())
                if normalized_list:
                    upper_filters[key_u] = normalized_list
            elif isinstance(value, str) and value.strip():
                upper_filters[key_u] = value.strip()
        k_value = plan.get("k")
        try:
            k_int = int(k_value)
        except (TypeError, ValueError):
            k_int = settings.oracle_knn_k
        if k_int <= 0 or k_int > 64:
            k_int = settings.oracle_knn_k
        hop2 = plan.get("hop2") if isinstance(plan.get("hop2"), dict) else None
        if hop2:
            hop2_filters = hop2.get("filters") if isinstance(hop2.get("filters"), dict) else {}
            normalized_hop2_filters = {}
            for key, value in hop2_filters.items():
                if isinstance(key, str):
                    key_u = key.strip().upper()
                    if key_u in settings.oracle_scope_filters:
                        normalized_hop2_filters[key_u] = value
            hop2_variants_raw = hop2.get("query_variants") if isinstance(hop2.get("query_variants"), list) else []
            hop2_variants: List[str] = []
            for item in hop2_variants_raw:
                if isinstance(item, str):
                    item = item.strip()
                    if item and item not in hop2_variants:
                        hop2_variants.append(item)
            hop2_data = {
                "reason": hop2.get("reason"),
                "filters": normalized_hop2_filters,
                "query_variants": hop2_variants[:5],
            }
        else:
            hop2_data = None

        return {
            "filters": upper_filters,
            "query_variants": cleaned_variants,
            "k": k_int,
            "hop2": hop2_data,
            "raw": plan,
        }

    def compose_small_talk(self, question: str) -> str:
        prompt = (
            "You are SustainaCore's assistant. Provide a short, warm reply (1-2 sentences) to the user input below.\n"
            "Do NOT add citations or mention sources.\n"
            "Keep it natural and contextual.\n"
            "Return ONLY the reply text as JSON: {\"answer\": \"...\"}.\n"
            f"User input: {question.strip()}"
        )
        payload = self._call_json(prompt, model=settings.gemini_model_answer) or {}
        answer = payload.get("answer") if isinstance(payload, dict) else None
        if not isinstance(answer, str) or not answer.strip():
            return "Happy to help!"
        return answer.strip()

    def compose_answer(
        self,
        question: str,
        retriever_result: Dict[str, Any],
        plan: Dict[str, Any],
        hop_count: int,
    ) -> Dict[str, Any]:
        """Ask Gemini to generate the final answer using retrieved facts."""

        payload = {
            "question": question.strip(),
            "plan": {k: v for k, v in plan.items() if k != "raw"},
            "retriever_result": retriever_result,
            "hop_count": hop_count,
            "instructions": {
                "style": "≤1 short paragraph + optional bullets; friendly and precise",
                "citations": "inline using [citation_id]",
                "no_debug": True,
                "max_sources": settings.retriever_fact_cap,
                "prohibited_openers": [
                    "Here’s the best supported answer",
                    "Here's the best supported answer",
                    "Why this answer",
                ],
            },
        }
        prompt = (
            "You are the Gemini composer for SustainaCore.\n"
            "Use ONLY the facts in retriever_result.facts to answer the question.\n"
            "Every claim referencing retrieved evidence must include an inline citation like [FCA_2024_Guidance].\n"
            "Do not invent citation ids or facts.\n"
            "Keep the tone confident, concise, and human.\n"
            "Do not prefix the answer with explanations such as 'Here’s the best supported answer' or 'Why this answer'.\n"
            "Return ONLY JSON with keys: answer (string) and sources (array of strings formatted 'Title — Publisher (Date)').\n"
            "Do not include debug sections, numbered sources, or redundant source listings in the answer body.\n"
            f"Payload: {json.dumps(payload, ensure_ascii=False)}"
        )
        response = self._call_json(prompt, model=settings.gemini_model_answer) or {}
        answer = response.get("answer") if isinstance(response, dict) else None
        if not isinstance(answer, str):
            answer = "I’m sorry, I couldn’t generate an answer from the retrieved facts."
        cleaned_answer = _clean_answer_text(answer)

        facts = retriever_result.get("facts") if isinstance(retriever_result, dict) else None
        derived_sources = _build_sources_from_facts(
            cleaned_answer, facts if isinstance(facts, list) else [], settings.retriever_fact_cap
        )

        if not derived_sources:
            sources = response.get("sources") if isinstance(response, dict) else None
            if not isinstance(sources, list):
                sources = []
            raw_sources: List[str] = []
            for item in sources:
                if isinstance(item, str):
                    candidate = item.strip()
                    if candidate:
                        raw_sources.append(candidate)
                elif isinstance(item, dict):
                    display = item.get("display") or item.get("title") or ""
                    if isinstance(display, str):
                        candidate = display.strip()
                        if candidate:
                            raw_sources.append(candidate)
            derived_sources = _dedup_sources(raw_sources, settings.retriever_fact_cap)

        final_sources = _dedup_sources(derived_sources, settings.retriever_fact_cap)

        return {"answer": cleaned_answer, "sources": final_sources, "raw": response}


gateway = GeminiGateway()


def _clean_answer_text(answer: str) -> str:
    """Remove boilerplate/debug headings from Gemini answers."""

    if not isinstance(answer, str):
        return ""

    text = answer.strip()
    if not text:
        return ""

    banned_prefixes = (
        "here’s the best supported answer",
        "here's the best supported answer",
    )
    lines = [line.rstrip() for line in text.splitlines()]
    cleaned: List[str] = []
    skip_debug = False
    removed_any = False

    for raw_line in lines:
        line = raw_line.strip()
        normalized = line.lower()

        if not line:
            if skip_debug:
                skip_debug = False
                removed_any = True
                continue
            if cleaned and cleaned[-1]:
                cleaned.append("")
            continue

        if any(normalized.startswith(prefix) for prefix in banned_prefixes):
            removed_any = True
            continue

        if normalized.startswith("why this answer") or normalized.startswith("sources"):
            skip_debug = True
            removed_any = True
            continue

        if normalized.startswith("source ") and ":" in normalized:
            removed_any = True
            continue

        if normalized.startswith("- source "):
            removed_any = True
            continue

        if skip_debug:
            removed_any = True
            continue

        cleaned.append(line)

    collapsed = re.sub(r"(\n\s*){2,}", "\n\n", "\n".join(cleaned)).strip()
    if collapsed:
        return collapsed

    return "" if removed_any else text


def _dedup_sources(sources: Iterable[str], limit: int) -> List[str]:
    """Deduplicate source displays while keeping the last occurrence."""

    normalized: List[Tuple[str, str]] = []
    last_index: Dict[str, int] = {}

    for item in sources:
        key = _normalize_source_key(item)
        if not key:
            continue
        normalized.append((key, item))
        last_index[key] = len(normalized) - 1

    deduped: List[str] = []
    for idx, (key, original) in enumerate(normalized):
        if last_index.get(key) != idx:
            continue
        deduped.append(original)
        if len(deduped) >= max(0, limit):
            break

    return deduped


def _normalize_source_key(value: str) -> str:
    if not isinstance(value, str):
        return ""
    text = value.strip()
    if not text:
        return ""
    normalized = re.sub(r"\s+", " ", text)
    return normalized.strip().lower()


_CITATION_PATTERN = re.compile(r"\[\s*([A-Za-z0-9._:-]+)\s*\]")


def _extract_citation_ids(answer: str) -> List[str]:
    if not isinstance(answer, str) or not answer:
        return []
    ids: List[str] = []
    for match in _CITATION_PATTERN.finditer(answer):
        citation = match.group(1).strip()
        if citation:
            ids.append(citation)
    return ids


def _build_sources_from_facts(
    answer: str, facts: Iterable[Dict[str, Any]], limit: int
) -> List[str]:
    if limit <= 0:
        return []

    citation_ids = _extract_citation_ids(answer)
    key_to_entry: Dict[str, Dict[str, Any]] = {}
    citation_to_key: Dict[str, str] = {}
    ordered_entries: List[Tuple[str, Dict[str, Any]]] = []

    for idx, fact in enumerate(facts or []):
        if not isinstance(fact, dict):
            continue
        canonical_key = _canonical_fact_key(fact)
        if not canonical_key:
            continue
        entry = {"fact": fact, "index": idx}
        key_to_entry[canonical_key] = entry
        ordered_entries.append((canonical_key, entry))
        citation_id = str(fact.get("citation_id") or "").strip()
        if citation_id:
            citation_to_key[citation_id.lower()] = canonical_key

    ordered_keys: List[str] = []
    seen_keys: set[str] = set()

    for citation in citation_ids:
        key = citation_to_key.get(citation.lower())
        if key and key in key_to_entry and key not in seen_keys:
            ordered_keys.append(key)
            seen_keys.add(key)

    for canonical_key, entry in ordered_entries:
        if key_to_entry.get(canonical_key) is entry and canonical_key not in seen_keys:
            ordered_keys.append(canonical_key)
            seen_keys.add(canonical_key)

    sources: List[str] = []
    for canonical_key in ordered_keys:
        entry = key_to_entry.get(canonical_key)
        if not entry:
            continue
        display = _format_source_display(entry["fact"])
        if not display:
            continue
        sources.append(display)
        if len(sources) >= limit:
            break

    return sources


def _canonical_fact_key(fact: Dict[str, Any]) -> str:
    url = fact.get("url")
    if isinstance(url, str) and url.strip():
        normalized = _normalize_url(url)
        if normalized:
            return normalized
    for key in ("citation_id", "doc_id", "source_id"):
        value = fact.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip().lower()
    title = fact.get("title")
    if isinstance(title, str) and title.strip():
        return title.strip().lower()
    return ""


def _format_source_display(fact: Dict[str, Any]) -> str:
    title = str(fact.get("title") or "").strip() or "Untitled excerpt"
    source = str(fact.get("source_name") or "").strip()
    date = fact.get("date")
    date_text = str(date).strip() if isinstance(date, str) else ""
    display = title
    if source:
        display = f"{display} — {source}"
    if date_text:
        display = f"{display} ({date_text})"
    return display.strip()


def _normalize_url(url: str) -> str:
    try:
        parsed = urlparse(url)
    except Exception:
        return url.strip().lower()
    filtered_params = [
        (k, v)
        for k, v in parse_qsl(parsed.query, keep_blank_values=False)
        if not k.lower().startswith(("utm_", "session", "ref", "fbclid", "gclid"))
    ]
    query = urlencode(filtered_params, doseq=True)
    sanitized = parsed._replace(query=query, fragment="")
    if sanitized.scheme in {"http", "https"}:
        sanitized = sanitized._replace(netloc=sanitized.netloc.lower())
    return urlunparse(sanitized)


__all__ = [
    "gateway",
    "_clean_answer_text",
    "_dedup_sources",
    "_build_sources_from_facts",
]

