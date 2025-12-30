"""Thin wrapper around the Gemini CLI for the Gemini-first orchestration."""

from __future__ import annotations

import json
import os
import time
import logging
import re
from time import perf_counter
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from app.rag.gemini_cli import GeminiCLIError, gemini_call, get_last_error

from .settings import settings


LOGGER = logging.getLogger("app.gemini")




def _synth_from_facts(facts, *, max_bullets=3):
    if not isinstance(facts, (list, tuple)) or not facts:
        return ""
    lines = []
    for fact in list(facts)[:max_bullets]:
        if not isinstance(fact, dict):
            continue
        title = (fact.get("title") or fact.get("source_name") or "").strip()
        snippet = (fact.get("snippet") or fact.get("chunk_text") or "").strip()
        if title and snippet:
            lines.append(f"- {title}: {snippet[:240]}")
        elif title:
            lines.append(f"- {title}")
        elif snippet:
            lines.append(f"- {snippet[:240]}")
    if not lines:
        return ""
    return (
        "**Answer**\n"
        "Here is a brief summary from SustainaCore sources.\n\n"
        "**Key facts (from SustainaCore)**\n"
        + "\n".join(lines)
        + "\n\n"
        "**Evidence**\n"
        "- See the retrieved sources list for full context."
    )


def _parse_json(text: Optional[str]) -> Optional[Dict[str, Any]]:
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        LOGGER.warning("Gemini response was not valid JSON: %s", text[:2000])
        return None


def _extract_usage(payload: Optional[Dict[str, Any]]) -> Dict[str, Optional[int]]:
    usage: Dict[str, Optional[int]] = {"input_tokens": None, "output_tokens": None}
    if not isinstance(payload, dict):
        return usage
    usage_block = payload.get("usage") or payload.get("usageMetadata")
    if not isinstance(usage_block, dict):
        return usage

    input_candidates = [
        usage_block.get("promptTokenCount"),
        usage_block.get("promptTokens"),
        usage_block.get("input_tokens"),
    ]
    output_candidates = [
        usage_block.get("candidatesTokenCount"),
        usage_block.get("completionTokenCount"),
        usage_block.get("output_tokens"),
    ]

    for value in input_candidates:
        try:
            usage["input_tokens"] = int(value)  # type: ignore[arg-type]
            break
        except (TypeError, ValueError):
            continue

    for value in output_candidates:
        try:
            usage["output_tokens"] = int(value)  # type: ignore[arg-type]
            break
        except (TypeError, ValueError):
            continue

    return usage


class GeminiGateway:
    """Lightweight helper that enforces consistent prompts and parsing."""

    def __init__(self) -> None:
        self._timeout = settings.gemini_timeout
        self._last_meta: Dict[str, Any] = {}

    def _call_json(self, prompt: str, *, model: str) -> Optional[Dict[str, Any]]:
        start = perf_counter()
        text = gemini_call(prompt, timeout=self._timeout, model=model)
        latency_ms = int((perf_counter() - start) * 1000)

        if not text:
            error = get_last_error()
            code: Optional[int]
            err_line: str
            if isinstance(error, GeminiCLIError):
                code = error.returncode
                err_line = error.first_line or error.stderr.strip() or error.args[0]
            else:
                code = None
                err_line = "no_output"
            LOGGER.warning('gemini=fail code=%s err="%s"', code if code is not None else "none", err_line)
            self._last_meta = {"model": model, "lat_ms": latency_ms, "status": "fail", "code": code}
            return None

        payload = _parse_json(text)
        usage = _extract_usage(payload)

        if payload is None:
            LOGGER.warning('gemini=fail code=%s err="%s"', 0, "invalid_json")
            self._last_meta = {"model": model, "lat_ms": latency_ms, "status": "fail", "code": 0}
            return None

        in_tok = usage.get("input_tokens")
        out_tok = usage.get("output_tokens")
        LOGGER.info(
            "gemini=ok model=%s lat_ms=%d in_tok=%s out_tok=%s",
            model,
            latency_ms,
            in_tok if in_tok is not None else "-",
            out_tok if out_tok is not None else "-",
        )
        meta: Dict[str, Any] = {"model": model, "lat_ms": latency_ms, "status": "ok"}
        if in_tok is not None:
            meta["input_tokens"] = in_tok
        if out_tok is not None:
            meta["output_tokens"] = out_tok
        self._last_meta = meta
        return payload

    @property
    def last_meta(self) -> Dict[str, Any]:
        return dict(self._last_meta)

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
                "style": "short paragraphs, direct sentences, no filler",
                "citations": "every sentence must include [citation_id]",
                "format": "Answer, Key facts (from SustainaCore), Evidence sections",
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
            "Every sentence must include an inline citation like [FCA_2024_Guidance].\n"
            "Do not invent citation ids or facts.\n"
            "Keep the tone confident, concise, and human.\n"
            "Do not prefix the answer with explanations such as 'Here’s the best supported answer' or 'Why this answer'.\n"
            "Return ONLY JSON with keys: answer (string) and key_facts (array of strings).\n"
            "The answer must follow this exact format:\n"
            "Title line (optional)\n"
            "blank line\n"
            "**Answer**\n"
            "1-3 short paragraphs.\n"
            "blank line\n"
            "**Key facts (from SustainaCore)**\n"
            "- bullet list, max 5 bullets, one sentence each, each with citation.\n"
            "blank line\n"
            "**Evidence**\n"
            "- 2-5 bullets, each bullet includes a source title or id plus a short quoted snippet.\n"
            "Only use citation ids from retriever_result.facts.\n"
            f"Payload: {json.dumps(payload, ensure_ascii=False)}"
        )
        response = self._call_json(prompt, model=settings.gemini_model_answer) or {}
        answer = response.get("answer") if isinstance(response, dict) else None
        key_facts_raw = response.get("key_facts") if isinstance(response, dict) else None
        if not isinstance(answer, str):
            answer = "I’m sorry, I couldn’t generate an answer from the retrieved facts."
        cleaned_answer = _clean_answer_text(answer)

        raw_facts = retriever_result.get("facts") if isinstance(retriever_result, dict) else []
        fact_list = raw_facts if isinstance(raw_facts, list) else []
        derived_sources = _build_sources_from_facts(
            cleaned_answer, fact_list, settings.retriever_fact_cap
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

        formatted_answer = _format_final_answer(cleaned_answer, fact_list, key_facts_raw)
        composed: Dict[str, Any] = {
            "answer": formatted_answer,
            "sources": final_sources,
            "raw": response,
        }


        ## PATCH: parse code-block JSON
        raw_blobs = []
        if isinstance(response, dict):
            candidates = response.get('candidates')
            if isinstance(candidates, list):
                for candidate in candidates:
                    content = candidate.get('content') if isinstance(candidate, dict) else None
                    parts = content.get('parts') if isinstance(content, dict) else None
                    if isinstance(parts, list):
                        for part in parts:
                            text_val = part.get('text') if isinstance(part, dict) else None
                            if isinstance(text_val, str) and text_val:
                                raw_blobs.append(text_val)
        elif isinstance(response, str):
            raw_blobs.append(response)
        if raw_blobs:
            for blob in raw_blobs:
                match = re.search(r"```(?:json)?\s*(.*?)\s*```", blob, re.S | re.I)
                if not match:
                    continue
                candidate = match.group(1)
                try:
                    parsed = json.loads(candidate)
                except Exception:
                    continue
                if not isinstance(parsed, dict):
                    continue
                ans_val = (parsed.get('answer') or '').strip()
                if ans_val:
                    composed['answer'] = ans_val
                parsed_sources = parsed.get('sources')
                if isinstance(parsed_sources, list) and parsed_sources:
                    composed['sources'] = parsed_sources
                break


        try:
            trace = os.environ.get("ASK2_TRACE_ID", "")
            ans_preview = (composed.get("answer") or "").strip()
            head = ans_preview[:180].replace("\n", " ")
            raw_payload = ""
            if isinstance(response, dict):
                raw_payload = json.dumps(response, ensure_ascii=False)
            with open("/tmp/gemini_compose_raw.log", "a", encoding="utf-8") as fh:
                ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                preview = raw_payload[:4000]
                fh.write(f"[COMPOSE_RAW] ts={ts} trace={trace} ans_len={len(ans_preview)} head={head} raw={preview}\n")
        except Exception:
            pass

        fallback_flag = os.getenv("ASK2_SYNTH_FALLBACK", "0").strip().lower()
        default_apology = "I’m sorry, I couldn’t generate an answer from the retrieved facts."
        if fallback_flag not in {"0", "false", "no", "off"}:
            try:
                ans_preview = (composed.get("answer") or "").strip()
                if (not ans_preview or ans_preview == default_apology) and fact_list:
                    synth = _synth_from_facts(fact_list)
                    if synth:
                        composed["answer"] = synth
            except Exception:
                pass

        return composed


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


def _strip_section_markers(text: str) -> str:
    if not isinstance(text, str):
        return ""
    lines = []
    for raw in text.splitlines():
        line = raw.strip()
        lowered = line.lower().strip("*").strip()
        if lowered in {"answer", "key facts (from sustainacore)", "key facts", "evidence", "sources"}:
            continue
        if lowered.startswith("key facts"):
            continue
        if lowered.startswith("evidence"):
            continue
        lines.append(raw)
    return "\n".join(lines).strip()

def _normalize_bullet_runs(text: str) -> str:
    if not isinstance(text, str) or not text:
        return ""
    if text.count(" - ") >= 2:
        return text.replace(" - ", "\n- ")
    return text


def _split_paragraphs(text: str) -> List[str]:
    if not isinstance(text, str) or not text.strip():
        return []
    paragraphs = [p.strip() for p in re.split(r"\n\s*\n", text) if p.strip()]
    if paragraphs:
        return paragraphs
    return [text.strip()]


def _sentence_split(text: str) -> List[str]:
    if not isinstance(text, str) or not text.strip():
        return []
    parts = re.split(r"(?<=[.!?])\s+(?=[A-Z0-9])", text.strip())
    return [p.strip() for p in parts if p.strip()]


def _strip_citations(text: str) -> str:
    if not isinstance(text, str):
        return ""
    cleaned = _CITATION_PATTERN.sub("", text)
    cleaned = re.sub(r"\s{2,}", " ", cleaned)
    return cleaned.strip()


def _sentences_with_valid_citations(text: str, valid_ids: set[str]) -> List[str]:
    sentences = _sentence_split(text)
    kept: List[str] = []
    for sentence in sentences:
        citations = _extract_citation_ids(sentence)
        if not citations:
            continue
        if not any(cit.lower() in valid_ids for cit in citations):
            continue
        stripped = _strip_citations(sentence)
        if stripped:
            kept.append(stripped)
    return kept


def _first_sentence(text: str, *, max_len: int = 240) -> str:
    if not isinstance(text, str) or not text.strip():
        return ""
    collapsed = re.sub(r"\s+", " ", text.strip())
    sentences = _sentence_split(collapsed)
    if sentences:
        sentence = sentences[0]
    else:
        sentence = collapsed
    if len(sentence) > max_len:
        sentence = sentence[: max_len - 3].rstrip() + "..."
    if sentence and sentence[-1] not in ".!?":
        sentence += "."
    return sentence


def _shorten_snippet(text: str, *, max_len: int = 220) -> str:
    if not isinstance(text, str) or not text.strip():
        return ""
    collapsed = re.sub(r"\s+", " ", text.strip())
    if len(collapsed) <= max_len:
        return collapsed
    return collapsed[: max_len - 3].rstrip() + "..."


def _build_key_facts_from_facts(facts: List[Dict[str, Any]], max_bullets: int = 5) -> List[str]:
    bullets: List[str] = []
    seen: set[str] = set()
    for fact in facts:
        if not isinstance(fact, dict):
            continue
        snippet = fact.get("snippet") or fact.get("chunk_text") or ""
        sentence = _first_sentence(str(snippet))
        if not sentence:
            continue
        key = sentence.lower()
        if key in seen:
            continue
        seen.add(key)
        bullets.append(sentence)
        if len(bullets) >= max_bullets:
            break
    return bullets


def _build_evidence_bullets(facts: List[Dict[str, Any]], *, max_bullets: int = 5) -> List[str]:
    bullets: List[str] = []
    for fact in facts:
        if not isinstance(fact, dict):
            continue
        title = str(fact.get("title") or fact.get("source_name") or "").strip()
        citation_id = str(fact.get("citation_id") or "").strip()
        snippet = _shorten_snippet(str(fact.get("snippet") or fact.get("chunk_text") or ""))
        if not snippet:
            continue
        label = title or citation_id or "Source"
        if citation_id and citation_id not in label:
            label = f"{label} (ID: {citation_id})"
        bullets.append(f'{label}: "{snippet}"')
        if len(bullets) >= max_bullets:
            break
    return bullets


def _build_key_facts_from_llm(items: object, valid_ids: set[str], max_bullets: int = 5) -> List[str]:
    bullets: List[str] = []
    if not isinstance(items, list):
        return bullets
    for raw in items:
        if not isinstance(raw, str):
            continue
        raw = raw.lstrip("-").strip()
        citations = _extract_citation_ids(raw)
        if not citations or not any(cit.lower() in valid_ids for cit in citations):
            continue
        cleaned = _strip_citations(raw)
        if cleaned:
            if cleaned[-1] not in ".!?":
                cleaned += "."
            bullets.append(cleaned)
        if len(bullets) >= max_bullets:
            break
    return bullets


def _summarize_facts_for_answer(facts: List[Dict[str, Any]]) -> List[str]:
    if not facts:
        return []
    snippets = []
    for fact in facts[:3]:
        snippet = _first_sentence(str(fact.get("snippet") or fact.get("chunk_text") or ""))
        if snippet:
            snippets.append(snippet)
    if not snippets:
        return []
    return [" ".join(snippets)]


def _format_final_answer(answer_text: str, facts: List[Dict[str, Any]], key_facts_raw: object) -> str:
    valid_ids = {str(f.get("citation_id") or "").strip().lower() for f in facts if isinstance(f, dict)}
    valid_ids = {vid for vid in valid_ids if vid}

    stripped = _normalize_bullet_runs(_strip_section_markers(answer_text))
    paragraphs: List[str] = []
    for paragraph in _split_paragraphs(stripped):
        sentences = _sentences_with_valid_citations(paragraph, valid_ids) if valid_ids else []
        if sentences:
            paragraphs.append(" ".join(sentences))
    if not paragraphs:
        paragraphs = _summarize_facts_for_answer(facts)

    if not paragraphs:
        paragraphs = ["I could not find enough SustainaCore context to answer this question."]

    if len(facts) < 2:
        paragraphs.append("Coverage looks thin based on the retrieved snippets.")

    key_facts = _build_key_facts_from_llm(key_facts_raw, valid_ids)
    if not key_facts:
        key_facts = _build_key_facts_from_facts(facts)
    if not key_facts:
        key_facts = ["No high-confidence facts were retrieved for this query."]

    evidence = _build_evidence_bullets(facts)
    if not evidence:
        evidence = ["No evidence snippets were returned by the retriever."]

    answer_lines = ["**Answer**"] + paragraphs
    key_fact_lines = ["", "**Key facts (from SustainaCore)**"] + [f"- {item}" for item in key_facts[:5]]
    evidence_lines = ["", "**Evidence**"] + [f"- {item}" for item in evidence[:5]]

    cap = int(os.getenv("ASK2_ANSWER_CHAR_CAP", "1200"))
    if cap > 0:
        evidence_text = "\n".join(evidence_lines).strip()
        remaining = max(cap - len(evidence_text) - 2, 0)
        content = "\n".join(answer_lines + key_fact_lines).strip()
        if remaining and len(content) > remaining:
            content = content[:remaining].rstrip()
        combined = "\n".join([content, evidence_text]).strip()
    else:
        combined = "\n".join(answer_lines + key_fact_lines + evidence_lines).strip()
    return combined


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
