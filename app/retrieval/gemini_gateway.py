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
        "**Key facts**\n"
        + "\n".join(lines)
        + "\n\n"
        "**Sources**\n"
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
            "\n"
            "Corpus routing guidance (SOURCE_TYPE values):\n"
            "- TECH100 methodology, scoring, definitions: SOURCE_TYPE=['methodology']\n"
            "- TECH100 constituents/membership: SOURCE_TYPE=['membership']\n"
            "- TECH100 public company pages: SOURCE_TYPE=['company_profile']\n"
            "- TECH100 performance/attribution/stats pages: SOURCE_TYPE=['performance']\n"
            "- Global AI regulation database: SOURCE_TYPE=['regulatory']\n"
            "- SustainaCore news pages: SOURCE_TYPE=['news_release']\n"
            "- About/FAQ/site pages: SOURCE_TYPE=['about_site']\n"
            "\n"
            "If the user asks about a specific company/ticker and it is clearly a public TECH100 company question:\n"
            "- Prefer SOURCE_TYPE=['company_profile'] and set SOURCE_ID to the ticker when confidently detected.\n"
            "If the user asks about a specific regulation jurisdiction:\n"
            "- Prefer SOURCE_TYPE=['regulatory'] and set SOURCE_ID to the ISO2 jurisdiction code when confidently detected.\n"
            "\n"
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
                "format": "Answer, Key facts, Sources sections",
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
            "**Answer**\n"
            "1-2 short paragraphs.\n"
            "\n"
            "**Key facts**\n"
            "- bullet list, 3-5 bullets, one sentence each.\n"
            "\n"
            "**Sources**\n"
            "1. Title — URL (if available)\n"
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
        raw_contexts = retriever_result.get("contexts") if isinstance(retriever_result, dict) else []
        if isinstance(raw_contexts, list):
            context_facts = _facts_from_contexts(raw_contexts)
            if context_facts:
                fact_list = _merge_fact_lists(
                    fact_list,
                    context_facts,
                    settings.retriever_fact_cap,
                )
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

        formatted_answer = _format_final_answer(question, cleaned_answer, fact_list, key_facts_raw)
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


_JUNK_PREFIXES = (
    "te.",
    "sibility.",
    "key sco",
    "e investment advice",
    "meta:",
)

_JUNK_PATTERNS = (
    "investment advice",
    "key sco",
)


def _strip_citations(text: str) -> str:
    if not isinstance(text, str):
        return ""
    cleaned = _CITATION_PATTERN.sub("", text)
    cleaned = re.sub(r"\[\d+\]", "", cleaned)
    cleaned = re.sub(r"\s{2,}", " ", cleaned)
    return cleaned.strip()


def _normalize_punctuation(text: str) -> str:
    if not isinstance(text, str):
        return ""
    cleaned = text.replace("•", " ")
    cleaned = re.sub(r"\s*\.\s*\.\s*", ". ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned)
    cleaned = re.sub(r"\s+([.,;:])", r"\1", cleaned)
    cleaned = re.sub(r"([.,;:])([^\s])", r"\1 \2", cleaned)
    cleaned = re.sub(r"([A-Za-z0-9])\.\s+(com|org|net|io|ai|gov|edu|co|us|uk|info|biz)\b", r"\1.\2", cleaned, flags=re.I)
    cleaned = re.sub(r"\s{2,}", " ", cleaned)
    return cleaned.strip()


def _looks_fragmentary(text: str) -> bool:
    if not isinstance(text, str):
        return True
    stripped = text.strip()
    if not stripped:
        return True
    lowered = stripped.lower()
    if any(lowered.startswith(prefix) for prefix in _JUNK_PREFIXES):
        return True
    if "card_path=" in lowered or "doc_ref=" in lowered or "aliases=" in lowered:
        return True
    if len(stripped) < 30 and not _has_numeric_fact(stripped):
        return True
    if stripped.lower().startswith(("by ", "and ", "or ", "as ", "to ", "of ")):
        return True
    if stripped[0].islower() and len(stripped) < 80:
        return True
    if ". ." in stripped or ".." in stripped:
        return True
    if any(pat in lowered for pat in _JUNK_PATTERNS) and len(stripped) < 80:
        return True
    return False


def _sanitize_snippet(text: str) -> str:
    if not isinstance(text, str):
        return ""
    cleaned = _normalize_punctuation(_strip_citations(text))
    cleaned = cleaned.strip(" \"'“” .;:-")
    if _looks_fragmentary(cleaned):
        return ""
    return cleaned


def _split_sentences(text: str) -> List[str]:
    if not isinstance(text, str) or not text.strip():
        return []
    parts = re.split(r"(?<=[.!?])\s+(?=[A-Z0-9])", text.strip())
    return [p.strip() for p in parts if p.strip()]


def _clean_fact_sentence(text: str) -> str:
    cleaned = _normalize_punctuation(text)
    cleaned = re.sub(r"^Definition\s*[-–:]\s*", "", cleaned, flags=re.I)
    cleaned = re.sub(r"^As noted,\s*", "", cleaned, flags=re.I)
    if cleaned.lower().startswith("meta:"):
        return ""
    if ">" in cleaned or "›" in cleaned:
        return ""
    cleaned = cleaned.strip()
    if _looks_fragmentary(cleaned):
        return ""
    if not re.search(
        r"\b(is|are|was|were|has|have|measures|measured|ranks|ranked|includes|included|contains|refreshes|rebalances|receives|receive|derived|derives|forms|form|designed|tracks|targets|uses|built|based)\b",
        cleaned,
        re.I,
    ) and not _has_numeric_fact(cleaned):
        return ""
    return cleaned


def _has_numeric_fact(text: str) -> bool:
    return bool(re.search(r"\b\d+(?:\.\d+)?%?\b", text))


def _extract_fact_sentences(snippet: str) -> List[str]:
    cleaned = _sanitize_snippet(snippet)
    if not cleaned:
        return []
    sentences = _split_sentences(cleaned)
    out: List[str] = []
    for sentence in sentences:
        fact = _clean_fact_sentence(sentence)
        if fact:
            out.append(fact)
    return out


def _keyword_sentences(snippet: str, keywords: List[str]) -> List[str]:
    if not keywords or not isinstance(snippet, str):
        return []
    cleaned = _normalize_punctuation(_strip_citations(snippet))
    text = cleaned or snippet
    out: List[str] = []
    lowered_keywords = [kw.lower() for kw in keywords if kw]
    for sentence in _split_sentences(text):
        lowered = sentence.lower()
        if any(kw in lowered for kw in lowered_keywords):
            candidate = sentence.strip()
            if candidate.lower().startswith("meta:"):
                continue
            if "card_path=" in candidate.lower() or "doc_ref=" in candidate.lower() or "aliases=" in candidate.lower():
                continue
            if candidate and len(candidate) >= 20:
                out.append(candidate)
    return out


def _score_fact(sentence: str) -> int:
    score = 0
    lowered = sentence.lower()
    if _has_numeric_fact(sentence):
        score += 2
    if any(term in lowered for term in ("rank", "weight", "rebalance", "quarter", "annual", "score", "index")):
        score += 2
    if any(term in lowered for term in ("includes", "included", "contains", "comprises")):
        score += 1
    return score


def _question_keywords(question: str) -> List[str]:
    if not isinstance(question, str):
        return []
    tokens = re.findall(r"[A-Za-z0-9]+", question.lower())
    stop = {
        "is",
        "are",
        "the",
        "a",
        "an",
        "of",
        "in",
        "on",
        "and",
        "or",
        "to",
        "for",
        "does",
        "do",
        "did",
        "what",
        "how",
        "who",
        "when",
        "where",
        "why",
        "show",
        "me",
        "summarize",
        "latest",
        "by",
    }
    return [t for t in tokens if t not in stop and len(t) > 2]


_GENERIC_QUERY_TERMS = {
    "tech100",
    "index",
    "score",
    "ai",
    "governance",
    "ethics",
    "sustainacore",
    "latest",
    "headlines",
    "headline",
    "news",
    "top",
    "company",
    "companies",
}


def _select_key_facts(
    facts: List[Dict[str, Any]], max_items: int = 3, keywords: Optional[List[str]] = None
) -> List[str]:
    candidates: List[Tuple[int, str]] = []
    for fact in facts:
        if not isinstance(fact, dict):
            continue
        snippet = str(fact.get("snippet") or fact.get("chunk_text") or "")
        for sentence in _extract_fact_sentences(snippet):
            candidates.append((_score_fact(sentence), sentence))
        for sentence in _keyword_sentences(snippet, keywords or []):
            candidates.append((_score_fact(sentence) + 1, sentence))
    candidates.sort(key=lambda x: x[0], reverse=True)
    keywords = [kw for kw in (keywords or []) if kw]
    preferred: List[Tuple[int, str]] = []
    if keywords:
        for score, sentence in candidates:
            lowered = sentence.lower()
            if any(kw in lowered for kw in keywords):
                preferred.append((score + 2, sentence))
    ordered = preferred if preferred else candidates
    out: List[str] = []
    seen: set[str] = set()
    for _, sentence in ordered:
        key = _normalize_sentence(sentence)
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(sentence)
        if len(out) >= max_items:
            break
    return out


def _facts_for_key_facts(facts: List[Dict[str, Any]], key_facts: List[str]) -> List[Dict[str, Any]]:
    matched: List[Dict[str, Any]] = []
    for fact in facts:
        if not isinstance(fact, dict):
            continue
        snippet = _sanitize_snippet(str(fact.get("snippet") or fact.get("chunk_text") or ""))
        snippet_norm = _normalize_sentence(snippet)
        for key_fact in key_facts:
            if not key_fact:
                continue
            if _normalize_sentence(key_fact) in snippet_norm:
                matched.append(fact)
                break
    return matched or facts

def _resolve_source_url(title: str, url: str) -> str:
    if isinstance(url, str) and url.strip():
        try:
            parsed = urlparse(url.strip())
        except Exception:
            parsed = None
        if parsed and parsed.scheme in {"http", "https"} and parsed.netloc:
            host = (parsed.hostname or "").lower()
            if host.endswith("sustainacore.org"):
                path = parsed.path or "/"
                return f"https://sustainacore.org{path}"
    lower_title = (title or "").lower()
    if "contact" in lower_title:
        return "https://sustainacore.org/contact"
    if "press" in lower_title:
        return "https://sustainacore.org/press"
    if "news" in lower_title or "headline" in lower_title:
        return "https://sustainacore.org/news"
    if "constituent" in lower_title:
        return "https://sustainacore.org/tech100/constituents"
    if "performance" in lower_title:
        return "https://sustainacore.org/tech100/performance"
    if "stats" in lower_title or "score" in lower_title or "metrics" in lower_title:
        return "https://sustainacore.org/tech100/stats"
    if "tech100" in lower_title or "index" in lower_title or "methodology" in lower_title:
        return "https://sustainacore.org/tech100"
    if "about" in lower_title or "sustainacore" in lower_title:
        return "https://sustainacore.org"
    return ""


def _normalize_sentence(text: str) -> str:
    if not isinstance(text, str):
        return ""
    cleaned = re.sub(r"[^a-z0-9]+", " ", text.lower()).strip()
    return cleaned


def _dedupe_sentences(sentences: List[str], seen: Optional[set[str]] = None) -> Tuple[List[str], set[str]]:
    if seen is None:
        seen = set()
    out: List[str] = []
    for sentence in sentences:
        key = _normalize_sentence(sentence)
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(sentence)
    return out, seen


def _sentences_with_valid_citations(text: str, valid_ids: set[str]) -> List[str]:
    sentences = _sentence_split(text)
    kept: List[str] = []
    for sentence in sentences:
        citations = _extract_citation_ids(sentence)
        if not citations:
            continue
        if not any(cit.lower() in valid_ids for cit in citations):
            continue
        stripped = _sanitize_snippet(sentence)
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
        cut = max(sentence.rfind(".", 0, max_len), sentence.rfind("!", 0, max_len), sentence.rfind("?", 0, max_len))
        sentence = sentence[: cut + 1].rstrip() if cut > 0 else sentence[:max_len].rstrip()
    if sentence and sentence[-1] not in ".!?":
        sentence += "."
    return sentence


def _shorten_snippet(text: str, *, max_len: int = 160) -> str:
    if not isinstance(text, str) or not text.strip():
        return ""
    collapsed = _normalize_punctuation(text.strip())
    if len(collapsed) <= max_len:
        return collapsed
    cut = max(collapsed.rfind(".", 0, max_len), collapsed.rfind("!", 0, max_len), collapsed.rfind("?", 0, max_len))
    if cut > 0:
        return collapsed[: cut + 1].rstrip()
    return collapsed[:max_len].rstrip()


def _build_key_facts_from_facts(facts: List[Dict[str, Any]], max_bullets: int = 5) -> List[str]:
    bullets: List[str] = []
    seen: set[str] = set()
    for fact in facts:
        if not isinstance(fact, dict):
            continue
        snippet = fact.get("snippet") or fact.get("chunk_text") or ""
        candidates = _extract_fact_sentences(str(snippet))
        if not candidates:
            continue
        for sentence in candidates:
            key = sentence.lower()
            if key in seen:
                continue
            seen.add(key)
            bullets.append(sentence)
            break
        if len(bullets) >= max_bullets:
            break
    return bullets


def _build_sources_list(facts: List[Dict[str, Any]], *, max_items: int = 3) -> List[str]:
    sources: List[str] = []
    seen: set[str] = set()
    for fact in facts:
        if not isinstance(fact, dict):
            continue
        title = str(fact.get("title") or fact.get("source_name") or "").strip()
        if ">" in title:
            title = title.split(">")[-1].strip()
        if "›" in title:
            title = title.split("›")[-1].strip()
        url = str(fact.get("source_url") or fact.get("url") or "").strip()
        if not title and not url:
            continue
        if url.startswith(("local://", "file://", "internal://")):
            url = ""
        url = _resolve_source_url(title, url)
        if not url:
            continue
        key = f"{title.lower()}|{url.lower()}"
        if key in seen:
            continue
        seen.add(key)
        label = f"{title} — {url}" if title else url
        sources.append(label)
        if len(sources) >= max_items:
            break
    return sources


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
        cleaned = _sanitize_snippet(raw)
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
        snippet = _first_sentence(_sanitize_snippet(str(fact.get("snippet") or fact.get("chunk_text") or "")))
        if snippet:
            snippets.append(snippet)
    if not snippets:
        return []
    return [" ".join(snippets)]


def _fact_snippet_norms(facts: List[Dict[str, Any]]) -> List[str]:
    norms: List[str] = []
    for fact in facts:
        if not isinstance(fact, dict):
            continue
        snippet = _sanitize_snippet(str(fact.get("snippet") or fact.get("chunk_text") or ""))
        if not snippet:
            continue
        norms.append(_normalize_sentence(snippet))
    return norms

def _ownership_intent(question: str) -> bool:
    lowered = (question or "").lower()
    return any(term in lowered for term in ("own", "owner", "created", "creator", "founded", "admin"))


def _news_intent(question: str) -> bool:
    lowered = (question or "").lower()
    return "headline" in lowered or "headlines" in lowered or "news" in lowered


def _filter_news_facts(facts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    filtered: List[Dict[str, Any]] = []
    for fact in facts:
        if not isinstance(fact, dict):
            continue
        title = str(fact.get("title") or "").lower()
        source = str(fact.get("source_name") or "").lower()
        if "news" in title or "headline" in title or "news" in source:
            filtered.append(fact)
    return filtered

def _facts_with_titles(facts: List[Dict[str, Any]], keywords: Tuple[str, ...]) -> List[Dict[str, Any]]:
    matched: List[Dict[str, Any]] = []
    for fact in facts:
        if not isinstance(fact, dict):
            continue
        title = str(fact.get("title") or fact.get("source_name") or "").lower()
        if any(key in title for key in keywords):
            matched.append(fact)
    return matched


def _ranking_intent(question: str) -> bool:
    lowered = (question or "").lower()
    return any(term in lowered for term in ("top companies", "top company", "highest", "ranked", "ranking", "top by"))


def _rank_sentences(facts: List[Dict[str, Any]]) -> List[str]:
    sentences: List[str] = []
    for fact in facts:
        if not isinstance(fact, dict):
            continue
        snippet = str(fact.get("snippet") or fact.get("chunk_text") or "")
        cleaned = _normalize_punctuation(_strip_citations(snippet))
        for sentence in _split_sentences(cleaned):
            if not re.search(r"\branks?\b", sentence, re.I):
                continue
            if not _has_numeric_fact(sentence):
                continue
            if not re.search(r"\b[A-Z][a-z]", sentence):
                continue
            if _looks_fragmentary(sentence):
                continue
            sentences.append(sentence.strip())
    return sentences


def _facts_from_contexts(contexts: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    facts: List[Dict[str, Any]] = []
    for idx, ctx in enumerate(contexts or []):
        if not isinstance(ctx, dict):
            continue
        citation = ctx.get("doc_id") or ctx.get("id") or f"CTX_{idx+1}"
        fact = {
            "citation_id": str(citation),
            "title": ctx.get("title") or "",
            "source_name": ctx.get("source_name") or "",
            "source_url": ctx.get("source_url") or "",
            "snippet": ctx.get("snippet") or ctx.get("chunk_text") or "",
            "score": ctx.get("score"),
        }
        facts.append(fact)
    return facts


def _merge_fact_lists(primary: List[Dict[str, Any]], secondary: List[Dict[str, Any]], limit: int) -> List[Dict[str, Any]]:
    seen: set[str] = set()
    merged: List[Dict[str, Any]] = []
    for fact in (primary or []) + (secondary or []):
        if not isinstance(fact, dict):
            continue
        key = _canonical_fact_key(fact) or str(len(merged))
        if key in seen:
            continue
        seen.add(key)
        merged.append(fact)
        if limit > 0 and len(merged) >= limit:
            break
    return merged


def _has_ownership_statement(facts: List[Dict[str, Any]]) -> bool:
    markers = ("owned by", "owner", "owned", "created by", "creator", "founded by", "founded", "operated by", "run by", "administered by")
    for fact in facts:
        if not isinstance(fact, dict):
            continue
        snippet = _sanitize_snippet(str(fact.get("snippet") or fact.get("chunk_text") or ""))
        lowered = snippet.lower()
        if any(marker in lowered for marker in markers):
            return True
    return False


def _postprocess_formatted(text: str) -> str:
    if not isinstance(text, str):
        return ""
    out = re.sub(r"\s*•\s*", "\n- ", text)
    out = re.sub(r"\n{3,}", "\n\n", out)
    out = re.sub(r"\n-\s*", "\n- ", out)
    out = out.replace("\r\n", "\n").strip()
    out = out.replace("...", "")
    out = re.sub(r"\n\*\*Key facts\*\*", "\n\n**Key facts**", out)
    out = re.sub(r"\n\*\*Sources\*\*", "\n\n**Sources**", out)
    if "**Sources**" not in out and re.search(r"^\d+\\. ", out, re.M):
        out = re.sub(r"\n+(\\d+\\.)", "\n\n**Sources**\n\\1", out, count=1)
    return out


def _ensure_period(text: str) -> str:
    if not isinstance(text, str) or not text.strip():
        return ""
    cleaned = text.strip()
    if cleaned[-1] not in ".!?":
        cleaned += "."
    return cleaned


def _fit_content(answer_lines: List[str], key_fact_lines: List[str], max_len: int) -> str:
    def _joined(lines: List[str]) -> str:
        return "\n".join(lines).strip()

    if max_len <= 0:
        return _joined(answer_lines + key_fact_lines)

    header = key_fact_lines[:1]
    bullets = key_fact_lines[1:]
    key_block = _joined(header + bullets)
    while bullets and len(key_block) > max_len:
        bullets = bullets[:-1]
        key_block = _joined(header + bullets)
    if not key_block or len(key_block) > max_len:
        key_block = _joined(
            [
                "**Key facts**",
                "- See the Sources section for details.",
            ]
        )

    remaining = max_len - len(key_block) - 4
    answer_block = _joined(answer_lines)
    if remaining <= 0:
        answer_block = "**Answer**\nSee the Sources section for the retrieved references."
    elif len(answer_block) > remaining:
        answer_block = answer_block[:remaining].rstrip()

    return _joined([answer_block, "", key_block])


def _format_final_answer(question: str, answer_text: str, facts: List[Dict[str, Any]], key_facts_raw: object) -> str:
    valid_ids = {str(f.get("citation_id") or "").strip().lower() for f in facts if isinstance(f, dict)}
    valid_ids = {vid for vid in valid_ids if vid}

    stripped = _normalize_bullet_runs(_strip_section_markers(answer_text))
    paragraphs: List[str] = []
    filtered_facts = facts
    if _news_intent(question):
        news_facts = _filter_news_facts(facts)
        if news_facts:
            filtered_facts = news_facts
    for paragraph in _split_paragraphs(stripped):
        sentences = _sentences_with_valid_citations(paragraph, valid_ids) if valid_ids else []
        if sentences:
            deduped, _ = _dedupe_sentences(sentences)
            if deduped:
                paragraphs.append(" ".join(deduped))
    if not paragraphs:
        paragraphs = _summarize_facts_for_answer(filtered_facts)

    if not paragraphs:
        paragraphs = ["I could not find enough SustainaCore context to answer this question."]

    if len(filtered_facts) < 2:
        paragraphs.append("Coverage looks thin based on the retrieved snippets.")
    if len(paragraphs) > 2:
        paragraphs = paragraphs[:2]

    keywords = _question_keywords(question)
    focus_terms = [kw for kw in keywords if kw not in _GENERIC_QUERY_TERMS]
    key_facts = _select_key_facts(filtered_facts, max_items=3, keywords=focus_terms or keywords)
    ranked_sentences: List[str] = []
    if _ranking_intent(question):
        ranked_sentences = _rank_sentences(filtered_facts)
        if ranked_sentences:
            key_facts = ranked_sentences[:3]
    if not key_facts:
        key_facts = ["No high-confidence facts were retrieved for this query."]
    if (
        paragraphs
        and paragraphs[0].startswith("I could not find enough SustainaCore context")
        and key_facts
        and not key_facts[0].startswith("No high-confidence")
    ):
        paragraphs = [f"Based on SustainaCore sources, {key_facts[0].rstrip('.')}."] + (
            [f"In addition, {key_facts[1].rstrip('.')}."] if len(key_facts) > 1 else []
        )
    if _ranking_intent(question) and ranked_sentences:
        paragraphs = [
            "Here are the top companies by AI Governance & Ethics Score mentioned in the retrieved snippets."
        ]

    seen: set[str] = set()
    paragraph_sents: List[str] = []
    for para in paragraphs:
        paragraph_sents.extend(_split_sentences(para))
    _, seen = _dedupe_sentences(paragraph_sents, seen)
    key_facts, seen = _dedupe_sentences(key_facts, seen)
    if len(key_facts) < 3:
        extras = _build_key_facts_from_facts(filtered_facts)
        for item in extras:
            if item in key_facts:
                continue
            key_facts.append(item)
            if len(key_facts) >= 3:
                break
    if len(key_facts) < 2:
        key_facts.append("The retrieved snippets do not include a ranked company list for this question.")

    source_facts = _facts_for_key_facts(filtered_facts, key_facts)
    sources = _build_sources_list(source_facts, max_items=3)
    if not sources:
        sources = _build_sources_list(filtered_facts, max_items=3)
    if not sources:
        sources = ["SustainaCore — https://sustainacore.org"]

    if _ownership_intent(question) and not _has_ownership_statement(facts):
        about_facts = _facts_with_titles(facts, ("about", "contact"))
        if not about_facts:
            about_facts = facts
        paragraphs = [
            "SustainaCore pages do not explicitly state the owner or creator of sustainacore.org.",
            "For administrative details, refer to the SustainaCore About or Contact page.",
        ]
        key_facts = _build_key_facts_from_facts(about_facts)
        if not key_facts:
            key_facts = ["The About and Contact pages are the only relevant sources in the retrieved results."]
        if len(key_facts) < 3:
            for fact in about_facts:
                snippet = _sanitize_snippet(str(fact.get("snippet") or fact.get("chunk_text") or ""))
                for sentence in _sentence_split(snippet):
                    if _normalize_sentence(sentence) in {_normalize_sentence(k) for k in key_facts}:
                        continue
                    key_facts.append(sentence)
                    if len(key_facts) >= 3:
                        break
                if len(key_facts) >= 3:
                    break
        sources = _build_sources_list(about_facts, max_items=3)
        if not sources:
            sources = ["SustainaCore — https://sustainacore.org"]

    key_facts = [_ensure_period(item) for item in key_facts if item]
    if _news_intent(question) and not _filter_news_facts(facts):
        paragraphs = [
            "I couldn’t find recent headlines in the indexed news set right now.",
            "Here are the most relevant governance items I found instead.",
        ]

    synthesized = paragraphs
    if not synthesized:
        synthesized = []
    if not synthesized and key_facts and not key_facts[0].startswith("No high-confidence"):
        synthesized.append(f"Based on SustainaCore sources, {key_facts[0].rstrip('.')}.")
        if len(key_facts) > 1:
            synthesized.append(f"In addition, {key_facts[1].rstrip('.')}.")
    if not synthesized:
        synthesized = paragraphs or ["I could not find enough SustainaCore context to answer this question."]

    answer_lines = ["**Answer**"]
    for idx, para in enumerate(synthesized[:2]):
        answer_lines.append(para)
        if idx < len(synthesized[:2]) - 1:
            answer_lines.append("")
    key_fact_lines = ["**Key facts**"] + [f"- {item}" for item in key_facts[:4]]
    source_lines = ["**Sources**"] + [f"{idx}. {item}" for idx, item in enumerate(sources[:3], start=1)]

    cap = int(os.getenv("ASK2_ANSWER_CHAR_CAP", "2000"))
    if cap > 0:
        sources_text = "\n".join(source_lines).strip()
        remaining = max(cap - len(sources_text) - 2, 0)
        content = _fit_content(answer_lines, key_fact_lines, remaining)
        if not content:
            content = "\n".join(
                [
                    "**Answer**",
                    "See the Sources section for the retrieved references.",
                    "",
                    "**Key facts**",
                    "- See the Sources section for details.",
                ]
            )
        combined = "\n".join([content, "", sources_text]).strip()
    else:
        combined = "\n".join(answer_lines + [""] + key_fact_lines + [""] + source_lines).strip()
    return _postprocess_formatted(combined)


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
    title = str(fact.get("title") or fact.get("source_name") or "").strip()
    if ">" in title:
        title = title.split(">")[-1].strip()
    if "›" in title:
        title = title.split("›")[-1].strip()
    raw_url = str(fact.get("source_url") or fact.get("url") or "").strip()
    url = _resolve_source_url(title, raw_url)
    if not url:
        return ""
    if not title:
        title = "SustainaCore"
    return f"{title} — {url}".strip()


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
