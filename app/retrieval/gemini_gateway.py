"""Thin wrapper around the Gemini CLI for the Gemini-first orchestration."""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

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
            },
        }
        prompt = (
            "You are the Gemini composer for SustainaCore.\n"
            "Use ONLY the facts in retriever_result.facts to answer the question.\n"
            "Every claim referencing retrieved evidence must include an inline citation like [FCA_2024_Guidance].\n"
            "Do not invent citation ids or facts.\n"
            "Keep the tone confident, concise, and human.\n"
            "Return ONLY JSON with keys: answer (string) and sources (array of strings formatted 'Title — Publisher (Date)').\n"
            "Do not include debug sections or numbered sources.\n"
            f"Payload: {json.dumps(payload, ensure_ascii=False)}"
        )
        response = self._call_json(prompt, model=settings.gemini_model_answer) or {}
        answer = response.get("answer") if isinstance(response, dict) else None
        if not isinstance(answer, str):
            answer = "I’m sorry, I couldn’t generate an answer from the retrieved facts."
        sources = response.get("sources") if isinstance(response, dict) else None
        if not isinstance(sources, list):
            sources = []
        cleaned_sources: List[str] = []
        for item in sources:
            if isinstance(item, str) and item.strip():
                cleaned_sources.append(item.strip())
            elif isinstance(item, dict):
                display = item.get("display") or item.get("title") or ""
                if isinstance(display, str) and display.strip():
                    cleaned_sources.append(display.strip())
        return {"answer": answer.strip(), "sources": cleaned_sources[: settings.retriever_fact_cap], "raw": response}


gateway = GeminiGateway()

