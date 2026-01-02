"""Ping-pong drafting loop with Gemini CLI."""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional, Tuple

from .gemini_cli import GeminiCLIError, run_gemini
from .validators import validate_writer_output

LOGGER = logging.getLogger("research_generator.ping_pong")


class DraftingError(RuntimeError):
    pass


def _parse_json_block(text: str) -> Dict[str, Any]:
    if not text:
        raise DraftingError("empty_response")
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise DraftingError("no_json_block")
    payload = text[start : end + 1]
    try:
        return json.loads(payload)
    except json.JSONDecodeError as exc:
        raise DraftingError("json_decode_failed") from exc


def run_writer(bundle: Dict[str, Any], editor_notes: Optional[str] = None) -> Dict[str, Any]:
    schema = {
        "headline": "8-14 words",
        "paragraphs": ["2-4 short paragraphs"],
        "table_caption": "string",
        "chart_caption": "string",
        "tags": ["ai-governance", "ethics"],
        "compliance_checklist": {"no_prices": True, "no_advice": True, "tone_ok": True},
    }
    notes = editor_notes.strip() if editor_notes else ""
    prompt = (
        "You are the WRITER. Output JSON only, no markdown. "
        "Use neutral research tone focused on AI governance & ethics. "
        "Do not mention prices or investment advice. "
        "Schema:\n"
        + json.dumps(schema)
        + "\nBundle:\n"
        + json.dumps(bundle)
        + ("\nEditor instructions:\n" + notes if notes else "")
    )
    response = run_gemini(prompt, timeout=60.0)
    return _parse_json_block(response)


def run_critic(
    bundle: Dict[str, Any],
    writer: Dict[str, Any],
    issues: list[str],
    editor_notes: Optional[str] = None,
) -> Dict[str, Any]:
    payload = {
        "bundle": bundle,
        "writer": writer,
        "issues": issues,
    }
    notes = editor_notes.strip() if editor_notes else ""
    prompt = (
        "You are the CRITIC. Output JSON only, no markdown. "
        "Focus on compliance, clarity, AI governance framing. "
        "Schema:\n"
        + json.dumps(
            {
                "issues": ["list"],
                "improved_headline": "string",
                "improved_paragraphs": ["list"],
                "improved_captions": {"table": "string", "chart": "string"},
                "suggestions_for_table": "string",
                "suggestions_for_chart": "string",
            }
        )
        + "\nPayload:\n"
        + json.dumps(payload)
        + ("\nEditor instructions:\n" + notes if notes else "")
    )
    response = run_gemini(prompt, timeout=60.0)
    return _parse_json_block(response)


def run_revise(
    bundle: Dict[str, Any],
    critic: Dict[str, Any],
    editor_notes: Optional[str] = None,
) -> Dict[str, Any]:
    payload = {"bundle": bundle, "critic": critic}
    notes = editor_notes.strip() if editor_notes else ""
    prompt = (
        "You are the REVISE step. Output JSON only, no markdown. "
        "Apply critic improvements, keep compliance. "
        "Schema:\n"
        + json.dumps(
            {
                "headline": "8-14 words",
                "paragraphs": ["2-4 short paragraphs"],
                "table_caption": "string",
                "chart_caption": "string",
                "tags": ["ai-governance", "ethics"],
                "compliance_checklist": {"no_prices": True, "no_advice": True, "tone_ok": True},
            }
        )
        + "\nPayload:\n"
        + json.dumps(payload)
        + ("\nEditor instructions:\n" + notes if notes else "")
    )
    response = run_gemini(prompt, timeout=60.0)
    return _parse_json_block(response)


def draft_with_ping_pong(
    bundle: Dict[str, Any],
    *,
    editor_notes: Optional[str] = None,
) -> Tuple[Optional[Dict[str, Any]], list[str]]:
    try:
        writer = run_writer(bundle, editor_notes=editor_notes)
    except (GeminiCLIError, DraftingError) as exc:
        return None, [str(exc)]

    ok, issues = validate_writer_output(bundle, writer)
    if ok:
        return writer, []
    LOGGER.warning("writer_output_invalid issues=%s output=%s", issues, writer)

    try:
        critic = run_critic(bundle, writer, issues, editor_notes=editor_notes)
    except (GeminiCLIError, DraftingError) as exc:
        return None, issues + [str(exc)]

    improved = writer.copy()
    if critic.get("improved_headline"):
        improved["headline"] = critic["improved_headline"]
    if critic.get("improved_paragraphs"):
        improved["paragraphs"] = critic["improved_paragraphs"]
    captions = critic.get("improved_captions") or {}
    if captions.get("table"):
        improved["table_caption"] = captions["table"]
    if captions.get("chart"):
        improved["chart_caption"] = captions["chart"]

    ok, issues = validate_writer_output(bundle, improved)
    if ok:
        return improved, []

    try:
        revised = run_revise(bundle, critic, editor_notes=editor_notes)
    except (GeminiCLIError, DraftingError) as exc:
        return None, issues + [str(exc)]

    ok, issues = validate_writer_output(bundle, revised)
    if ok:
        return revised, []
    return None, issues
