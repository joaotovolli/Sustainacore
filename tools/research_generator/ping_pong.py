"""Ping-pong drafting loop with GPT + Gemini CLI."""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional, Tuple

from .gemini_cli import GeminiCLIError, run_gemini
from .gpt_client import GPTClientError, run_gpt_json
from .learned_notes import append_note
from .validators import validate_quality_gate, validate_writer_output

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


def _bundle_context(bundle: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "report_type": bundle.get("report_type"),
        "window": bundle.get("window"),
        "metrics": bundle.get("metrics"),
        "csv_extracts": bundle.get("csv_extracts"),
        "constraints": bundle.get("constraints"),
    }


def run_gpt_compute(bundle: Dict[str, Any], editor_notes: Optional[str] = None) -> Dict[str, Any]:
    schema = {
        "analysis_notes": ["bullet insights with numbers"],
        "table_caption": "string",
        "chart_captions": ["strings"],
        "validation_flags": {"sector_delta_inconsistent": False, "missing_fields": []},
        "narrative_table": "markdown",
    }
    notes = editor_notes.strip() if editor_notes else ""
    prompt = (
        "You are the COMPUTE step. Use only the bundle data. Output JSON only. "
        "Generate analysis_notes with concrete stats (IQR, HHI, turnover, breadth). "
        "Flag sector delta inconsistencies if present. "
        "Schema:\n"
        + json.dumps(schema)
        + "\nBundle:\n"
        + json.dumps(_bundle_context(bundle))
        + ("\nEditor instructions:\n" + notes if notes else "")
    )
    messages = [{"role": "user", "content": prompt}]
    return run_gpt_json(messages, timeout=70.0)


def run_gemini_writer(bundle: Dict[str, Any], compute: Dict[str, Any]) -> Dict[str, Any]:
    schema = {
        "headline": "8-14 words",
        "paragraphs": ["2-4 short paragraphs"],
        "table_caption": "string",
        "chart_caption": "string",
        "tags": ["ai-governance", "ethics"],
        "compliance_checklist": {"no_prices": True, "no_advice": True, "tone_ok": True},
    }
    prompt = (
        "You are the WRITER. Output JSON only, no markdown. "
        "Use neutral research tone focused on AI governance & ethics. "
        "Do not mention prices or investment advice. "
        "Narrative must be based on computed metrics only. "
        "Use association-not-causation language when referencing performance. "
        "Use only 1-2 short support lines from public source summaries. "
        "Schema:\n"
        + json.dumps(schema)
        + "\nAnalysis notes:\n"
        + json.dumps(compute.get("analysis_notes", []))
        + "\nCaptions:\n"
        + json.dumps(
            {
                "table": compute.get("table_caption"),
                "charts": compute.get("chart_captions", []),
            }
        )
        + "\nConstraints:\n"
        + json.dumps(bundle.get("constraints"))
    )
    response = run_gemini(prompt, timeout=60.0)
    return _parse_json_block(response)


def run_gpt_writer(bundle: Dict[str, Any], compute: Dict[str, Any]) -> Dict[str, Any]:
    schema = {
        "headline": "8-14 words",
        "paragraphs": ["2-4 short paragraphs"],
        "table_caption": "string",
        "chart_caption": "string",
        "tags": ["ai-governance", "ethics"],
        "compliance_checklist": {"no_prices": True, "no_advice": True, "tone_ok": True},
    }
    prompt = (
        "You are the WRITER. Output JSON only. Follow all constraints. "
        "Use the analysis_notes as the factual base. "
        "Use association-not-causation language when referencing performance. "
        "Use only 1-2 short support lines from public source summaries. "
        "Schema:\n"
        + json.dumps(schema)
        + "\nAnalysis notes:\n"
        + json.dumps(compute.get("analysis_notes", []))
        + "\nCaptions:\n"
        + json.dumps(
            {
                "table": compute.get("table_caption"),
                "charts": compute.get("chart_captions", []),
            }
        )
        + "\nConstraints:\n"
        + json.dumps(bundle.get("constraints"))
    )
    messages = [{"role": "user", "content": prompt}]
    return run_gpt_json(messages, timeout=70.0)


def run_gpt_critic(bundle: Dict[str, Any], compute: Dict[str, Any], writer: Dict[str, Any], issues: list[str]) -> Dict[str, Any]:
    schema = {
        "pass": True,
        "issues": ["list"],
        "needs_retry": False,
        "suggested_fixes": ["list"],
    }
    prompt = (
        "You are the CRITIC. Output JSON only. "
        "Check compliance, presence of core vs coverage, and at least 3 non-trivial stats "
        "(IQR, HHI, turnover, breadth) in the narrative. "
        "If sector delta inconsistent, ensure it is flagged. "
        "Schema:\n"
        + json.dumps(schema)
        + "\nBundle context:\n"
        + json.dumps(_bundle_context(bundle))
        + "\nAnalysis notes:\n"
        + json.dumps(compute.get("analysis_notes", []))
        + "\nWriter output:\n"
        + json.dumps(writer)
        + "\nValidation issues:\n"
        + json.dumps(issues)
    )
    messages = [{"role": "user", "content": prompt}]
    return run_gpt_json(messages, timeout=60.0)


def _append_needs_review(writer: Dict[str, Any]) -> Dict[str, Any]:
    paragraphs = list(writer.get("paragraphs") or [])
    paragraphs.append("Needs review: automated checks flagged compliance or coverage gaps.")
    writer["paragraphs"] = paragraphs
    return writer


def draft_with_ping_pong(
    bundle: Dict[str, Any],
    *,
    editor_notes: Optional[str] = None,
) -> Tuple[Optional[Dict[str, Any]], list[str], Optional[Dict[str, Any]]]:
    issues: list[str] = []
    compute: Dict[str, Any] = {}

    try:
        compute = run_gpt_compute(bundle, editor_notes=editor_notes)
    except GPTClientError as exc:
        append_note(failure_type="gpt_compute_failed", fix_hint=str(exc)[:120], report_type=bundle.get("report_type", ""))
        compute = {
            "analysis_notes": ["Compute step unavailable; using bundle metrics directly."],
            "table_caption": bundle.get("table_caption_draft"),
            "chart_captions": [bundle.get("chart_caption_draft")],
            "validation_flags": {"sector_delta_inconsistent": False, "missing_fields": []},
            "narrative_table": "",
        }
        issues.append(str(exc))

    retries = 0
    last_writer: Optional[Dict[str, Any]] = None
    while retries <= 2:
        try:
            writer = run_gemini_writer(bundle, compute)
        except GeminiCLIError as exc:
            append_note(
                failure_type="gemini_writer_failed",
                fix_hint=str(exc)[:120],
                report_type=bundle.get("report_type", ""),
            )
            try:
                writer = run_gpt_writer(bundle, compute)
            except GPTClientError as gpt_exc:
                append_note(
                    failure_type="gpt_writer_failed",
                    fix_hint=str(gpt_exc)[:120],
                    report_type=bundle.get("report_type", ""),
                )
                return None, issues + [str(exc), str(gpt_exc)], compute

        last_writer = writer
        ok, writer_issues = validate_writer_output(bundle, writer)
        local_ok, local_issues = validate_quality_gate(bundle, writer, compute)
        issues = writer_issues + local_issues
        if not ok or not local_ok:
            LOGGER.warning("writer_output_invalid issues=%s output=%s", issues, writer)

        try:
            critic = run_gpt_critic(bundle, compute, writer, issues)
        except GPTClientError as exc:
            append_note(
                failure_type="gpt_critic_failed",
                fix_hint=str(exc)[:120],
                report_type=bundle.get("report_type", ""),
            )
            if not issues:
                return writer, [], compute
            return _append_needs_review(writer), issues + [str(exc)], compute

        if critic.get("pass") and ok and local_ok:
            return writer, [], compute

        retries += 1
        if retries > 2:
            append_note(
                failure_type="quality_gate_failed",
                fix_hint=";".join(critic.get("issues", []))[:120],
                report_type=bundle.get("report_type", ""),
            )
            return _append_needs_review(writer), critic.get("issues", []), compute

        suggested = critic.get("suggested_fixes") or []
        if suggested:
            compute["analysis_notes"] = compute.get("analysis_notes", []) + [
                f"Critic note: {item}" for item in suggested
            ]

    return last_writer, issues, compute
