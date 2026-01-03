"""Ping-pong drafting loop with GPT + Gemini CLI."""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional, Tuple

from .gemini_cli import GeminiCLIError, is_quota_near_limit, run_gemini
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
        "table_callouts": {t.get("title"): t.get("callouts") for t in (bundle.get("docx_tables") or [])},
        "figure_callouts": {c.get("title"): c.get("callouts") for c in (bundle.get("docx_charts") or [])},
        "constraints": bundle.get("constraints"),
    }


def run_gpt_compute(bundle: Dict[str, Any], editor_notes: Optional[str] = None) -> Dict[str, Any]:
    schema = {
        "analysis_notes": ["bullet insights with numbers"],
        "table_caption": "string",
        "chart_captions": ["strings"],
        "validation_flags": {"sector_delta_inconsistent": False, "missing_fields": []},
        "narrative_table": "markdown",
        "table_callouts": {"Table Name": ["callouts"]},
        "figure_callouts": {"Figure Name": ["callouts"]},
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
        "Explicitly reference the figure and table callouts (e.g., 'The chart above shows...'). "
        "Include at least 6 numeric references across the narrative. "
        "Schema:\n"
        + json.dumps(schema)
        + "\nAnalysis notes:\n"
        + json.dumps(compute.get("analysis_notes", []))
        + "\nTable callouts:\n"
        + json.dumps(compute.get("table_callouts", {}))
        + "\nFigure callouts:\n"
        + json.dumps(compute.get("figure_callouts", {}))
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
        "Explicitly reference the figure and table callouts (e.g., 'The chart above shows...'). "
        "Include at least 6 numeric references across the narrative. "
        "Schema:\n"
        + json.dumps(schema)
        + "\nAnalysis notes:\n"
        + json.dumps(compute.get("analysis_notes", []))
        + "\nTable callouts:\n"
        + json.dumps(compute.get("table_callouts", {}))
        + "\nFigure callouts:\n"
        + json.dumps(compute.get("figure_callouts", {}))
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


def _sanitize_text(text: str) -> str:
    banned = [
        "needs review",
        "automated checks",
        "flagged",
        "compliance",
        "coverage gaps",
        "json_decode_failed",
    ]
    value = text or ""
    for phrase in banned:
        value = value.replace(phrase, "")
        value = value.replace(phrase.title(), "")
    return " ".join(value.split()).strip()


def _remove_external_claims(text: str) -> str:
    triggers = ["industry reports", "academic research", "studies", "external research"]
    sentences = [s.strip() for s in text.split(".") if s.strip()]
    kept = []
    for sentence in sentences:
        lower = sentence.lower()
        if any(t in lower for t in triggers):
            continue
        kept.append(sentence)
    return ". ".join(kept) + ("." if kept else "")


def _sanitize_writer(writer: Dict[str, Any]) -> Dict[str, Any]:
    writer["headline"] = _sanitize_text(writer.get("headline") or "")
    paragraphs = [_sanitize_text(str(p)) for p in (writer.get("paragraphs") or []) if p]
    paragraphs = [_remove_external_claims(p) for p in paragraphs if p]
    writer["paragraphs"] = _inject_definitions(paragraphs)
    return writer


def _inject_definitions(paragraphs: list[str]) -> list[str]:
    if not paragraphs:
        return paragraphs
    import re

    joined = "\n".join(paragraphs)
    if "AI Governance & Ethics Score (AIGES)" not in joined and re.search(r"\bAIGES\b", joined):
        paragraphs = [re.sub(r"\bAIGES\b", "AI Governance & Ethics Score (AIGES)", p, count=1) for p in paragraphs]
    if "the Core (the 25 non-zero-weight constituents)" not in joined and re.search(r"\bcore\b", joined, re.IGNORECASE):
        paragraphs = [
            re.sub(
                r"\bcore\b",
                "the Core (the 25 non-zero-weight constituents)",
                p,
                count=1,
                flags=re.IGNORECASE,
            )
            for p in paragraphs
        ]
    if (
        "the Coverage set (the full 100-name monitoring universe, including zero-weight names)" not in joined
        and re.search(r"\bcoverage\b", joined, re.IGNORECASE)
    ):
        paragraphs = [
            re.sub(
                r"\bcoverage\b",
                "the Coverage set (the full 100-name monitoring universe, including zero-weight names)",
                p,
                count=1,
                flags=re.IGNORECASE,
            )
            for p in paragraphs
        ]
    elif "the Coverage set (the full 100-name monitoring universe, including zero-weight names)" not in joined:
        paragraphs.append(
            "The Coverage set (the full 100-name monitoring universe, including zero-weight names) provides the broader benchmark."
        )
    if "the quarterly rebalance (composition/weights refresh)" not in joined and re.search(r"\brebalance\b", joined, re.IGNORECASE):
        paragraphs = [
            re.sub(
                r"\brebalance\b",
                "the quarterly rebalance (composition/weights refresh)",
                p,
                count=1,
                flags=re.IGNORECASE,
            )
            for p in paragraphs
        ]
    return paragraphs


def run_publisher(bundle: Dict[str, Any], compute: Dict[str, Any], draft: Dict[str, Any]) -> Dict[str, Any]:
    schema = {
        "headline": "8-14 words",
        "standfirst": "one sentence",
        "key_takeaways": ["3 bullets with numbers"],
        "paragraphs": ["2-4 paragraphs"],
        "table_caption": "string",
        "chart_caption": "string",
    }
    prompt = (
        "You are the SENIOR PUBLISHER. Output JSON only. "
        "Remove internal phrases like 'needs review' or 'automated checks'. "
        "Generate three headline candidates internally and output only the best one. "
        "Define terms inline at first mention only. "
        "When AIGES appears first time, write 'AI Governance & Ethics Score (AIGES)' and continue. "
        "When Core appears first time, write 'the Core (the 25 non-zero-weight constituents)'. "
        "When Coverage appears first time, write 'the Coverage set (the full 100-name monitoring universe, including zero-weight names)'. "
        "When rebalance appears first time, write 'the quarterly rebalance (composition/weights refresh)'. "
        "Use this structure: headline, standfirst, key takeaways (3 bullets), paragraph 1 (what changed), "
        "paragraph 2 (sector shifts referencing Figure 1), paragraph 3 (distribution/dispersion referencing Figure 2), "
        "paragraph 4 (movers referencing the mover tables), closing method note (1-2 sentences). "
        "Include at least 2 figure references and 2 table references and at least 8 numeric values. "
        "Add a short 'why it matters' line tied to governance/regulatory readiness, using only computed metrics. "
        "Explicitly mention Coverage at least once. "
        "No investment advice, no stock prices, no external claims. "
        "Schema:\n"
        + json.dumps(schema)
        + "\nAnalysis notes:\n"
        + json.dumps(compute.get("analysis_notes", []))
        + "\nTable callouts:\n"
        + json.dumps(compute.get("table_callouts", {}))
        + "\nFigure callouts:\n"
        + json.dumps(compute.get("figure_callouts", {}))
        + "\nDraft narrative:\n"
        + json.dumps(draft)
    )
    messages = [{"role": "user", "content": prompt}]
    return run_gpt_json(messages, timeout=70.0)


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
            "table_callouts": {t.get("title"): t.get("callouts") for t in (bundle.get("docx_tables") or [])},
            "figure_callouts": {c.get("title"): c.get("callouts") for c in (bundle.get("docx_charts") or [])},
        }
        issues.append(str(exc))
    compute.setdefault("table_callouts", {t.get("title"): t.get("callouts") for t in (bundle.get("docx_tables") or [])})
    compute.setdefault("figure_callouts", {c.get("title"): c.get("callouts") for c in (bundle.get("docx_charts") or [])})
    if "validation_flags" not in compute:
        compute["validation_flags"] = {}
    if "sector_delta_inconsistent" not in compute["validation_flags"]:
        flags = (bundle.get("metrics") or {}).get("sector_exposure", {}).get("core_count_delta_flags") or []
        compute["validation_flags"]["sector_delta_inconsistent"] = bool(flags)

    retries = 0
    last_writer: Optional[Dict[str, Any]] = None
    while retries <= 2:
        try:
            if is_quota_near_limit():
                raise GeminiCLIError("gemini_quota_near_limit")
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
                return _sanitize_writer(writer), [], compute
            return _sanitize_writer(writer), issues + [str(exc)], compute

        if critic.get("pass") and ok and local_ok:
            try:
                published = run_publisher(bundle, compute, writer)
                paragraphs = [published.get("standfirst")]
                paragraphs += (published.get("key_takeaways") or [])
                paragraphs += (published.get("paragraphs") or [])
                final_writer = {
                    "headline": published.get("headline") or writer.get("headline"),
                    "paragraphs": [p for p in paragraphs if p],
                    "table_caption": published.get("table_caption") or writer.get("table_caption"),
                    "chart_caption": published.get("chart_caption") or writer.get("chart_caption"),
                    "compliance_checklist": writer.get("compliance_checklist") or {},
                }
                return _sanitize_writer(final_writer), [], compute
            except GPTClientError as exc:
                append_note(
                    failure_type="gpt_publisher_failed",
                    fix_hint=str(exc)[:120],
                    report_type=bundle.get("report_type", ""),
                )
                return _sanitize_writer(writer), [], compute

        retries += 1
        if retries > 2:
            append_note(
                failure_type="quality_gate_failed",
                fix_hint=";".join(critic.get("issues", []))[:120],
                report_type=bundle.get("report_type", ""),
            )
            return _sanitize_writer(writer), critic.get("issues", []), compute

        suggested = critic.get("suggested_fixes") or []
        if suggested:
            compute["analysis_notes"] = compute.get("analysis_notes", []) + [
                f"Critic note: {item}" for item in suggested
            ]

    return _sanitize_writer(last_writer or {}), issues, compute
