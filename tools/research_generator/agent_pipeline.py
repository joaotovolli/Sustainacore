"""Agent pipeline for research generator drafting."""
from __future__ import annotations

import datetime as dt
import json
import logging
import os
from typing import Any, Dict, List, Optional, Tuple

from . import config
from .gemini_cli import GeminiCLIError, is_quota_near_limit, run_gemini
from .gpt_client import GPTClientError, run_gpt_json
from .learned_notes import append_note
from .publish_pass import linearize_draft, publisher_in_chief_rewrite
from .sanitize import sanitize_text_blocks

LOGGER = logging.getLogger("research_generator.agent_pipeline")


def _bundle_context(bundle: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "report_type": bundle.get("report_type"),
        "window": bundle.get("window"),
        "metrics": bundle.get("metrics"),
        "tables": [{"title": t.get("title"), "rows": t.get("rows")} for t in (bundle.get("docx_tables") or [])],
        "figures": [{"title": f.get("title"), "caption": f.get("caption")} for f in (bundle.get("docx_charts") or [])],
        "constraints": bundle.get("constraints"),
    }


def _debug_path() -> str:
    return os.path.join(config.DEFAULT_OUTPUT_DIR, "debug")


def _write_debug(stage: str, state: Dict[str, Any]) -> None:
    try:
        os.makedirs(_debug_path(), exist_ok=True)
        stamp = dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        path = os.path.join(_debug_path(), f"{stamp}_{stage}.json")
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(state, handle, indent=2, ensure_ascii=True)
    except Exception:
        return


def _has_blocker(issues: List[Dict[str, Any]]) -> bool:
    return any(issue.get("severity") == "BLOCKER" for issue in issues)


def run_analyst(bundle: Dict[str, Any]) -> Dict[str, Any]:
    schema = {
        "insight_candidates": [
            {
                "title": "string",
                "evidence": ["bullet with numbers"],
                "why_it_matters": "string",
                "artifact_suggestions": ["Figure 1", "Table 2"],
                "novelty": "obvious|non_obvious",
            }
        ]
    }
    prompt = (
        "You are the ANALYST. Output JSON only. "
        "Use only the data bundle. Produce at least 8 insight candidates, "
        "including 3 non-obvious insights beyond sector delta and top movers. "
        "Schema:\n"
        + json.dumps(schema)
        + "\nBundle:\n"
        + json.dumps(_bundle_context(bundle))
        + "\nExisting insight candidates:\n"
        + json.dumps(bundle.get("insight_candidates") or [])
    )
    messages = [{"role": "user", "content": prompt}]
    return run_gpt_json(messages, timeout=70.0)


def run_editor(bundle: Dict[str, Any], analyst: Dict[str, Any]) -> Dict[str, Any]:
    schema = {
        "throughline": "1-2 sentences",
        "outline": [
            {"type": "paragraph", "intent": "string"},
            {"type": "figure", "id": 1},
            {"type": "table", "id": 1},
        ],
        "definitions": {
            "aiges": "first_mention",
            "core": "first_mention",
            "coverage": "first_mention",
            "rebalance": "first_mention",
        },
    }
    prompt = (
        "You are the STORY ARCHITECT. Output JSON only. "
        "Select a narrative arc and place figure/table insertions in the outline. "
        "Ensure Core/Coverage/AIGES definitions appear only at first mention. "
        "Schema:\n"
        + json.dumps(schema)
        + "\nInsights:\n"
        + json.dumps(analyst.get("insight_candidates") or [])
        + "\nAvailable figures/tables:\n"
        + json.dumps({
            "figures": bundle.get("docx_charts") or [],
            "tables": bundle.get("docx_tables") or [],
        })
    )
    messages = [{"role": "user", "content": prompt}]
    return run_gpt_json(messages, timeout=70.0)


def run_writer(bundle: Dict[str, Any], editor: Dict[str, Any], analyst: Dict[str, Any]) -> Dict[str, Any]:
    schema = {
        "headline": "string",
        "paragraphs": ["string"],
        "outline": [{"type": "paragraph|figure|table", "id": 1, "text": "string"}],
        "compliance_checklist": {"no_prices": True, "no_advice": True, "tone_ok": True},
    }
    prompt = (
        "You are the WRITER. Output JSON only. "
        "Write a cohesive narrative following the outline intents. "
        "Reference artifacts inline as 'Figure 1' or 'Table 2'. "
        "Do not use phrases like 'anchors the evidence' or 'what it shows'. "
        "Entrants/exits are membership changes only; incumbents get deltas. "
        "Schema:\n"
        + json.dumps(schema)
        + "\nThroughline:\n"
        + json.dumps(editor.get("throughline"))
        + "\nOutline intents:\n"
        + json.dumps(editor.get("outline") or [])
        + "\nInsights:\n"
        + json.dumps(analyst.get("insight_candidates") or [])
    )
    if not is_quota_near_limit():
        try:
            response = run_gemini(prompt, timeout=60.0)
            payload = response.strip()
            return json.loads(payload[payload.find("{") : payload.rfind("}") + 1])
        except (GeminiCLIError, json.JSONDecodeError, ValueError) as exc:
            append_note(failure_type="gemini_writer_failed", fix_hint=str(exc)[:120], report_type=bundle.get("report_type", ""))
    messages = [{"role": "user", "content": prompt}]
    return run_gpt_json(messages, timeout=70.0)


def run_copyeditor(draft: Dict[str, Any]) -> Dict[str, Any]:
    schema = {"paragraphs": ["string"]}
    prompt = (
        "You are the COPYEDITOR. Output JSON only. "
        "Fix repetition, tighten language, remove scaffolding phrases. "
        "Do not alter numeric values. "
        "Schema:\n"
        + json.dumps(schema)
        + "\nDraft paragraphs:\n"
        + json.dumps(draft.get("paragraphs") or [])
    )
    messages = [{"role": "user", "content": prompt}]
    return run_gpt_json(messages, timeout=60.0)


def run_fact_checker(bundle: Dict[str, Any], draft: Dict[str, Any]) -> Dict[str, Any]:
    schema = {
        "issues": [
            {"severity": "BLOCKER|WARN", "issue": "string", "suggested_fix": "string"}
        ]
    }
    prompt = (
        "You are the FACT-CHECKER. Output JSON only. "
        "Check equal-weight logic, numeric consistency, forbidden advice, and entrants/exits handling. "
        "Do not invent numbers. "
        "Schema:\n"
        + json.dumps(schema)
        + "\nBundle metrics:\n"
        + json.dumps(_bundle_context(bundle))
        + "\nDraft:\n"
        + json.dumps(draft.get("paragraphs") or [])
    )
    messages = [{"role": "user", "content": prompt}]
    return run_gpt_json(messages, timeout=60.0)


def run_pipeline(bundle: Dict[str, Any], *, editor_notes: Optional[str] = None) -> Tuple[Optional[Dict[str, Any]], List[str], Dict[str, Any]]:
    issues: List[str] = []
    state: Dict[str, Any] = {"report_type": bundle.get("report_type"), "warnings": []}

    try:
        analyst = run_analyst(bundle)
        state["insight_candidates"] = analyst.get("insight_candidates")
        _write_debug("analyst", state)
    except GPTClientError as exc:
        append_note(failure_type="analyst_failed", fix_hint=str(exc)[:120], report_type=bundle.get("report_type", ""))
        analyst = {"insight_candidates": bundle.get("insight_candidates") or []}

    try:
        editor = run_editor(bundle, analyst)
        state["outline"] = editor.get("outline")
        state["throughline"] = editor.get("throughline")
        _write_debug("editor", state)
    except GPTClientError as exc:
        append_note(failure_type="editor_failed", fix_hint=str(exc)[:120], report_type=bundle.get("report_type", ""))
        editor = {"outline": []}

    try:
        writer = run_writer(bundle, editor, analyst)
        _write_debug("writer", {**state, "draft": writer})
    except GPTClientError as exc:
        append_note(failure_type="writer_failed", fix_hint=str(exc)[:120], report_type=bundle.get("report_type", ""))
        return None, [str(exc)], analyst

    try:
        copyedited = run_copyeditor(writer)
        writer["paragraphs"] = copyedited.get("paragraphs") or writer.get("paragraphs")
        _write_debug("copyeditor", {**state, "draft": writer})
    except GPTClientError as exc:
        append_note(failure_type="copyeditor_failed", fix_hint=str(exc)[:120], report_type=bundle.get("report_type", ""))

    try:
        check = run_fact_checker(bundle, writer)
        issues = [f"{item.get('severity')}: {item.get('issue')}" for item in (check.get("issues") or [])]
        _write_debug("factcheck", {**state, "issues": issues})
        if _has_blocker(check.get("issues") or []):
            retry_note = " ".join(item.get("suggested_fix", "") for item in check.get("issues") or [])
            writer["paragraphs"] = (writer.get("paragraphs") or []) + [retry_note]
            copyedited = run_copyeditor(writer)
            writer["paragraphs"] = copyedited.get("paragraphs") or writer.get("paragraphs")
    except GPTClientError as exc:
        append_note(failure_type="factcheck_failed", fix_hint=str(exc)[:120], report_type=bundle.get("report_type", ""))

    linear = linearize_draft(
        headline=writer.get("headline") or "",
        paragraphs=writer.get("paragraphs") or [],
        outline=writer.get("outline") or editor.get("outline") or [],
        charts=bundle.get("docx_charts") or [],
        tables=bundle.get("docx_tables") or [],
    )

    constraints = {
        "no_prices": True,
        "no_advice": True,
        "tone": "research/education",
        "editor_notes": editor_notes or "",
    }
    final = {}
    try:
        final = publisher_in_chief_rewrite(linear, constraints=constraints)
    except GPTClientError as exc:
        append_note(failure_type="publisher_failed", fix_hint=str(exc)[:120], report_type=bundle.get("report_type", ""))
        final = {
            "headline": writer.get("headline"),
            "dek": None,
            "body": writer.get("paragraphs") or [],
        }

    body = final.get("body") or writer.get("paragraphs") or []
    body = sanitize_text_blocks(body)
    draft = {
        "headline": final.get("headline") or writer.get("headline"),
        "dek": final.get("dek"),
        "paragraphs": body,
        "outline": writer.get("outline") or editor.get("outline") or [],
        "compliance_checklist": writer.get("compliance_checklist") or {},
    }
    _write_debug("final", {**state, "final": draft})
    return draft, issues, analyst
