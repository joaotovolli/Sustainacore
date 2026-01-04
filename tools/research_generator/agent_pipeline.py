"""Agent pipeline for research generator (OpenAI-only)."""
from __future__ import annotations

import datetime as dt
import json
import logging
import os
from typing import Any, Dict, List, Optional, Tuple

from .gpt_client import GPTClientError, run_gpt_json
from .idea_engine import build_chart_bank, build_metric_pool, ensure_angle_count, generate_angles, rank_angles
from .oracle import get_connection
from .sanitize import sanitize_text_blocks

LOGGER = logging.getLogger("research_generator.agent_pipeline")


def _debug_dir(output_dir: str) -> str:
    return os.path.join(output_dir, "debug")


def _write_debug(output_dir: str, stage: str, payload: Dict[str, Any]) -> None:
    try:
        os.makedirs(_debug_dir(output_dir), exist_ok=True)
        stamp = dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        path = os.path.join(_debug_dir(output_dir), f"{stamp}_{stage}.json")
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, ensure_ascii=True)
    except Exception:
        return


def _core_vs_rest_table(bundle: Dict[str, Any], metric_pool: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def _metric(name: str) -> Optional[float]:
        for item in metric_pool:
            if item.get("name") == name:
                return item.get("value")
        return None

    rows = [
        {
            "Metric": "AIGES mean",
            "Core": _metric("core.aiges.mean"),
            "Rest": _metric("rest.aiges.mean"),
        },
        {
            "Metric": "AIGES median",
            "Core": _metric("core.aiges.p50"),
            "Rest": _metric("rest.aiges.p50"),
        },
        {
            "Metric": "AIGES IQR",
            "Core": (_metric("core.aiges.p75") or 0) - (_metric("core.aiges.p25") or 0),
            "Rest": (_metric("rest.aiges.p75") or 0) - (_metric("rest.aiges.p25") or 0),
        },
        {
            "Metric": "AIGES std",
            "Core": _metric("core.aiges.std"),
            "Rest": _metric("rest.aiges.std"),
        },
    ]
    return rows


def _ensure_core_rest_mentions(paragraphs: List[str], metric_pool: List[Dict[str, Any]]) -> List[str]:
    joined = " ".join(paragraphs).lower()
    if "core" in joined and "rest" in joined:
        return paragraphs
    core_mean = next((m.get("value") for m in metric_pool if m.get("name") == "core.aiges.mean"), None)
    rest_mean = next((m.get("value") for m in metric_pool if m.get("name") == "rest.aiges.mean"), None)
    sentence = f"The Core vs Rest split shows mean AIGES at {core_mean:.2f} vs {rest_mean:.2f}." if core_mean and rest_mean else "The Core vs Rest split is a primary lens in this report."
    if paragraphs:
        paragraphs[0] = paragraphs[0].rstrip() + " " + sentence
    else:
        paragraphs = [sentence]
    return paragraphs


def _sector_table(bundle: Dict[str, Any]) -> List[Dict[str, Any]]:
    core = bundle.get("core_latest_rows") or []
    rest = bundle.get("rest_latest_rows") or []
    sector_counts: Dict[str, Dict[str, int]] = {}
    for row in core:
        sector = row.get("sector") or "Unknown"
        sector_counts.setdefault(sector, {"Core": 0, "Rest": 0})
        sector_counts[sector]["Core"] += 1
    for row in rest:
        sector = row.get("sector") or "Unknown"
        sector_counts.setdefault(sector, {"Core": 0, "Rest": 0})
        sector_counts[sector]["Rest"] += 1
    rows = []
    for sector, counts in sorted(sector_counts.items()):
        rows.append({"Sector": sector, "Core": counts["Core"], "Rest": counts["Rest"]})
    return rows


def run_data_investigator(bundle: Dict[str, Any], metric_pool: List[Dict[str, Any]]) -> Dict[str, Any]:
    schema = {
        "insight_candidates": [
            {"title": "string", "evidence": ["numbered bullets"], "why_it_matters": "string"}
        ]
    }
    prompt = (
        "You are the Data Investigator. Output JSON only. "
        "Use only metrics provided. Generate at least 8 candidates including non-obvious insights. "
        "Schema:\n"
        + json.dumps(schema)
        + "\nMetric pool:\n"
        + json.dumps(metric_pool[:200])
    )
    messages = [{"role": "user", "content": prompt}]
    return run_gpt_json(messages, timeout=70.0)


def run_editor(bundle: Dict[str, Any], angles: List[Dict[str, Any]]) -> Dict[str, Any]:
    schema = {
        "outline": [
            {"type": "paragraph", "intent": "string"},
            {"type": "figure", "id": 1},
            {"type": "table", "id": 1},
        ],
        "throughline": "string",
    }
    prompt = (
        "You are the Story Architect. Output JSON only. "
        "Pick the top angle and produce a concise outline with insertion points. "
        "Schema:\n"
        + json.dumps(schema)
        + "\nAngles:\n"
        + json.dumps(angles)
    )
    messages = [{"role": "user", "content": prompt}]
    return run_gpt_json(messages, timeout=70.0)


def run_writer(angle: Dict[str, Any], outline: List[Dict[str, Any]]) -> Dict[str, Any]:
    schema = {
        "headline": "8-14 words",
        "paragraphs": ["string paragraphs"],
        "outline": [{"type": "paragraph|figure|table", "id": 1, "text": "string"}],
    }
    prompt = (
        "You are the Writer. Output JSON only. "
        "Write a cohesive narrative using the outline. "
        "Reference artifacts inline as 'Figure 1' or 'Table 1'. "
        "No scaffolding phrases like 'what it shows'. "
        "Schema:\n"
        + json.dumps(schema)
        + "\nAngle:\n"
        + json.dumps(angle)
        + "\nOutline:\n"
        + json.dumps(outline)
    )
    messages = [{"role": "user", "content": prompt}]
    return run_gpt_json(messages, timeout=70.0)


def run_copyeditor(paragraphs: List[str]) -> Dict[str, Any]:
    schema = {"paragraphs": ["string"]}
    prompt = (
        "You are the Copyeditor. Output JSON only. "
        "Improve flow, remove repetition, keep numbers unchanged. "
        "Schema:\n"
        + json.dumps(schema)
        + "\nParagraphs:\n"
        + json.dumps(paragraphs)
    )
    messages = [{"role": "user", "content": prompt}]
    return run_gpt_json(messages, timeout=60.0)


def run_fact_checker(bundle: Dict[str, Any], paragraphs: List[str]) -> Dict[str, Any]:
    schema = {"status": "PASS|FAIL", "issues": ["string"]}
    prompt = (
        "You are the Fact Checker. Output JSON only. "
        "Check numeric sanity, equal-weight invariants, and forbidden advice. "
        "Return PASS/FAIL and issues only. "
        "Schema:\n"
        + json.dumps(schema)
        + "\nBundle:\n"
        + json.dumps({"metrics": bundle.get("metrics"), "tables": bundle.get("docx_tables")})
        + "\nParagraphs:\n"
        + json.dumps(paragraphs)
    )
    messages = [{"role": "user", "content": prompt}]
    return run_gpt_json(messages, timeout=60.0)


def run_publisher(angle: Dict[str, Any], paragraphs: List[str]) -> Dict[str, Any]:
    schema = {"headline": "string", "dek": "15-25 words", "body": ["string"]}
    prompt = (
        "You are the Editor-in-Chief. Output JSON only. "
        "Rewrite for publish-ready tone. "
        "Include a 15-25 word dek. "
        "No scaffolding phrases or advice. "
        "Schema:\n"
        + json.dumps(schema)
        + "\nAngle:\n"
        + json.dumps(angle)
        + "\nParagraphs:\n"
        + json.dumps(paragraphs)
    )
    messages = [{"role": "user", "content": prompt}]
    return run_gpt_json(messages, timeout=70.0)


def build_pipeline(bundle: Dict[str, Any], *, output_dir: str, editor_notes: Optional[str] = None) -> Tuple[Dict[str, Any], List[str]]:
    issues: List[str] = []

    metric_pool = build_metric_pool(bundle)
    charts = build_chart_bank(bundle, metric_pool)
    tables = [
        {"title": "Core vs Rest summary", "rows": _core_vs_rest_table(bundle, metric_pool)},
        {"title": "Sector composition (Core vs Rest)", "rows": _sector_table(bundle)},
    ]
    bundle["metric_pool"] = metric_pool
    bundle["docx_charts"] = charts
    bundle["docx_tables"] = tables
    _write_debug(output_dir, "metrics", {"metric_pool": metric_pool, "charts": charts, "tables": tables})

    try:
        investigator = run_data_investigator(bundle, metric_pool)
    except GPTClientError as exc:
        issues.append(str(exc))
        investigator = {"insight_candidates": []}
    _write_debug(output_dir, "investigator", investigator)

    angles = ensure_angle_count(generate_angles(bundle, metric_pool))
    with get_connection() as conn:  # type: ignore[name-defined]
        ranked = rank_angles(angles, report_type=bundle.get("report_type", ""), conn=conn)
    top = ranked[0] if ranked else {"angle_title": "Core vs Rest overview", "callouts": []}
    _write_debug(output_dir, "angles", {"ranked": ranked})

    try:
        editor = run_editor(bundle, ranked[:5])
        outline = editor.get("outline") or []
    except GPTClientError as exc:
        issues.append(str(exc))
        outline = []
    _write_debug(output_dir, "outline", {"outline": outline})

    try:
        writer = run_writer(top, outline)
        paragraphs = writer.get("paragraphs") or []
        outline = writer.get("outline") or outline
    except GPTClientError as exc:
        issues.append(str(exc))
        paragraphs = []
    _write_debug(output_dir, "writer", {"paragraphs": paragraphs})

    if not paragraphs:
        paragraphs = [
            "This report summarizes governance signals using the Core vs Rest lens.",
            "Figure 1 and Table 1 provide the supporting evidence for the score distributions and sector composition.",
        ]
    try:
        copyedited = run_copyeditor(paragraphs)
        paragraphs = copyedited.get("paragraphs") or paragraphs
    except GPTClientError as exc:
        issues.append(str(exc))
    _write_debug(output_dir, "copyedited", {"paragraphs": paragraphs})

    try:
        fact = run_fact_checker(bundle, paragraphs)
        if fact.get("status") == "FAIL":
            issues.extend(fact.get("issues") or [])
    except GPTClientError as exc:
        issues.append(str(exc))

    paragraphs = _ensure_core_rest_mentions(paragraphs, metric_pool)
    try:
        published = run_publisher(top, paragraphs)
    except GPTClientError as exc:
        issues.append(str(exc))
        published = {"headline": top.get("angle_title"), "dek": None, "body": paragraphs}

    body = sanitize_text_blocks(published.get("body") or paragraphs)
    draft = {
        "headline": published.get("headline") or top.get("angle_title"),
        "dek": published.get("dek"),
        "paragraphs": body,
        "outline": outline,
    }
    _write_debug(output_dir, "final", draft)
    return draft, issues
