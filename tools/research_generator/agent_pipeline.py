"""Agent pipeline for research generator (OpenAI-only)."""
from __future__ import annotations

import datetime as dt
import json
import logging
import os
from typing import Any, Dict, List, Optional, Tuple

from .codex_cli_runner import CodexCLIError, call_codex
from .data_integrity import run_integrity
from .budget_manager import Profile
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


def _time_exceeded(started_at: dt.datetime, time_budget_minutes: int) -> bool:
    if time_budget_minutes <= 0:
        return True
    elapsed = (dt.datetime.utcnow() - started_at).total_seconds()
    return elapsed > time_budget_minutes * 60


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


def _inject_integrity_note(paragraphs: List[str], integrity: Dict[str, Any]) -> List[str]:
    warnings = integrity.get("warnings") or []
    if not warnings:
        return paragraphs
    if "rest_aiges_low_due_to_missing" in warnings:
        note = (
            "Data note: the Rest cohort shows higher missingness in parts of the score data, so the report emphasizes dispersion and sector composition rather than absolute level comparisons."
            " This keeps the narrative grounded in the most reliable signals."
        )
    else:
        note = (
            "Data note: some fields show elevated missingness, so the narrative prioritizes dispersion and composition signals over absolute level comparisons."
        )
    if paragraphs:
        paragraphs.insert(1, note)
    else:
        paragraphs = [note]
    return paragraphs


def _template_paragraphs(metric_pool: List[Dict[str, Any]]) -> List[str]:
    def _metric(name: str, default: float = 0.0) -> float:
        for item in metric_pool:
            if item.get("name") == name:
                try:
                    return float(item.get("value"))
                except (TypeError, ValueError):
                    return default
        return default

    core_mean = _metric("core.aiges.mean")
    rest_mean = _metric("rest.aiges.mean")
    core_med = _metric("core.aiges.p50")
    rest_med = _metric("rest.aiges.p50")
    core_iqr = _metric("core.aiges.p75") - _metric("core.aiges.p25")
    rest_iqr = _metric("rest.aiges.p75") - _metric("rest.aiges.p25")
    turnover = _metric("core.turnover.membership")
    delta_core = _metric("delta.core.aiges.mean")
    return [
        (
            "Figure 1 frames the Core vs Rest comparison: Core mean AIGES is "
            f"{core_mean:.2f} vs Rest {rest_mean:.2f}, with medians at {core_med:.2f} and {rest_med:.2f}."
        ),
        (
            "Table 1 anchors dispersion: Core IQR is {core_iqr:.2f} versus Rest {rest_iqr:.2f}. "
            f"That spread signals a Core vs Rest gap of {core_mean - rest_mean:.2f} points."
        ).format(core_iqr=core_iqr, rest_iqr=rest_iqr),
        (
            "Change vs prior rebalance shows a Core mean shift of {delta:.2f}, while membership turnover "
            f"is {turnover:.2%}. Table 2 summarizes sector composition changes alongside these shifts."
        ).format(delta=delta_core),
        (
            "Figure 2 highlights distribution shape differences alongside the Core vs Rest split; "
            f"the Core median {core_med:.2f} remains above the Rest median {rest_med:.2f}."
        ),
    ]


def _ensure_artifact_mentions(
    paragraphs: List[str],
    metric_pool: List[Dict[str, Any]],
) -> List[str]:
    text = " ".join(paragraphs).lower()
    if "figure" in text and "table" in text:
        return paragraphs

    def _metric(name: str, default: float = 0.0) -> float:
        for item in metric_pool:
            if item.get("name") == name:
                try:
                    return float(item.get("value"))
                except (TypeError, ValueError):
                    return default
        return default

    core_mean = _metric("core.aiges.mean")
    rest_mean = _metric("rest.aiges.mean")
    core_med = _metric("core.aiges.p50")
    rest_med = _metric("rest.aiges.p50")
    additions: List[str] = []
    if "figure" not in text:
        additions.append(
            f"Figure 1 highlights the Core vs Rest distribution, with means {core_mean:.2f} and {rest_mean:.2f}."
        )
    if "table" not in text:
        additions.append(
            f"Table 1 summarizes the Core vs Rest medians at {core_med:.2f} and {rest_med:.2f}."
        )
    if paragraphs:
        paragraphs[0] = paragraphs[0].rstrip() + " " + " ".join(additions)
    else:
        paragraphs = additions
    return paragraphs


def _ensure_prior_comparison(
    paragraphs: List[str],
    metric_pool: List[Dict[str, Any]],
) -> List[str]:
    text = " ".join(paragraphs).lower()
    if "prior" in text or "previous" in text:
        return paragraphs

    def _metric(name: str, default: float = 0.0) -> float:
        for item in metric_pool:
            if item.get("name") == name:
                try:
                    return float(item.get("value"))
                except (TypeError, ValueError):
                    return default
        return default

    delta_core = _metric("delta.core.aiges.mean")
    sentence = f"Compared with the prior rebalance, the Core mean AIGES shifted by {delta_core:.2f} points."
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
        "Each candidate must include metric + direction + magnitude + why it matters. "
        "Schema:\n"
        + json.dumps(schema)
        + "\nMetric pool:\n"
        + json.dumps(metric_pool)
    )
    return call_codex(prompt, purpose="data_investigator", expect_json=True)


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
        "Ensure the outline includes at least one paragraph for Core vs Rest differences and one for change vs prior rebalance. "
        "Schema:\n"
        + json.dumps(schema)
        + "\nAngles:\n"
        + json.dumps(angles)
    )
    return call_codex(prompt, purpose="editor", expect_json=True)


def run_writer(angle: Dict[str, Any], outline: List[Dict[str, Any]]) -> Dict[str, Any]:
    schema = {
        "headline": "8-14 words",
        "paragraphs": ["string paragraphs"],
        "outline": [{"type": "paragraph|figure|table", "id": 1, "text": "string"}],
    }
    prompt = (
        "You are the Writer. Output JSON only. "
        "Write a cohesive narrative using the outline. "
        "Each paragraph must include at least two numeric facts. "
        "Include at least 8 distinct insights with metric, direction, and magnitude. "
        "Include at least one 'change vs prior rebalance' insight. "
        "Reference artifacts inline as 'Figure 1' or 'Table 1'. "
        "Avoid scaffolding phrases like 'what it shows' or 'provides the detailed breakdown'. "
        "Schema:\n"
        + json.dumps(schema)
        + "\nAngle:\n"
        + json.dumps(angle)
        + "\nOutline:\n"
        + json.dumps(outline)
    )
    return call_codex(prompt, purpose="writer", expect_json=True)


def run_copyeditor(paragraphs: List[str]) -> Dict[str, Any]:
    schema = {"paragraphs": ["string"]}
    prompt = (
        "You are the Copyeditor. Output JSON only. "
        "Improve flow, remove repetition, keep numbers unchanged. "
        "Remove templated phrasing and any scaffolding language. "
        "Schema:\n"
        + json.dumps(schema)
        + "\nParagraphs:\n"
        + json.dumps(paragraphs)
    )
    return call_codex(prompt, purpose="copyeditor", expect_json=True)


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
    return call_codex(prompt, purpose="fact_checker", expect_json=True)


def run_publisher(angle: Dict[str, Any], paragraphs: List[str]) -> Dict[str, Any]:
    schema = {"headline": "string", "dek": "15-25 words", "body": ["string"]}
    prompt = (
        "You are the Editor-in-Chief. Output JSON only. "
        "Rewrite for publish-ready tone. "
        "Include a 15-25 word dek. "
        "Ensure each paragraph includes at least two numeric facts and one interpretation. "
        "Avoid scaffolding phrases like 'Figure X provides the detailed breakdown' or 'anchors the evidence'. "
        "No advice. "
        "Schema:\n"
        + json.dumps(schema)
        + "\nAngle:\n"
        + json.dumps(angle)
        + "\nParagraphs:\n"
        + json.dumps(paragraphs)
    )
    return call_codex(prompt, purpose="publisher", expect_json=True)


def build_pipeline(
    bundle: Dict[str, Any],
    *,
    output_dir: str,
    editor_notes: Optional[str] = None,
    template_only: bool = False,
    profile: Optional[Profile] = None,
) -> Tuple[Dict[str, Any], List[str]]:
    issues: List[str] = []
    codex_failed = False
    profile = profile or Profile(
        name="MEDIUM",
        max_angles=3,
        max_candidate_metrics=25,
        max_charts=2,
        max_tables=3,
        max_iterations=2,
        time_budget_minutes=8,
    )
    started_at = dt.datetime.utcnow()

    metric_pool = build_metric_pool(bundle, max_metrics=profile.max_candidate_metrics)
    integrity = bundle.get("integrity") or run_integrity(bundle)
    bundle["integrity"] = integrity
    charts = build_chart_bank(bundle, metric_pool, max_charts=profile.max_charts)
    tables = [
        {"title": "Core vs Rest summary", "rows": _core_vs_rest_table(bundle, metric_pool)},
        {"title": "Sector composition (Core vs Rest)", "rows": _sector_table(bundle)},
    ]
    if profile.max_tables and len(tables) > profile.max_tables:
        tables = tables[: profile.max_tables]
    bundle["metric_pool"] = metric_pool
    bundle["docx_charts"] = charts
    bundle["docx_tables"] = tables
    if integrity.get("issues"):
        issues.extend(integrity.get("issues"))
    if integrity.get("warnings"):
        issues.extend(integrity.get("warnings"))
    _write_debug(output_dir, "metrics", {"metric_pool": metric_pool, "charts": charts, "tables": tables})

    investigator = {"insight_candidates": []}
    if not template_only and not _time_exceeded(started_at, profile.time_budget_minutes):
        try:
            investigator = run_data_investigator(bundle, metric_pool)
        except CodexCLIError as exc:
            issues.append(str(exc))
            codex_failed = True
    _write_debug(output_dir, "investigator", investigator)

    angles = []
    if not _time_exceeded(started_at, profile.time_budget_minutes):
        angles = generate_angles(
            bundle,
            metric_pool,
            max_angles=profile.max_angles,
            max_candidate_metrics=profile.max_candidate_metrics,
        )
    angles = ensure_angle_count(angles, minimum=max(profile.max_angles, 1))
    with get_connection() as conn:  # type: ignore[name-defined]
        ranked = rank_angles(angles, report_type=bundle.get("report_type", ""), conn=conn)
    top = ranked[0] if ranked else {"angle_title": "Core vs Rest overview", "callouts": []}
    for candidate in ranked:
        categories = candidate.get("categories") or []
        if "core vs rest" in str(candidate.get("angle_title", "")).lower() and len(categories) < 2:
            continue
        top = candidate
        break
    bundle["selected_angle"] = top
    _write_debug(output_dir, "angles", {"ranked": ranked})

    outline = []
    if not template_only and not _time_exceeded(started_at, profile.time_budget_minutes):
        try:
            editor = run_editor(bundle, ranked[: max(profile.max_angles, 1)])
            outline = editor.get("outline") or []
        except CodexCLIError as exc:
            issues.append(str(exc))
            codex_failed = True
    _write_debug(output_dir, "outline", {"outline": outline})

    paragraphs: List[str] = []
    if not template_only and not _time_exceeded(started_at, profile.time_budget_minutes):
        try:
            writer = run_writer(top, outline)
            paragraphs = writer.get("paragraphs") or []
            outline = writer.get("outline") or outline
        except CodexCLIError as exc:
            issues.append(str(exc))
            codex_failed = True
    _write_debug(output_dir, "writer", {"paragraphs": paragraphs})

    if not paragraphs:
        paragraphs = _template_paragraphs(metric_pool)
    if not template_only and not _time_exceeded(started_at, profile.time_budget_minutes):
        try:
            copyedited = run_copyeditor(paragraphs)
            paragraphs = copyedited.get("paragraphs") or paragraphs
        except CodexCLIError as exc:
            issues.append(str(exc))
            codex_failed = True
    _write_debug(output_dir, "copyedited", {"paragraphs": paragraphs})

    if not template_only and not _time_exceeded(started_at, profile.time_budget_minutes):
        try:
            fact = run_fact_checker(bundle, paragraphs)
            if fact.get("status") == "FAIL":
                issues.extend(fact.get("issues") or [])
        except CodexCLIError as exc:
            issues.append(str(exc))
            codex_failed = True

    paragraphs = _inject_integrity_note(paragraphs, integrity)
    paragraphs = _ensure_core_rest_mentions(paragraphs, metric_pool)
    if not template_only and not _time_exceeded(started_at, profile.time_budget_minutes):
        try:
            published = run_publisher(top, paragraphs)
        except CodexCLIError as exc:
            issues.append(str(exc))
            codex_failed = True
            published = {"headline": top.get("angle_title"), "dek": None, "body": paragraphs}
    else:
        published = {"headline": top.get("angle_title"), "dek": None, "body": paragraphs}

    body = sanitize_text_blocks(published.get("body") or paragraphs)
    body = _ensure_artifact_mentions(body, metric_pool)
    body = _ensure_prior_comparison(body, metric_pool)
    draft = {
        "headline": published.get("headline") or top.get("angle_title"),
        "dek": published.get("dek"),
        "paragraphs": body,
        "outline": outline,
        "template_mode": template_only or codex_failed,
    }
    _write_debug(output_dir, "final", draft)
    return draft, issues
