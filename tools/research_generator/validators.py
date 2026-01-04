"""Validation helpers for research drafts."""
from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple

from . import config


class ValidationError(Exception):
    pass


_CURRENCY_RE = re.compile(r"[\$€£¥]")
_NUMBER_RE = re.compile(r"[+-]?\d+(?:\.\d+)?")


def _word_count(text: str) -> int:
    return len([w for w in (text or "").strip().split() if w])


def validate_writer_output(bundle: Dict[str, Any], writer: Dict[str, Any]) -> Tuple[bool, List[str]]:
    issues: List[str] = []
    headline = (writer.get("headline") or "").strip()
    paragraphs = writer.get("paragraphs") or []
    table_caption = (writer.get("table_caption") or "").strip()
    chart_caption = (writer.get("chart_caption") or "").strip()
    compliance = writer.get("compliance_checklist") or {}

    if not headline:
        issues.append("missing_headline")
    else:
        words = _word_count(headline)
        if words < config.HEADLINE_MIN_WORDS or words > config.HEADLINE_MAX_WORDS:
            issues.append("headline_word_count")
        _check_banned(headline, issues)

    if not isinstance(paragraphs, list) or not (2 <= len(paragraphs) <= 4):
        issues.append("paragraph_count")
    else:
        for para in paragraphs:
            _check_banned(str(para), issues)

    if not table_caption:
        issues.append("missing_table_caption")
    else:
        _check_banned(table_caption, issues)

    if not chart_caption:
        issues.append("missing_chart_caption")
    else:
        _check_banned(chart_caption, issues)

    if not compliance.get("no_prices"):
        issues.append("compliance_no_prices_false")
    if not compliance.get("no_advice"):
        issues.append("compliance_no_advice_false")
    if not compliance.get("tone_ok"):
        issues.append("compliance_tone_false")

    table_rows = bundle.get("table_rows") or []
    if len(table_rows) > config.MAX_TABLE_ROWS:
        issues.append("table_rows_exceed_limit")

    chart = bundle.get("chart_data") or {}
    if not chart.get("x") or not chart.get("y"):
        issues.append("missing_chart_data")

    if not _table_columns_ok(table_rows, issues):
        issues.append("table_columns_invalid")

    return (len(issues) == 0), issues


def _table_columns_ok(rows: List[Dict[str, Any]], issues: List[str]) -> bool:
    if not rows:
        return False
    for key in rows[0].keys():
        if len(str(key)) > config.MAX_COLUMN_NAME_LENGTH:
            issues.append("column_name_too_long")
            return False
    return True


def _check_banned(text: str, issues: List[str]) -> None:
    lower = text.lower()
    for phrase in config.BANNED_PHRASES:
        if phrase in lower:
            issues.append(f"banned_phrase:{phrase}")
    if _CURRENCY_RE.search(text):
        issues.append("currency_symbol")


def quality_gate_strict(bundle: Dict[str, Any], draft: Dict[str, Any]) -> Tuple[bool, List[str]]:
    issues: List[str] = []
    text = " ".join(draft.get("paragraphs") or [])
    lower = text.lower()

    forbidden = [
        "chart above",
        "table above",
        "what it shows",
        "figure x — what it shows",
        "table x — key takeaways",
        "anchors the evidence",
        "provides the detailed breakdown",
    ]
    for phrase in forbidden:
        if phrase in lower:
            issues.append(f"forbidden_phrase:{phrase}")

    if re.search(r"\d+\.\s+\d+", text):
        issues.append("spaced_decimal")

    if "figure" not in lower:
        issues.append("missing_figure_reference")
    if "table" not in lower:
        issues.append("missing_table_reference")

    if "core" not in lower or "rest" not in lower:
        issues.append("missing_core_vs_rest_mentions")
    if "prior" not in lower and "previous" not in lower:
        issues.append("missing_prior_comparison")

    if len(bundle.get("docx_charts") or []) < 2:
        issues.append("insufficient_charts")
    if len(bundle.get("docx_tables") or []) < 2:
        issues.append("insufficient_tables")

    if not any("Core vs Rest" in str(t.get("title") or "") for t in (bundle.get("docx_tables") or [])):
        issues.append("missing_core_vs_rest_table")

    if any("Weight Movers" in str(t.get("title") or "") for t in (bundle.get("docx_tables") or [])):
        issues.append("weight_movers_not_allowed")

    metric_pool = bundle.get("metric_pool") or []
    if len(metric_pool) < 100:
        issues.append("metric_pool_too_small")

    selected_angle = bundle.get("selected_angle") or {}
    callouts = selected_angle.get("callouts") or []
    if len(callouts) < 8:
        issues.append("insufficient_angle_callouts")

    total_numbers = len(_NUMBER_RE.findall(text))
    if total_numbers < 8:
        issues.append("insufficient_numeric_insights")

    # Each paragraph should include at least two numeric references.
    for paragraph in draft.get("paragraphs") or []:
        if len(_NUMBER_RE.findall(paragraph)) < 2:
            issues.append("insufficient_paragraph_numbers")
            break

    return (len(issues) == 0), issues
