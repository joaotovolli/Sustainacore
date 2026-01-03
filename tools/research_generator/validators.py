"""Validation helpers for research drafts."""
from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple

from . import config


class ValidationError(Exception):
    pass


_CURRENCY_RE = re.compile(r"[\$€£¥]")
_NUMBER_RE = re.compile(r"\b\d+(?:\.\d+)?\b")


def _word_count(text: str) -> int:
    return len([w for w in (text or "").strip().split() if w])


def validate_writer_output(bundle: Dict[str, Any], writer: Dict[str, Any]) -> Tuple[bool, List[str]]:
    issues: List[str] = []
    headline = (writer.get("headline") or "").strip()
    paragraphs = writer.get("paragraphs") or []
    table_caption = str(writer.get("table_caption") or "").strip()
    chart_caption = str(writer.get("chart_caption") or "").strip()
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
    has_series = bool(chart.get("series"))
    if not chart.get("x") or not (chart.get("y") or has_series):
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


def validate_quality_gate(
    bundle: Dict[str, Any],
    writer: Dict[str, Any],
    compute: Dict[str, Any],
) -> Tuple[bool, List[str]]:
    issues: List[str] = []
    paragraphs = " ".join(writer.get("paragraphs") or []).lower()
    report_type = (bundle.get("report_type") or "").upper()

    if report_type == "REBALANCE":
        if "core" not in paragraphs or "coverage" not in paragraphs:
            issues.append("missing_core_coverage")
        required_terms = ["iqr", "hhi", "turnover", "breadth"]
        stat_hits = sum(1 for term in required_terms if term in paragraphs)
        if stat_hits < 3:
            issues.append("missing_non_trivial_stats")

        flags = (compute.get("validation_flags") or {}).get("sector_delta_inconsistent")
        if flags and "flag" not in paragraphs and "inconsistent" not in paragraphs:
            issues.append("sector_delta_not_flagged")

    return (len(issues) == 0), issues


def quality_gate_strict(
    bundle: Dict[str, Any],
    writer: Dict[str, Any],
    compute: Dict[str, Any],
    docx_meta: Dict[str, Any],
) -> Tuple[bool, List[str]]:
    issues: List[str] = []
    report_type = (bundle.get("report_type") or "").upper()
    metrics = bundle.get("metrics") or {}
    coverage_mean = (metrics.get("coverage") or {}).get("mean_aiges")
    if coverage_mean in (None, ""):
        issues.append("coverage_mean_missing")

    paragraphs = " ".join(writer.get("paragraphs") or []).lower()
    if "chart" not in paragraphs and "figure" not in paragraphs:
        issues.append("missing_chart_reference")
    if "table" not in paragraphs:
        issues.append("missing_table_reference")

    numeric_refs = len(_NUMBER_RE.findall(" ".join(writer.get("paragraphs") or [])))
    if numeric_refs < 6:
        issues.append("insufficient_numeric_references")

    if report_type == "REBALANCE":
        required_terms = ["iqr", "hhi", "turnover", "breadth"]
        stat_hits = sum(1 for term in required_terms if term in paragraphs)
        if stat_hits < 3:
            issues.append("missing_non_trivial_stats")

        flagged = (metrics.get("sector_exposure") or {}).get("core_count_delta_flags") or []
        if flagged:
            sector_table = None
            for table in bundle.get("docx_tables") or []:
                if table.get("title") == "Sector Exposure Comparison":
                    sector_table = table
                    break
            if sector_table:
                for row in sector_table.get("rows", []):
                    if row.get("Sector") in flagged and row.get("Coverage Count Delta") != "Not comparable":
                        issues.append("sector_delta_flag_missing")
                        break

    if not docx_meta.get("table_style_applied"):
        issues.append("table_style_missing")

    ok = len(issues) == 0
    return ok, issues
