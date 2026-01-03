"""Publisher-in-chief rewrite pass for research generator."""
from __future__ import annotations

import json
import re
from typing import Any, Dict, List

from .gpt_client import run_gpt_json


_FILLER_RE = re.compile(
    r"(provides the detailed breakdown|what it shows|chart above|table above|figure below|table below)",
    re.IGNORECASE,
)


def linearize_draft(
    *,
    headline: str,
    paragraphs: List[str],
    outline: List[Dict[str, Any]],
    charts: List[Dict[str, Any]],
    tables: List[Dict[str, Any]],
) -> str:
    lines: List[str] = []
    lines.append("[HEADLINE]")
    lines.append(headline or "")
    lines.append("[BODY]")

    para_iter = iter(paragraphs)
    for item in outline:
        if item.get("type") == "paragraph":
            text = item.get("text")
            if text is None:
                text = next(para_iter, "")
            if text and _FILLER_RE.search(str(text)):
                continue
            lines.append(str(text or "").strip())
        elif item.get("type") == "figure":
            fig_id = int(item.get("id") or 0)
            caption = ""
            if 0 < fig_id <= len(charts):
                caption = str(charts[fig_id - 1].get("caption") or "")
            lines.append(f"[INSERT FIGURE {fig_id}: {caption}]")
        elif item.get("type") == "table":
            tbl_id = int(item.get("id") or 0)
            caption = ""
            if 0 < tbl_id <= len(tables):
                caption = str(tables[tbl_id - 1].get("title") or "")
            lines.append(f"[INSERT TABLE {tbl_id}: {caption}]")

    return "\n".join([line for line in lines if line.strip()])


def publisher_in_chief_rewrite(linear_draft: str, *, constraints: Dict[str, Any]) -> Dict[str, Any]:
    schema = {
        "headline": "8-14 words, factual and curiosity-driven",
        "dek": "15-25 words, single sentence",
        "body": ["2-5 paragraphs, continuous narrative"],
    }
    prompt = (
        "You are the PUBLISHER-IN-CHIEF. Rewrite the draft for publication. "
        "Keep all metrics unchanged and do not invent facts. "
        "Output JSON only. "
        "Headline: 8-14 words, Financial Times style, factual, no hype words. "
        "Dek: 15-25 words, single sentence, placed above paragraph 1. "
        "Body: 2-5 paragraphs, smooth narrative with inline artifact references like 'Figure 1' or 'Table 2'. "
        "Use first-mention definitions only: "
        "AI Governance & Ethics Score (AIGES), the Core (the 25 non-zero-weight constituents), "
        "the Coverage set (the full 100-name monitoring universe, including zero-weight names), "
        "and the quarterly rebalance (composition/weights refresh). "
        "Forbidden phrases: 'chart above', 'table above', 'figure below', 'table below', "
        "'provides the detailed breakdown', 'what it shows', 'needs review'. "
        "No investment advice, no prices, no buy/sell language, no external claims. "
        "Entrants/exits are membership changes only; only incumbents get deltas. "
        "Constraints:\n"
        + json.dumps(constraints)
        + "\nDraft:\n"
        + linear_draft
        + "\nSchema:\n"
        + json.dumps(schema)
    )
    messages = [{"role": "user", "content": prompt}]
    return run_gpt_json(messages, timeout=70.0)
