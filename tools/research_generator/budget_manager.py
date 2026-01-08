"""Adaptive budget manager for research generator."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class Profile:
    name: str
    max_angles: int
    max_candidate_metrics: int
    max_charts: int
    max_tables: int
    max_iterations: int
    time_budget_minutes: int
    max_prompt_chars: int


@dataclass
class BudgetDecision:
    profile: Profile
    stop_reason: str
    usage_unavailable: bool = False


MEDIUM = Profile(
    name="MEDIUM",
    max_angles=3,
    max_candidate_metrics=60,
    max_charts=2,
    max_tables=3,
    max_iterations=1,
    time_budget_minutes=8,
    max_prompt_chars=12000,
)

LOW = Profile(
    name="LOW",
    max_angles=2,
    max_candidate_metrics=40,
    max_charts=1,
    max_tables=2,
    max_iterations=1,
    time_budget_minutes=6,
    max_prompt_chars=9000,
)

MINIMAL = Profile(
    name="MINIMAL",
    max_angles=1,
    max_candidate_metrics=20,
    max_charts=1,
    max_tables=1,
    max_iterations=0,
    time_budget_minutes=4,
    max_prompt_chars=6000,
)


def _used_pct(snapshot: Dict[str, Any], key: str) -> Optional[float]:
    if not snapshot.get("available"):
        return None
    section = snapshot.get(key) or {}
    value = section.get("used_pct")
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def choose_profile(settings: Dict[str, Any]) -> BudgetDecision:
    max_context_pct = settings.get("max_context_pct") if settings else None
    saver_mode = str(settings.get("saver_mode") or "MEDIUM").upper()
    if max_context_pct is not None:
        try:
            max_context_pct = float(max_context_pct)
        except (TypeError, ValueError):
            max_context_pct = None

    if max_context_pct is not None and max_context_pct <= 5:
        return BudgetDecision(profile=MINIMAL, stop_reason="context_budget_minimal")
    if max_context_pct is not None and max_context_pct <= 8:
        return BudgetDecision(profile=LOW, stop_reason="context_budget_low")

    if saver_mode == "MINIMAL":
        return BudgetDecision(profile=MINIMAL, stop_reason="saver_mode_minimal")
    if saver_mode == "LOW":
        return BudgetDecision(profile=LOW, stop_reason="saver_mode_low")
    return BudgetDecision(profile=MEDIUM, stop_reason="saver_mode_medium")
