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


@dataclass
class BudgetDecision:
    profile: Profile
    stop_reason: str
    usage_unavailable: bool = False


MEDIUM = Profile(
    name="MEDIUM",
    max_angles=3,
    max_candidate_metrics=25,
    max_charts=2,
    max_tables=3,
    max_iterations=2,
    time_budget_minutes=8,
)

LOW = Profile(
    name="LOW",
    max_angles=1,
    max_candidate_metrics=12,
    max_charts=1,
    max_tables=2,
    max_iterations=1,
    time_budget_minutes=6,
)

MINIMAL = Profile(
    name="MINIMAL",
    max_angles=1,
    max_candidate_metrics=8,
    max_charts=1,
    max_tables=2,
    max_iterations=1,
    time_budget_minutes=5,
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


def choose_profile(snapshot: Dict[str, Any]) -> BudgetDecision:
    weekly = _used_pct(snapshot, "weekly")
    five_hour = _used_pct(snapshot, "five_hour")
    if not snapshot.get("available"):
        safe = Profile(
            name="MEDIUM_SAFE_NO_USAGE",
            max_angles=MEDIUM.max_angles,
            max_candidate_metrics=MEDIUM.max_candidate_metrics,
            max_charts=1,
            max_tables=2,
            max_iterations=1,
            time_budget_minutes=MEDIUM.time_budget_minutes,
        )
        return BudgetDecision(profile=safe, stop_reason="usage_unavailable", usage_unavailable=True)

    if (weekly is not None and weekly >= 95) or (five_hour is not None and five_hour >= 95):
        skip = Profile(
            name="SKIPPED_BUDGET",
            max_angles=0,
            max_candidate_metrics=0,
            max_charts=0,
            max_tables=0,
            max_iterations=0,
            time_budget_minutes=0,
        )
        return BudgetDecision(profile=skip, stop_reason="skipped_budget")

    if (weekly is not None and weekly >= 85) or (five_hour is not None and five_hour >= 85):
        return BudgetDecision(profile=MINIMAL, stop_reason="downgraded_high_usage")

    if (weekly is not None and weekly >= 70) or (five_hour is not None and five_hour >= 70):
        return BudgetDecision(profile=LOW, stop_reason="downgraded_usage")

    return BudgetDecision(profile=MEDIUM, stop_reason="normal")
