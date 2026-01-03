"""Insight miner for research generator."""
from __future__ import annotations

from typing import Any, Dict, List


def _fmt(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def mine_insights(bundle: Dict[str, Any]) -> List[Dict[str, Any]]:
    metrics = bundle.get("metrics") or {}
    core = metrics.get("core") or {}
    coverage = metrics.get("coverage") or {}
    gaps = metrics.get("gaps") or {}
    rebalance = metrics.get("rebalance") or {}
    sector = metrics.get("sector_exposure") or {}

    insights: List[Dict[str, Any]] = []

    mean_gap = gaps.get("mean_gap_core_vs_coverage")
    if mean_gap is not None:
        insights.append(
            {
                "title": "Core vs coverage gap",
                "evidence": f"Mean gap {mean_gap:+.2f} points (core vs coverage).",
                "score": 0.9,
                "tags": ["gap"],
            }
        )

    if core.get("std_aiges") is not None and coverage.get("std_aiges") is not None:
        insights.append(
            {
                "title": "Dispersion shift",
                "evidence": f"Std dev core {core.get('std_aiges')} vs coverage {coverage.get('std_aiges')}",
                "score": 0.8,
                "tags": ["dispersion"],
            }
        )

    if core.get("membership_turnover_pct") is not None:
        insights.append(
            {
                "title": "Membership turnover",
                "evidence": f"Membership turnover {core.get('membership_turnover_pct')}% of Core.",
                "score": 0.75,
                "tags": ["turnover"],
            }
        )

    if core.get("top_quintile_share_pct") is not None:
        insights.append(
            {
                "title": "Top-quintile concentration",
                "evidence": f"Core names in top coverage quintile: {core.get('top_quintile_share_pct')}%.",
                "score": 0.7,
                "tags": ["concentration"],
            }
        )

    notable_moves = sector.get("notable_moves") or []
    if notable_moves:
        sector_name, delta = notable_moves[0]
        insights.append(
            {
                "title": "Sector shift leader",
                "evidence": f"Largest sector move: {sector_name} {delta:+.1f}pp.",
                "score": 0.7,
                "tags": ["sector"],
            }
        )

    sector_turnover = sector.get("sector_turnover") or []
    if sector_turnover:
        top = sorted(sector_turnover, key=lambda r: abs(r.get("Net Change") or 0), reverse=True)[0]
        insights.append(
            {
                "title": "Sector membership churn",
                "evidence": f"{top.get('Sector')} net change {top.get('Net Change')} names.",
                "score": 0.6,
                "tags": ["sector"],
            }
        )

    if core.get("breadth_pct") is not None:
        insights.append(
            {
                "title": "Governance breadth",
                "evidence": f"Breadth {core.get('breadth_pct')}% of incumbents improved.",
                "score": 0.6,
                "tags": ["breadth"],
            }
        )

    entrants = (metrics.get("top_movers") or {}).get("entrants") or []
    if entrants:
        top = entrants[0]
        insights.append(
            {
                "title": "Entrant profile",
                "evidence": f"Entrant {top.get('ticker')} score {_fmt(top.get('aiges_new'))}.",
                "score": 0.55,
                "tags": ["entrants"],
            }
        )

    return sorted(insights, key=lambda i: i.get("score", 0), reverse=True)
