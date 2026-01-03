"""Data quality checks for research generator."""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import Any, Dict, List, Tuple


@dataclass
class FactCheckResult:
    critical: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    details: List[str] = field(default_factory=list)

    def ok(self) -> bool:
        return not self.critical


def _detect_weight_mode(raw_weights: List[Any]) -> str:
    values = [float(w) for w in raw_weights if w is not None]
    if not values:
        return "unknown"
    max_weight = max(values)
    total = sum(values)
    if max_weight > 1.5 or total > 1.5:
        return "percent"
    return "fraction"


def _weight_fraction(value: Any, mode: str) -> float:
    if value is None:
        return 0.0
    raw = float(value)
    if mode == "percent":
        return raw / 100.0
    return raw


def _unique_duplicates(items: List[str]) -> List[str]:
    seen = {}
    duplicates = []
    for item in items:
        if not item:
            continue
        seen[item] = seen.get(item, 0) + 1
    for item, count in seen.items():
        if count > 1:
            duplicates.append(item)
    return duplicates


def run_fact_check(bundle: Dict[str, Any]) -> FactCheckResult:
    result = FactCheckResult()
    metrics = bundle.get("metrics") or {}
    core_metrics = metrics.get("core") or {}
    sector_metrics = metrics.get("sector_exposure") or {}
    top_movers = metrics.get("top_movers") or {}

    raw_weights = core_metrics.get("weights_raw") or []
    mode = _detect_weight_mode(raw_weights)
    weight_fracs = [_weight_fraction(w, mode) for w in raw_weights]

    if mode == "percent":
        if any(w > 100 or w < 0 for w in raw_weights if w is not None):
            result.critical.append("weight_out_of_bounds_percent")
            result.details.append(f"Raw weight out of 0-100% range: {raw_weights[:6]}")
    elif mode == "fraction":
        if any(w > 1 or w < 0 for w in weight_fracs):
            result.critical.append("weight_out_of_bounds_fraction")
            result.details.append(f"Weight fraction out of 0-1 range: {weight_fracs[:6]}")

    if weight_fracs:
        total = sum(weight_fracs)
        if abs(total - 1.0) > 0.02:
            result.critical.append("core_weight_sum_invalid")
            result.details.append(f"Core weight sum {total:.4f} not near 1.0")

        n = len(weight_fracs)
        target = 1.0 / n if n else 0.0
        if n and max(abs(w - target) for w in weight_fracs) < 0.002:
            for w in weight_fracs:
                if abs(w - target) > 0.004:
                    result.critical.append("equal_weight_outlier")
                    result.details.append(f"Equal-weight deviation {w:.4f} vs {target:.4f}")
                    break

    core_exposure = sector_metrics.get("core_weighted_latest") or {}
    if core_exposure:
        total = sum(core_exposure.values())
        if abs(total - 1.0) > 0.02:
            result.critical.append("core_sector_sum_invalid")
            result.details.append(f"Core sector exposure sum {total:.4f} not near 1.0")

    coverage_counts = sector_metrics.get("coverage_count_latest") or {}
    if coverage_counts:
        total = sum(coverage_counts.values())
        if abs(total - 100.0) > 2.0:
            result.critical.append("coverage_sector_sum_invalid")
            result.details.append(f"Coverage sector exposure sum {total:.2f} not near 100")

    incumbents = {row.get("ticker") for row in top_movers.get("incumbent_weight", [])}
    entrants = {row.get("ticker") for row in top_movers.get("entrants", [])}
    exits = {row.get("ticker") for row in top_movers.get("exits", [])}
    overlap = incumbents & (entrants | exits)
    if overlap:
        result.critical.append("entrant_exit_in_incumbents")
        result.details.append(f"Overlap in mover tables: {sorted(overlap)}")

    for row in top_movers.get("incumbent_weight", []):
        delta_pp = float(row.get("delta_weight") or 0) * 100
        if abs(delta_pp) > 50:
            result.critical.append("delta_weight_pp_out_of_bounds")
            result.details.append(f"Delta weight pp {delta_pp:.1f} for {row.get('ticker')}")
            break

    for row in top_movers.get("incumbent_weight", []):
        prev = float(row.get("weight_prev") or 0)
        if prev > 1.0:
            result.critical.append("weight_prev_out_of_bounds")
            result.details.append(f"Prev weight {prev} for {row.get('ticker')}")
            break

    table_callouts = []
    for table in bundle.get("docx_tables") or []:
        for note in table.get("callouts") or []:
            table_callouts.append(note)
    duplicates = _unique_duplicates(table_callouts)
    if duplicates:
        result.warnings.append("duplicate_table_takeaways")
        result.details.append(f"Duplicate takeaways: {duplicates[:3]}")

    return result
