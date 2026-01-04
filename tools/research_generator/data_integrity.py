"""Data integrity and scale checks for research reports."""
from __future__ import annotations

import math
from typing import Any, Dict, List, Tuple

from . import config


def _to_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return math.nan


def _stats(values: List[float]) -> Dict[str, float]:
    values = [v for v in values if not math.isnan(v)]
    if not values:
        return {}
    values.sort()
    def pct(p: float) -> float:
        idx = (len(values) - 1) * p
        lo = math.floor(idx)
        hi = math.ceil(idx)
        if lo == hi:
            return values[int(idx)]
        return values[lo] + (values[hi] - values[lo]) * (idx - lo)
    return {
        "min": values[0],
        "max": values[-1],
        "p10": pct(0.10),
        "p50": pct(0.50),
        "p90": pct(0.90),
    }


def run_integrity(bundle: Dict[str, Any]) -> Dict[str, Any]:
    core = bundle.get("core_latest_rows") or []
    rest = bundle.get("rest_latest_rows") or []
    columns = ["aiges"] + [col for col in config.PILLAR_COLUMNS if any(col in row for row in core + rest)]

    report: Dict[str, Any] = {
        "issues": [],
        "warnings": [],
        "core": {},
        "rest": {},
    }

    for group_name, rows in (("core", core), ("rest", rest)):
        group_report: Dict[str, Any] = {}
        for col in columns:
            values = [_to_float(row.get(col)) for row in rows]
            missing = sum(1 for v in values if math.isnan(v))
            stats = _stats(values)
            group_report[col] = {
                "count": len(values),
                "missing": missing,
                "missing_rate": missing / max(len(values), 1),
                "stats": stats,
            }
            if stats:
                if stats["min"] < 0 or stats["max"] > 100:
                    report["issues"].append(f"{group_name}.{col}.out_of_range")
                if missing / max(len(values), 1) > 0.4:
                    report["warnings"].append(f"{group_name}.{col}.missing_high")
                if stats["p50"] <= 5 and missing / max(len(values), 1) > 0.2:
                    report["warnings"].append(f"{group_name}.{col}.low_median_high_missing")
        report[group_name] = group_report

    # Detect suspicious core vs rest scale gaps due to missingness.
    try:
        core_med = report["core"]["aiges"]["stats"]["p50"]
        rest_med = report["rest"]["aiges"]["stats"]["p50"]
        rest_missing = report["rest"]["aiges"]["missing_rate"]
        if rest_missing > 0.35 and core_med - rest_med > 15:
            report["warnings"].append("rest_aiges_low_due_to_missing")
    except KeyError:
        pass

    return report
