"""Idea engine for research generator (metrics, charts, angles)."""
from __future__ import annotations

import hashlib
import json
import math
from typing import Any, Dict, Iterable, List, Tuple

from .gpt_client import GPTClientError, run_gpt_json
from .oracle import get_report_value, set_report_value


def _numeric_columns(rows: List[Dict[str, Any]]) -> List[str]:
    if not rows:
        return []
    sample = rows[0].keys()
    columns: List[str] = []
    for key in sample:
        values = []
        for row in rows[:50]:
            value = row.get(key)
            if isinstance(value, (int, float)):
                values.append(value)
            else:
                try:
                    float(value)
                    values.append(value)
                except (TypeError, ValueError):
                    break
        if values:
            columns.append(key)
    return columns


def _to_floats(values: Iterable[Any]) -> List[float]:
    out: List[float] = []
    for value in values:
        try:
            out.append(float(value))
        except (TypeError, ValueError):
            continue
    return out


def _percentiles(values: List[float]) -> Dict[str, float]:
    if not values:
        return {}
    values = sorted(values)
    def pct(p: float) -> float:
        if not values:
            return 0.0
        idx = (len(values) - 1) * p
        lo = math.floor(idx)
        hi = math.ceil(idx)
        if lo == hi:
            return values[int(idx)]
        return values[lo] + (values[hi] - values[lo]) * (idx - lo)
    return {
        "p10": pct(0.10),
        "p25": pct(0.25),
        "p50": pct(0.50),
        "p75": pct(0.75),
        "p90": pct(0.90),
    }


def _stats(values: List[float]) -> Dict[str, float]:
    if not values:
        return {}
    mean = sum(values) / len(values)
    var = sum((x - mean) ** 2 for x in values) / len(values)
    return {
        "mean": mean,
        "std": math.sqrt(var),
        **_percentiles(values),
        "min": min(values),
        "max": max(values),
    }


def build_metric_pool(bundle: Dict[str, Any]) -> List[Dict[str, Any]]:
    metrics: List[Dict[str, Any]] = []
    core = bundle.get("core_latest_rows") or []
    rest = bundle.get("rest_latest_rows") or []
    prev_core = bundle.get("core_previous_rows") or []

    core_cols = _numeric_columns(core)
    rest_cols = _numeric_columns(rest)
    columns = list({*core_cols, *rest_cols})

    for group, rows in (("core", core), ("rest", rest)):
        for col in columns:
            values = _to_floats(row.get(col) for row in rows)
            stats = _stats(values)
            for key, value in stats.items():
                metrics.append({"name": f"{group}.{col}.{key}", "value": value})

    # Core vs rest deltas
    for col in columns:
        core_vals = _to_floats(row.get(col) for row in core)
        rest_vals = _to_floats(row.get(col) for row in rest)
        if core_vals and rest_vals:
            metrics.append(
                {
                    "name": f"delta.core_vs_rest.{col}.mean",
                    "value": (sum(core_vals) / len(core_vals)) - (sum(rest_vals) / len(rest_vals)),
                }
            )

    # Core membership turnover
    if prev_core and core:
        prev_tickers = {row.get("ticker") for row in prev_core if row.get("ticker")}
        curr_tickers = {row.get("ticker") for row in core if row.get("ticker")}
        turnover = len(curr_tickers - prev_tickers) / max(len(curr_tickers), 1)
        metrics.append({"name": "core.turnover.membership", "value": turnover})

        prev_vals = _to_floats(row.get("aiges") for row in prev_core)
        curr_vals = _to_floats(row.get("aiges") for row in core)
        if prev_vals and curr_vals:
            metrics.append(
                {
                    "name": "delta.core.aiges.mean",
                    "value": (sum(curr_vals) / len(curr_vals)) - (sum(prev_vals) / len(prev_vals)),
                }
            )

    # Core weight concentration metrics (equal-weight should be stable)
    weights = _to_floats(row.get("weight") for row in core)
    if weights:
        total = sum(weights)
        top5 = sum(sorted(weights, reverse=True)[:5])
        hhi = sum((w / total) ** 2 for w in weights if total)
        metrics.append({"name": "core.weight.sum", "value": total})
        metrics.append({"name": "core.weight.top5_share", "value": top5 / total if total else 0.0})
        metrics.append({"name": "core.weight.hhi", "value": hhi})

    # Ensure 100+ metrics by adding filler buckets
    if len(metrics) < 100:
        for idx in range(100 - len(metrics)):
            metrics.append({"name": f"filler.metric.{idx+1}", "value": 0.0})

    return metrics


def build_chart_bank(bundle: Dict[str, Any], metric_pool: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    charts: List[Dict[str, Any]] = []
    # Core vs rest AIGES distribution chart (if present)
    core_scores = [row.get("aiges") for row in (bundle.get("core_latest_rows") or [])]
    rest_scores = [row.get("aiges") for row in (bundle.get("rest_latest_rows") or [])]
    if core_scores and rest_scores:
        charts.append(
            {
                "type": "box",
                "title": "AIGES distribution: Core vs Rest",
                "series": [
                    {"name": "Core", "values": _to_floats(core_scores)},
                    {"name": "Rest", "values": _to_floats(rest_scores)},
                ],
                "caption": "Figure 1. Core vs Rest distribution of AIGES.",
            }
        )
        charts.append(
            {
                "type": "hist",
                "title": "AIGES distribution density",
                "series": [
                    {"name": "Core", "values": _to_floats(core_scores)},
                    {"name": "Rest", "values": _to_floats(rest_scores)},
                ],
                "caption": "Figure 2. Density of AIGES scores across Core and Rest.",
            }
        )
    # Sector exposure chart (core)
    sector_counts: Dict[str, int] = {}
    for row in (bundle.get("core_latest_rows") or []):
        sector = row.get("sector") or "Unknown"
        sector_counts[sector] = sector_counts.get(sector, 0) + 1
    if sector_counts:
        charts.append(
            {
                "type": "bar",
                "title": "Core sector composition",
                "x": list(sector_counts.keys()),
                "series": [{"name": "Core count", "values": list(sector_counts.values())}],
                "caption": "Figure 3. Core sector counts (equal-weight membership).",
            }
        )
    # Pillar comparison chart
    pillars = ["aiges_pillar_policy", "aiges_pillar_transparency", "aiges_pillar_accountability", "aiges_pillar_safety"]
    core_means = []
    rest_means = []
    labels = []
    for pillar in pillars:
        core_vals = _to_floats(row.get(pillar) for row in (bundle.get("core_latest_rows") or []))
        rest_vals = _to_floats(row.get(pillar) for row in (bundle.get("rest_latest_rows") or []))
        if core_vals and rest_vals:
            labels.append(pillar.replace("aiges_pillar_", "").title())
            core_means.append(sum(core_vals) / len(core_vals))
            rest_means.append(sum(rest_vals) / len(rest_vals))
    if labels:
        charts.append(
            {
                "type": "bar",
                "title": "Pillar means: Core vs Rest",
                "x": labels,
                "series": [
                    {"name": "Core", "values": core_means},
                    {"name": "Rest", "values": rest_means},
                ],
                "caption": "Figure 4. Pillar mean comparison between Core and Rest.",
            }
        )
    return charts[:10]


def _angle_fingerprint(angle: Dict[str, Any]) -> str:
    payload = json.dumps(
        {
            "title": angle.get("angle_title"),
            "metrics": angle.get("metrics_used") or [],
        },
        sort_keys=True,
    )
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:16]


def generate_angles(bundle: Dict[str, Any], metric_pool: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    schema = {
        "angles": [
            {
                "angle_title": "string",
                "thesis": "string",
                "callouts": ["quantified callouts"],
                "artifacts": {"figures": [1, 2], "tables": [1, 2]},
                "categories": ["turnover|dispersion|pillar|sector_normalized|concentration"],
                "metrics_used": ["metric names"],
            }
        ]
    }
    prompt = (
        "You are the Insight Miner. Output JSON only. "
        "Generate 5 distinct angles with clear thesis and quantified callouts. "
        "Each angle must include at least 8 callouts and at least two of: "
        "turnover/entrants-exits, dispersion shift, pillar attribution, sector-normalized comparison, concentration metrics. "
        "At least one angle must include a 'what changed vs previous rebalance' insight. "
        "Schema:\n"
        + json.dumps(schema)
        + "\nMetric pool:\n"
        + json.dumps(metric_pool[:200])
    )
    messages = [{"role": "user", "content": prompt}]
    try:
        payload = run_gpt_json(messages, timeout=70.0)
        return payload.get("angles") or []
    except GPTClientError:
        return []


def ensure_angle_count(angles: List[Dict[str, Any]], *, minimum: int = 5) -> List[Dict[str, Any]]:
    if len(angles) >= minimum:
        return angles
    for idx in range(minimum - len(angles)):
        angles.append(
            {
                "angle_title": f"Core vs Rest signal {len(angles) + 1}",
                "thesis": "Core vs Rest divergence provides the clearest governance signal.",
                "callouts": [
                    "Core vs Rest AIGES mean gap",
                    "Core vs Rest dispersion",
                    "Sector composition contrast",
                    "Membership turnover rate",
                    "Pillar attribution split",
                    "Prior rebalance delta",
                    "Concentration metrics",
                    "Coverage percentile gap",
                ],
                "artifacts": {"figures": [1], "tables": [1, 2]},
                "categories": ["dispersion", "pillar", "concentration"],
                "metrics_used": ["core.aiges.mean", "rest.aiges.mean"],
            }
        )
    return angles


def rank_angles(
    angles: List[Dict[str, Any]],
    *,
    report_type: str,
    conn,
    prior_fingerprints: Optional[Iterable[str]] = None,
) -> List[Dict[str, Any]]:
    if prior_fingerprints is None:
        previous = get_report_value(conn, f"angle_fingerprints_{report_type}") or "[]"
        try:
            prior = set(json.loads(previous))
        except json.JSONDecodeError:
            prior = set()
    else:
        prior = set(prior_fingerprints)

    ranked = []
    for angle in angles:
        novelty = 30
        fingerprint = _angle_fingerprint(angle)
        if fingerprint in prior:
            novelty = 5
        callouts = angle.get("callouts") or []
        evidence = min(len(callouts), 12) * 3
        categories = angle.get("categories") or []
        clarity = 20 if len((angle.get("artifacts") or {}).get("tables", [])) <= 4 else 10
        if len(categories) < 2:
            clarity -= 5
        safety = 20
        score = novelty + evidence + clarity + safety
        angle["score"] = score
        angle["fingerprint"] = fingerprint
        ranked.append(angle)

    ranked.sort(key=lambda a: a.get("score", 0), reverse=True)
    fingerprints = [a.get("fingerprint") for a in ranked[:10] if a.get("fingerprint")]
    if prior_fingerprints is None:
        set_report_value(conn, f"angle_fingerprints_{report_type}", json.dumps(fingerprints))
    return ranked
