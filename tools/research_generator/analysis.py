"""Analysis bundle generation for research reports."""
from __future__ import annotations

import csv
import datetime as dt
import math
from dataclasses import dataclass
from statistics import mean, median
from typing import Any, Dict, Iterable, List, Optional, Tuple

from . import config
from .oracle import (
    fetch_contributions,
    fetch_levels_window,
    fetch_rebalance_rows,
    fetch_stats_latest,
    fetch_stats_window,
)


@dataclass
class AnalysisBundle:
    report_type: str
    window_start: str
    window_end: str
    key_numbers: Dict[str, Any]
    top_lists: Dict[str, Any]
    table_rows: List[Dict[str, Any]]
    chart_data: Dict[str, Any]
    chart_caption_draft: str
    table_caption_draft: str
    methodology_url: str
    safe_source_snippets: List[str]
    constraints: Dict[str, Any]
    metrics: Dict[str, Any]
    docx_tables: List[Dict[str, Any]]
    docx_charts: List[Dict[str, Any]]
    csv_extracts: Dict[str, str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "report_type": self.report_type,
            "window": {"start": self.window_start, "end": self.window_end},
            "key_numbers": self.key_numbers,
            "top_lists": self.top_lists,
            "table_rows": self.table_rows,
            "chart_data": self.chart_data,
            "chart_caption_draft": self.chart_caption_draft,
            "table_caption_draft": self.table_caption_draft,
            "methodology_url": self.methodology_url,
            "safe_source_snippets": self.safe_source_snippets,
            "constraints": self.constraints,
            "metrics": self.metrics,
            "docx_tables": self.docx_tables,
            "docx_charts": self.docx_charts,
            "csv_extracts": self.csv_extracts,
        }


def _fmt_date(value: Optional[dt.date]) -> str:
    if not value:
        return ""
    return value.strftime("%Y-%m-%d")


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _weight_fraction(value: Any) -> float:
    raw = _safe_float(value)
    if abs(raw) > 1.5:
        return raw / 100.0
    return raw


def _percentile(values: List[float], pct: float) -> Optional[float]:
    if not values:
        return None
    if pct <= 0:
        return min(values)
    if pct >= 100:
        return max(values)
    ordered = sorted(values)
    k = (len(ordered) - 1) * (pct / 100)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return ordered[int(k)]
    return ordered[f] + (ordered[c] - ordered[f]) * (k - f)


def _iqr(values: List[float]) -> Optional[float]:
    if not values:
        return None
    p75 = _percentile(values, 75)
    p25 = _percentile(values, 25)
    if p75 is None or p25 is None:
        return None
    return p75 - p25


def _stats_summary(values: List[float]) -> Dict[str, Optional[float]]:
    if not values:
        return {"mean": None, "median": None, "iqr": None}
    return {
        "mean": mean(values),
        "median": median(values),
        "iqr": _iqr(values),
    }


def _std_dev(values: List[float]) -> Optional[float]:
    if not values:
        return None
    avg = mean(values)
    variance = sum((v - avg) ** 2 for v in values) / len(values)
    return math.sqrt(variance)


def _split_core_coverage(rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    core = [row for row in rows if _weight_fraction(row.get("weight")) > config.CORE_WEIGHT_THRESHOLD]
    return core, rows


def _compute_sector_exposure(rows: List[Dict[str, Any]], *, weight_key: str) -> Dict[str, float]:
    exposure: Dict[str, float] = {}
    for row in rows:
        sector = row.get("sector") or "Unknown"
        weight = _weight_fraction(row.get(weight_key))
        exposure[sector] = exposure.get(sector, 0.0) + weight
    return exposure


def _normalize_exposure(exposure: Dict[str, float]) -> Dict[str, float]:
    total = sum(exposure.values())
    if total == 0:
        return {k: 0.0 for k in exposure}
    return {k: v / total for k, v in exposure.items()}


def _count_exposure(rows: List[Dict[str, Any]]) -> Dict[str, float]:
    counts: Dict[str, int] = {}
    for row in rows:
        sector = row.get("sector") or "Unknown"
        counts[sector] = counts.get(sector, 0) + 1
    total = sum(counts.values())
    if total == 0:
        return {k: 0.0 for k in counts}
    return {k: (v / total) * 100 for k, v in counts.items()}


def _build_sector_delta(latest: Dict[str, float], previous: Dict[str, float]) -> Dict[str, float]:
    sectors = set(latest) | set(previous)
    return {sector: latest.get(sector, 0.0) - previous.get(sector, 0.0) for sector in sectors}


def _check_equal_weight_delta(delta: float, n_core: int, tolerance: float = 0.6) -> bool:
    if n_core <= 0:
        return False
    step = 100.0 / n_core
    if step == 0:
        return False
    ratio = delta / step
    return abs(ratio - round(ratio)) <= (tolerance / step)


def _bucket_aiges(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    buckets = {"80_plus": 0, "60_79": 0, "below_60": 0}
    for row in rows:
        score = row.get("aiges")
        if score is None:
            continue
        try:
            score_val = float(score)
        except (TypeError, ValueError):
            continue
        if score_val >= 80:
            buckets["80_plus"] += 1
        elif score_val >= 60:
            buckets["60_79"] += 1
        else:
            buckets["below_60"] += 1
    return buckets


def _weighted_mean(values: List[float], weights: List[float]) -> Optional[float]:
    if not values or not weights or len(values) != len(weights):
        return None
    total = sum(weights)
    if total == 0:
        return None
    return sum(v * w for v, w in zip(values, weights)) / total


def _pillar_stats(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Optional[float]]]:
    stats: Dict[str, Dict[str, Optional[float]]] = {}
    for pillar in config.PILLAR_COLUMNS:
        values = [
            _safe_float(row.get(pillar))
            for row in rows
            if row.get(pillar) is not None and row.get(pillar) != ""
        ]
        if not values:
            continue
        stats[pillar] = {
            "mean": round(mean(values), 2),
            "median": round(median(values), 2),
        }
    return stats


def _rank_core_by_aiges(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    scored = [row for row in rows if row.get("aiges") is not None]
    scored.sort(key=lambda r: _safe_float(r.get("aiges")), reverse=True)
    return {row.get("ticker") or f"row_{idx}": idx + 1 for idx, row in enumerate(scored)}


def _compute_breadth(current: List[Dict[str, Any]], previous: List[Dict[str, Any]]) -> Optional[float]:
    prev_scores = {row.get("ticker"): row.get("aiges") for row in previous if row.get("ticker")}
    deltas = []
    for row in current:
        ticker = row.get("ticker")
        if not ticker or ticker not in prev_scores:
            continue
        prev_score = prev_scores.get(ticker)
        if prev_score is None or row.get("aiges") is None:
            continue
        deltas.append(_safe_float(row.get("aiges")) - _safe_float(prev_score))
    if not deltas:
        return None
    positives = sum(1 for d in deltas if d > 0)
    return round((positives / len(deltas)) * 100, 2)


def _compute_turnover(current: List[Dict[str, Any]], previous: List[Dict[str, Any]]) -> Optional[float]:
    current_weights = {row.get("ticker"): _weight_fraction(row.get("weight")) for row in current if row.get("ticker")}
    prev_weights = {row.get("ticker"): _weight_fraction(row.get("weight")) for row in previous if row.get("ticker")}
    current_total = sum(current_weights.values())
    prev_total = sum(prev_weights.values())
    if current_total == 0 or prev_total == 0:
        return None
    turnover = 0.0
    tickers = set(current_weights) | set(prev_weights)
    for ticker in tickers:
        w_new = current_weights.get(ticker, 0.0) / current_total
        w_old = prev_weights.get(ticker, 0.0) / prev_total
        turnover += abs(w_new - w_old)
    return round(0.5 * turnover, 4)


def _membership_turnover(prev_core: List[Dict[str, Any]], latest_core: List[Dict[str, Any]]) -> float:
    prev_tickers = {row.get("ticker") for row in prev_core if row.get("ticker")}
    latest_tickers = {row.get("ticker") for row in latest_core if row.get("ticker")}
    if not prev_tickers:
        return 0.0
    entrants = len(latest_tickers - prev_tickers)
    exits = len(prev_tickers - latest_tickers)
    return round(((entrants + exits) / max(len(prev_tickers), 1)) * 100, 2)


def _top_quintile_threshold(values: List[float]) -> Optional[float]:
    return _percentile(values, 80)


def _sector_turnover(prev_core: List[Dict[str, Any]], latest_core: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    prev_tickers = {row.get("ticker") for row in prev_core if row.get("ticker")}
    latest_tickers = {row.get("ticker") for row in latest_core if row.get("ticker")}
    entrants = latest_tickers - prev_tickers
    exits = prev_tickers - latest_tickers

    def _sector_counts(rows: List[Dict[str, Any]], tickers: set[str]) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for row in rows:
            ticker = row.get("ticker")
            if ticker not in tickers:
                continue
            sector = row.get("sector") or "Unknown"
            counts[sector] = counts.get(sector, 0) + 1
        return counts

    entrant_counts = _sector_counts(latest_core, entrants)
    exit_counts = _sector_counts(prev_core, exits)
    sectors = sorted(set(entrant_counts) | set(exit_counts))
    rows = []
    for sector in sectors:
        rows.append(
            {
                "Sector": sector,
                "Entrants": entrant_counts.get(sector, 0),
                "Exits": exit_counts.get(sector, 0),
                "Net Change": entrant_counts.get(sector, 0) - exit_counts.get(sector, 0),
            }
        )
    return rows


def _rows_to_csv(rows: List[Dict[str, Any]]) -> str:
    if not rows:
        return ""
    columns = list(rows[0].keys())
    import io

    stream = io.StringIO()
    writer = csv.writer(stream)
    writer.writerow(columns)
    for row in rows:
        writer.writerow([str(row.get(col, "")).replace("\n", " ") for col in columns])
    return stream.getvalue().strip()


def _fmt_pct(value: Optional[float], digits: int = 1) -> str:
    if value is None:
        return ""
    return f"{value:.{digits}f}%"


def _fmt_delta_pct(value: Optional[float], digits: int = 1) -> str:
    if value is None:
        return ""
    sign = "+" if value >= 0 else ""
    return f"{sign}{value:.{digits}f}%"


def _fmt_score(value: Optional[float], digits: int = 2) -> str:
    if value is None:
        return ""
    return f"{value:.{digits}f}"


def _fmt_delta_pp(value: Optional[float], digits: int = 1) -> str:
    if value is None:
        return ""
    sign = "+" if value >= 0 else ""
    return f"{sign}{value:.{digits}f}pp"


def _truncate_text(text: str, limit: int = 160) -> str:
    payload = (text or "").strip()
    if len(payload) <= limit:
        return payload
    trimmed = payload[: limit + 1].rsplit(" ", 1)[0]
    if not trimmed:
        trimmed = payload[:limit]
    return trimmed.rstrip() + "..."


def _build_movers(
    latest: List[Dict[str, Any]],
    previous: List[Dict[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    prev_by_ticker = {row.get("ticker"): row for row in previous if row.get("ticker")}
    latest_by_ticker = {row.get("ticker"): row for row in latest if row.get("ticker")}
    all_tickers = set(prev_by_ticker) | set(latest_by_ticker)

    entrants: List[Dict[str, Any]] = []
    exits: List[Dict[str, Any]] = []
    incumbent_score: List[Dict[str, Any]] = []
    incumbent_weight: List[Dict[str, Any]] = []

    for ticker in all_tickers:
        prev = prev_by_ticker.get(ticker)
        latest_row = latest_by_ticker.get(ticker)
        prev_weight = _weight_fraction(prev.get("weight")) if prev else 0.0
        new_weight = _weight_fraction(latest_row.get("weight") if latest_row else 0.0)
        prev_score = _safe_float(prev.get("aiges")) if prev else None
        new_score = _safe_float(latest_row.get("aiges")) if latest_row else None
        row = latest_row or prev or {}
        summary = _truncate_text(row.get("summary") or "")

        if prev_weight <= config.CORE_WEIGHT_THRESHOLD and new_weight > config.CORE_WEIGHT_THRESHOLD:
            entrants.append(
                {
                    "ticker": ticker,
                    "company": row.get("company"),
                    "sector": row.get("sector") or "Unknown",
                    "weight_new": new_weight,
                    "aiges_new": new_score,
                    "summary": summary,
                }
            )
            continue
        if prev_weight > config.CORE_WEIGHT_THRESHOLD and new_weight <= config.CORE_WEIGHT_THRESHOLD:
            exits.append(
                {
                    "ticker": ticker,
                    "company": row.get("company"),
                    "sector": row.get("sector") or "Unknown",
                    "weight_prev": prev_weight,
                    "aiges_prev": prev_score,
                    "summary": summary,
                }
            )
            continue

        if prev_weight > config.CORE_WEIGHT_THRESHOLD and new_weight > config.CORE_WEIGHT_THRESHOLD:
            if new_score is not None and prev_score is not None:
                incumbent_score.append(
                    {
                        "ticker": ticker,
                        "company": row.get("company"),
                        "sector": row.get("sector") or "Unknown",
                        "delta_aiges": round(new_score - prev_score, 2),
                        "aiges_prev": round(prev_score, 2),
                        "aiges_new": round(new_score, 2),
                        "summary": summary,
                    }
                )
            incumbent_weight.append(
                {
                    "ticker": ticker,
                    "company": row.get("company"),
                    "sector": row.get("sector") or "Unknown",
                    "delta_weight": round(new_weight - prev_weight, 6),
                    "weight_prev": round(prev_weight, 6),
                    "weight_new": round(new_weight, 6),
                    "summary": summary,
                }
            )

    entrants.sort(key=lambda r: r.get("weight_new") or 0.0, reverse=True)
    exits.sort(key=lambda r: r.get("weight_prev") or 0.0, reverse=True)
    incumbent_score.sort(key=lambda r: abs(_safe_float(r.get("delta_aiges"))), reverse=True)
    incumbent_weight.sort(key=lambda r: abs(_safe_float(r.get("delta_weight"))), reverse=True)
    return {
        "entrants": entrants[:10],
        "exits": exits[:10],
        "incumbent_score": incumbent_score[:10],
        "incumbent_weight": incumbent_weight[:10],
    }


def build_rebalance_bundle(
    latest_date: dt.date,
    previous_date: Optional[dt.date],
    latest_rows: List[Dict[str, Any]],
    previous_rows: List[Dict[str, Any]],
) -> AnalysisBundle:
    latest_core, latest_cov = _split_core_coverage(latest_rows)
    prev_core, prev_cov = _split_core_coverage(previous_rows)

    tickers_latest = {row.get("ticker") for row in latest_core if row.get("ticker")}
    tickers_prev = {row.get("ticker") for row in prev_core if row.get("ticker")}
    entrants = sorted(tickers_latest - tickers_prev)
    exits = sorted(tickers_prev - tickers_latest)

    weighted_sector_latest = _normalize_exposure(_compute_sector_exposure(latest_core, weight_key="weight"))
    weighted_sector_prev = _normalize_exposure(_compute_sector_exposure(prev_core, weight_key="weight"))
    weighted_sector_delta = _build_sector_delta(weighted_sector_latest, weighted_sector_prev)

    coverage_count_latest = _count_exposure(latest_cov)
    coverage_count_prev = _count_exposure(prev_cov)
    coverage_count_delta = _build_sector_delta(coverage_count_latest, coverage_count_prev)

    core_count_latest = _count_exposure(latest_core)
    core_count_prev = _count_exposure(prev_core)
    core_count_delta = _build_sector_delta(core_count_latest, core_count_prev)

    inconsistent_sectors = []
    for sector, delta in core_count_delta.items():
        if not _check_equal_weight_delta(delta, max(len(latest_core), 1)):
            inconsistent_sectors.append(sector)

    scores_core = [row.get("aiges") for row in latest_core if row.get("aiges") is not None]
    scores_cov = [row.get("aiges") for row in latest_cov if row.get("aiges") is not None]
    scores_core_float = [_safe_float(x) for x in scores_core]
    scores_cov_float = [_safe_float(x) for x in scores_cov]

    weights_core = [_weight_fraction(row.get("weight")) for row in latest_core]
    weighted_mean_core = _weighted_mean(scores_core_float, weights_core)
    stats_core = _stats_summary(scores_core_float)
    stats_cov = _stats_summary(scores_cov_float)

    zero_weight_rows = [
        row for row in latest_cov if _weight_fraction(row.get("weight")) <= config.CORE_WEIGHT_THRESHOLD
    ]
    scores_zero_float = [
        _safe_float(row.get("aiges")) for row in zero_weight_rows if row.get("aiges") is not None
    ]
    stats_zero = _stats_summary(scores_zero_float)

    if weighted_mean_core is None or stats_cov["mean"] is None:
        raise ValueError("coverage_mean_missing")

    top5_weight_share = None
    hhi = None
    total_weight = sum(weights_core)
    if total_weight:
        sorted_weights = sorted(weights_core, reverse=True)
        top5_weight_share = round(sum(sorted_weights[:5]) / total_weight, 4)
        hhi = round(sum((w / total_weight) ** 2 for w in weights_core), 4)

    breadth_pct = _compute_breadth(latest_core, prev_core)
    turnover = _compute_turnover(latest_core, prev_core)
    membership_turnover = _membership_turnover(prev_core, latest_core)
    std_core = _std_dev(scores_core_float)
    std_cov = _std_dev(scores_cov_float)
    top_quintile = _top_quintile_threshold(scores_cov_float)
    core_in_top_quintile = None
    if top_quintile is not None and scores_core_float:
        core_in_top_quintile = round(
            (sum(1 for v in scores_core_float if v >= top_quintile) / len(scores_core_float)) * 100,
            2,
        )

    movers = _build_movers(latest_core, prev_core)

    avg_aiges = weighted_mean_core
    mean_gap = round(weighted_mean_core - stats_cov["mean"], 2) if stats_cov["mean"] is not None else None
    zero_gap = (
        round(weighted_mean_core - stats_zero["mean"], 2)
        if stats_zero["mean"] is not None
        else None
    )
    core_max_sector = max(weighted_sector_latest.values()) * 100 if weighted_sector_latest else 0.0
    coverage_max_sector = max(coverage_count_latest.values()) if coverage_count_latest else 0.0
    sector_concentration_gap = round(core_max_sector - coverage_max_sector, 2)
    key_numbers = {
        "core_constituents": len(latest_core),
        "coverage_constituents": len(latest_cov),
        "avg_aiges_weighted": round(avg_aiges, 2) if avg_aiges is not None else None,
        "entrants": len(entrants),
        "exits": len(exits),
        "turnover": turnover,
        "membership_turnover": membership_turnover,
    }

    table_rows = []
    for row in latest_core[: config.MAX_TABLE_ROWS]:
        table_rows.append(
            {
                "Company": row.get("company"),
                "Ticker": row.get("ticker"),
                "Sector": row.get("sector"),
                "Weight %": round(_weight_fraction(row.get("weight")) * 100, 2),
                "AIGES": round(_safe_float(row.get("aiges")), 2),
            }
        )

    safe_snippets = []
    for row in latest_core:
        summary = row.get("summary")
        if summary:
            safe_snippets.append(f"public source summary field: {summary.strip()}")
        if len(safe_snippets) >= 2:
            break

    summary_table = [
        {
            "Segment": "Core (Top 25)",
            "N": len(latest_core),
            "Mean Composite": round(weighted_mean_core, 2),
            "Median Composite": round(stats_core["median"], 2) if stats_core["median"] is not None else None,
            "IQR Composite": round(stats_core["iqr"], 2) if stats_core["iqr"] is not None else None,
            "Std Dev": round(std_core, 2) if std_core is not None else None,
            "Top5 Weight Share": round((top5_weight_share or 0) * 100, 2) if top5_weight_share is not None else None,
            "HHI": hhi,
        },
        {
            "Segment": "Coverage (All 100)",
            "N": len(latest_cov),
            "Mean Composite": round(stats_cov["mean"], 2) if stats_cov["mean"] is not None else None,
            "Median Composite": round(stats_cov["median"], 2) if stats_cov["median"] is not None else None,
            "IQR Composite": round(stats_cov["iqr"], 2) if stats_cov["iqr"] is not None else None,
            "Std Dev": round(std_cov, 2) if std_cov is not None else None,
            "Top5 Weight Share": None,
            "HHI": None,
        },
        {
            "Segment": "Zero-Weight Slice (75)",
            "N": len(zero_weight_rows),
            "Mean Composite": round(stats_zero["mean"], 2) if stats_zero["mean"] is not None else None,
            "Median Composite": round(stats_zero["median"], 2) if stats_zero["median"] is not None else None,
            "IQR Composite": round(stats_zero["iqr"], 2) if stats_zero["iqr"] is not None else None,
            "Top5 Weight Share": None,
            "HHI": None,
        },
    ]

    sector_rows = []
    sectors = sorted(set(weighted_sector_latest) | set(weighted_sector_prev) | set(coverage_count_latest))
    for sector in sectors:
        count_delta = coverage_count_delta.get(sector, 0.0)
        count_delta_display = round(count_delta, 2)
        if sector in inconsistent_sectors:
            count_delta_display = "Not comparable"
        sector_rows.append(
            {
                "Sector": sector,
                "Core Weighted Prev": round(weighted_sector_prev.get(sector, 0.0) * 100, 2),
                "Core Weighted New": round(weighted_sector_latest.get(sector, 0.0) * 100, 2),
                "Core Weighted Delta": round(weighted_sector_delta.get(sector, 0.0) * 100, 2),
                "Coverage Count Prev %": round(coverage_count_prev.get(sector, 0.0), 2),
                "Coverage Count New %": round(coverage_count_latest.get(sector, 0.0), 2),
                "Coverage Count Delta": count_delta_display,
            }
        )

    sector_move_candidates = []
    divergence_candidates = []
    for sector in sectors:
        delta_weighted = weighted_sector_delta.get(sector, 0.0) * 100
        if abs(delta_weighted) > 0:
            sector_move_candidates.append((sector, delta_weighted))
        divergence = abs(delta_weighted - coverage_count_delta.get(sector, 0.0))
        divergence_candidates.append((sector, divergence, delta_weighted))
    sector_move_candidates.sort(key=lambda item: abs(item[1]), reverse=True)
    divergence_candidates.sort(key=lambda item: item[1], reverse=True)
    notable_sector_moves = sector_move_candidates[:3]
    notable_sector_divergence = divergence_candidates[:3]

    entrants_rows = [
        {
            "Company": row.get("company"),
            "Ticker": row.get("ticker"),
            "Sector": row.get("sector"),
            "New Weight %": round((_safe_float(row.get("weight_new")) * 100), 2),
            "Composite Score": row.get("aiges_new"),
            "Support Short": row.get("summary") or "",
        }
        for row in movers["entrants"]
    ]
    exits_rows = [
        {
            "Company": row.get("company"),
            "Ticker": row.get("ticker"),
            "Sector": row.get("sector"),
            "Prev Weight %": round((_safe_float(row.get("weight_prev")) * 100), 2),
            "Composite Score": row.get("aiges_prev"),
            "Support Short": row.get("summary") or "",
        }
        for row in movers["exits"]
    ]
    incumbent_score_rows = [
        {
            "Company": row.get("company"),
            "Ticker": row.get("ticker"),
            "Sector": row.get("sector"),
            "Delta Score": row.get("delta_aiges"),
            "Prev Score": row.get("aiges_prev"),
            "New Score": row.get("aiges_new"),
            "Support Short": row.get("summary") or "",
        }
        for row in movers["incumbent_score"]
    ]
    incumbent_weight_rows = [
        {
            "Company": row.get("company"),
            "Ticker": row.get("ticker"),
            "Sector": row.get("sector"),
            "Delta Weight (pp)": round((_safe_float(row.get("delta_weight")) * 100), 2),
            "Prev Weight %": round((_safe_float(row.get("weight_prev")) * 100), 2),
            "New Weight %": round((_safe_float(row.get("weight_new")) * 100), 2),
            "Support Short": row.get("summary") or "",
        }
        for row in movers["incumbent_weight"]
    ]

    core_mean = round(weighted_mean_core, 2)
    cov_mean = round(stats_cov["mean"], 2) if stats_cov["mean"] is not None else None
    zero_mean = round(stats_zero["mean"], 2) if stats_zero["mean"] is not None else None

    diagnostics_rows = [
        {"Metric": "Membership turnover", "Value": membership_turnover},
        {"Metric": "Breadth (incumbents improving)", "Value": breadth_pct},
        {"Metric": "Core top-quintile share", "Value": core_in_top_quintile},
        {"Metric": "Core-weighted turnover", "Value": (turnover or 0) * 100},
    ]
    diagnostics_formats = {"Value": "pct"}
    diagnostics_widths = {"Metric": 2.2, "Value": 1.0}

    summary_callouts = [
        f"Core weighted mean composite is {_fmt_score(core_mean)} vs coverage mean {_fmt_score(cov_mean)} (gap {mean_gap:+.2f}).",
        f"Core IQR {_fmt_score(stats_core['iqr'])} vs coverage IQR {_fmt_score(stats_cov['iqr'])}.",
        f"Top5 weight share {_fmt_pct((top5_weight_share or 0) * 100)} and HHI {_fmt_score(hhi)} indicate concentration.",
        f"Breadth is {_fmt_pct(breadth_pct)} of incumbents improving; membership turnover is {_fmt_pct(membership_turnover)}.",
    ]
    if zero_mean is not None:
        summary_callouts.append(
            f"Zero-weight slice mean {_fmt_score(zero_mean)} vs core mean {_fmt_score(core_mean)} (gap {zero_gap:+.2f})."
        )

    sector_callouts = []
    for sector, delta in notable_sector_moves:
        prev_val = weighted_sector_prev.get(sector, 0.0) * 100
        new_val = weighted_sector_latest.get(sector, 0.0) * 100
        sector_callouts.append(
            f"{sector} moved from {_fmt_pct(prev_val)} to {_fmt_pct(new_val)} ({_fmt_delta_pct(delta)})."
        )
    for sector, divergence, delta_weighted in notable_sector_divergence:
        coverage_delta = coverage_count_delta.get(sector, 0.0)
        sector_callouts.append(
            f"{sector} divergence: core {_fmt_delta_pct(delta_weighted)} vs coverage {_fmt_delta_pct(coverage_delta)}."
        )

    entrants_callouts = []
    for row in movers["entrants"][:3]:
        entrants_callouts.append(
            f"Entrant {row.get('ticker')} ({row.get('sector')}): {_fmt_pct(_weight_fraction(row.get('weight_new')) * 100)} weight, score {_fmt_score(row.get('aiges_new'))}."
        )
    exits_callouts = []
    for row in movers["exits"][:3]:
        exits_callouts.append(
            f"Exit {row.get('ticker')} ({row.get('sector')}): prev {_fmt_pct(_weight_fraction(row.get('weight_prev')) * 100)} weight, score {_fmt_score(row.get('aiges_prev'))}."
        )
    inc_score_callouts = []
    for row in movers["incumbent_score"][:3]:
        delta_score = row.get("delta_aiges")
        delta_score_str = f"{delta_score:+.2f}" if isinstance(delta_score, (int, float)) else ""
        inc_score_callouts.append(
            f"Score mover {row.get('ticker')} ({row.get('sector')}): {delta_score_str} vs prior."
        )

    chart_sector = {
        "type": "bar",
        "title": "Core sector exposure (weighted)",
        "x": sectors,
        "series": [
            {"name": "Prev", "values": [weighted_sector_prev.get(s, 0.0) * 100 for s in sectors]},
            {"name": "New", "values": [weighted_sector_latest.get(s, 0.0) * 100 for s in sectors]},
        ],
        "y_label": "Percent",
        "caption": "Core weighted sector exposure before vs after rebalance.",
    }

    chart_aiges = {
        "type": "box",
        "title": "AIGES composite distribution",
        "series": [
            {"name": "Core", "values": scores_core_float},
            {"name": "Coverage", "values": scores_cov_float},
        ],
        "caption": "Distribution of AIGES composite scores (core vs coverage).",
    }

    chart_breadth = {
        "type": "bar",
        "title": "Breadth and turnover",
        "x": ["Breadth %", "Turnover"],
        "series": [
            {
                "name": "Current",
                "values": [breadth_pct or 0.0, turnover or 0.0],
            }
        ],
        "y_label": "Percent / Ratio",
        "caption": "Breadth of positive score changes and portfolio turnover.",
    }

    metrics = {
        "core": {
            "n": len(latest_core),
            "mean_weighted_aiges": round(weighted_mean_core, 2) if weighted_mean_core else None,
            "median_aiges": round(median(scores_core_float), 2) if scores_core_float else None,
            "iqr_aiges": round(_iqr(scores_core_float), 2) if scores_core_float else None,
            "std_aiges": round(std_core, 2) if std_core is not None else None,
            "top5_weight_share": top5_weight_share,
            "hhi": hhi,
            "breadth_pct": breadth_pct,
            "membership_turnover_pct": membership_turnover,
            "top_quintile_share_pct": core_in_top_quintile,
            "weights_raw": [row.get("weight") for row in latest_core],
            "weights_sum": round(sum(weights_core), 6),
        },
        "coverage": {
            "n": len(latest_cov),
            "mean_aiges": round(stats_cov["mean"], 2) if stats_cov["mean"] is not None else None,
            "median_aiges": round(stats_cov["median"], 2) if stats_cov["median"] is not None else None,
            "iqr_aiges": round(stats_cov["iqr"], 2) if stats_cov["iqr"] is not None else None,
            "std_aiges": round(std_cov, 2) if std_cov is not None else None,
        },
        "zero_weight_slice": {
            "n": len(zero_weight_rows),
            "mean_aiges": round(stats_zero["mean"], 2) if stats_zero["mean"] is not None else None,
            "median_aiges": round(stats_zero["median"], 2) if stats_zero["median"] is not None else None,
            "iqr_aiges": round(stats_zero["iqr"], 2) if stats_zero["iqr"] is not None else None,
        },
        "gaps": {
            "mean_gap_core_vs_coverage": mean_gap,
            "mean_gap_core_vs_zero": zero_gap,
            "sector_concentration_gap": sector_concentration_gap,
        },
        "pillars": _pillar_stats(latest_cov),
        "rebalance": {
            "turnover": turnover,
            "entrants": entrants[:10],
            "exits": exits[:10],
            "membership_turnover_pct": membership_turnover,
        },
        "sector_exposure": {
            "core_weighted_latest": weighted_sector_latest,
            "core_weighted_prev": weighted_sector_prev,
            "coverage_count_latest": coverage_count_latest,
            "coverage_count_prev": coverage_count_prev,
            "core_count_delta_flags": inconsistent_sectors,
            "notable_moves": notable_sector_moves,
            "notable_divergence": notable_sector_divergence,
            "sector_turnover": _sector_turnover(prev_core, latest_core),
        },
        "top_movers": {
            "entrants": movers["entrants"],
            "exits": movers["exits"],
            "incumbent_score": movers["incumbent_score"],
            "incumbent_weight": movers["incumbent_weight"],
        },
        "core_rank_by_aiges": _rank_core_by_aiges(latest_core),
    }

    csv_extracts = {
        "sector_exposure": _rows_to_csv(sector_rows),
        "core_entrants": _rows_to_csv(entrants_rows),
        "core_exits": _rows_to_csv(exits_rows),
        "incumbent_score_movers": _rows_to_csv(incumbent_score_rows),
        "incumbent_weight_movers": _rows_to_csv(incumbent_weight_rows),
        "summary": _rows_to_csv(summary_table),
    }

    summary_formats = {
        "Mean Composite": "score",
        "Median Composite": "score",
        "IQR Composite": "score",
        "Std Dev": "score",
        "Top5 Weight Share": "pct",
        "HHI": "ratio",
    }
    summary_widths = {
        "Segment": 1.7,
        "N": 0.6,
        "Mean Composite": 1.1,
        "Median Composite": 1.0,
        "IQR Composite": 0.9,
        "Std Dev": 0.9,
        "Top5 Weight Share": 0.9,
        "HHI": 0.7,
    }
    sector_formats = {
        "Core Weighted Prev": "pct",
        "Core Weighted New": "pct",
        "Core Weighted Delta": "delta_pct",
        "Coverage Count Prev %": "pct",
        "Coverage Count New %": "pct",
        "Coverage Count Delta": "delta_pct",
    }
    sector_widths = {
        "Sector": 1.6,
        "Core Weighted Prev": 1.1,
        "Core Weighted New": 1.1,
        "Core Weighted Delta": 1.1,
        "Coverage Count Prev %": 1.1,
        "Coverage Count New %": 1.1,
        "Coverage Count Delta": 1.1,
    }
    entrants_formats = {"New Weight %": "pct", "Composite Score": "score"}
    exits_formats = {"Prev Weight %": "pct", "Composite Score": "score"}
    incumbent_score_formats = {
        "Delta Score": "score_signed",
        "Prev Score": "score",
        "New Score": "score",
    }
    movers_widths = {
        "Company": 1.6,
        "Ticker": 0.7,
        "Sector": 1.0,
        "Delta Score": 0.9,
        "Prev Score": 0.8,
        "New Score": 0.8,
        "Prev Weight %": 0.9,
        "New Weight %": 0.9,
        "Composite Score": 0.9,
        "Support Short": 2.2,
    }

    bundle = AnalysisBundle(
        report_type="REBALANCE",
        window_start=_fmt_date(previous_date),
        window_end=_fmt_date(latest_date),
        key_numbers=key_numbers,
        top_lists={
            "entrants": entrants[:10],
            "exits": exits[:10],
            "governance_buckets": _bucket_aiges(latest_core),
            "top_sectors_delta": dict(sorted(weighted_sector_delta.items(), key=lambda kv: abs(kv[1]), reverse=True)[:6]),
        },
        table_rows=table_rows,
        chart_data=chart_sector,
        chart_caption_draft=chart_sector["caption"],
        table_caption_draft="Core constituents by weight with AI governance scores.",
        methodology_url=config.REPORT_METHOD_URL,
        safe_source_snippets=safe_snippets,
        constraints={
            "no_prices": True,
            "no_advice": True,
            "tone": "research/education",
        },
        metrics=metrics,
        docx_tables=[
            {
                "title": "Core vs Coverage Summary",
                "rows": summary_table,
                "formats": summary_formats,
                "column_widths": summary_widths,
                "callouts": summary_callouts,
            },
            {
                "title": "Sector Exposure Comparison",
                "rows": sector_rows,
                "formats": sector_formats,
                "column_widths": sector_widths,
                "callouts": sector_callouts,
                "highlight_rules": [
                    {"column": "Core Weighted Delta", "abs_gte": 4.0, "color": "FFF2CC"},
                    {"column": "Coverage Count Delta", "column_value": "Not comparable", "color": "F8D7DA"},
                ],
            },
            {
                "title": "Core Diagnostics (Equal-weight)",
                "rows": diagnostics_rows,
                "formats": diagnostics_formats,
                "column_widths": diagnostics_widths,
                "callouts": [
                    f"Membership turnover is {_fmt_pct(membership_turnover)} of the Core.",
                    f"Core top-quintile share is {_fmt_pct(core_in_top_quintile)}.",
                ],
            },
            {
                "title": "Core Entrants (New to core)",
                "rows": entrants_rows,
                "formats": entrants_formats,
                "column_widths": movers_widths,
                "callouts": entrants_callouts,
            },
            {
                "title": "Core Exits (Removed from core)",
                "rows": exits_rows,
                "formats": exits_formats,
                "column_widths": movers_widths,
                "callouts": exits_callouts,
            },
            {
                "title": "Incumbent Score Movers (Comparable)",
                "rows": incumbent_score_rows,
                "formats": incumbent_score_formats,
                "column_widths": movers_widths,
                "callouts": inc_score_callouts,
                "highlight_rules": [
                    {"column": "Delta Score", "abs_gte": 10.0, "color": "E2F0D9"},
                ],
            },
        ],
        docx_charts=[
            {**chart_sector, "callouts": sector_callouts[:3]},
            {**chart_aiges, "callouts": summary_callouts[:2]},
            {**chart_breadth, "callouts": summary_callouts[2:]},
        ],
        csv_extracts=csv_extracts,
    )
    return bundle


def build_core_vs_coverage_gap_bundle(
    latest_date: dt.date,
    previous_date: Optional[dt.date],
    latest_rows: List[Dict[str, Any]],
    previous_rows: List[Dict[str, Any]],
) -> AnalysisBundle:
    base = build_rebalance_bundle(latest_date, previous_date, latest_rows, previous_rows)
    metrics = base.metrics or {}
    gaps = metrics.get("gaps") or {}

    gap_rows = [
        {"Metric": "Mean gap (core - coverage)", "Value": gaps.get("mean_gap_core_vs_coverage")},
        {"Metric": "Mean gap (core - zero)", "Value": gaps.get("mean_gap_core_vs_zero")},
        {"Metric": "Sector concentration gap", "Value": gaps.get("sector_concentration_gap")},
        {"Metric": "Breadth", "Value": metrics.get("core", {}).get("breadth_pct")},
        {"Metric": "Turnover", "Value": metrics.get("rebalance", {}).get("turnover")},
    ]

    gap_formats = {"Value": "score_signed"}
    gap_widths = {"Metric": 2.5, "Value": 1.2}
    gap_callouts = [
        f"Core vs coverage mean gap is {gaps.get('mean_gap_core_vs_coverage')} points.",
        f"Core vs zero-weight gap is {gaps.get('mean_gap_core_vs_zero')} points.",
        f"Sector concentration gap is {gaps.get('sector_concentration_gap')} percentage points.",
    ]

    return AnalysisBundle(
        report_type="CORE_VS_COVERAGE_GAP",
        window_start=base.window_start,
        window_end=base.window_end,
        key_numbers=base.key_numbers,
        top_lists=base.top_lists,
        table_rows=base.table_rows,
        chart_data=base.chart_data,
        chart_caption_draft=base.chart_caption_draft,
        table_caption_draft="Core vs coverage gap metrics.",
        methodology_url=base.methodology_url,
        safe_source_snippets=base.safe_source_snippets,
        constraints=base.constraints,
        metrics=base.metrics,
        docx_tables=[
            base.docx_tables[0],
            {
                "title": "Core vs Coverage Gaps",
                "rows": gap_rows,
                "formats": gap_formats,
                "column_widths": gap_widths,
                "callouts": gap_callouts,
            },
            base.docx_tables[1],
        ],
        docx_charts=[base.docx_charts[1], base.docx_charts[0]],
        csv_extracts=base.csv_extracts,
    )


def build_top25_movers_bundle(
    latest_date: dt.date,
    previous_date: Optional[dt.date],
    latest_rows: List[Dict[str, Any]],
    previous_rows: List[Dict[str, Any]],
) -> AnalysisBundle:
    base = build_rebalance_bundle(latest_date, previous_date, latest_rows, previous_rows)
    return AnalysisBundle(
        report_type="TOP25_MOVERS_ONLY",
        window_start=base.window_start,
        window_end=base.window_end,
        key_numbers=base.key_numbers,
        top_lists=base.top_lists,
        table_rows=base.table_rows,
        chart_data=base.chart_data,
        chart_caption_draft="Top movers and sector shifts within the core index.",
        table_caption_draft="Top movers by score and weight.",
        methodology_url=base.methodology_url,
        safe_source_snippets=base.safe_source_snippets,
        constraints=base.constraints,
        metrics=base.metrics,
        docx_tables=[base.docx_tables[2], base.docx_tables[1]],
        docx_charts=[base.docx_charts[0], base.docx_charts[2]],
        csv_extracts=base.csv_extracts,
    )


def build_anomaly_bundle(stats: Dict[str, Any], contributions: List[Dict[str, Any]]) -> AnalysisBundle:
    trade_date = stats.get("trade_date")
    vol = stats.get("vol_20d") or 0
    ret_1d = stats.get("ret_1d") or 0
    z_score = None
    if vol:
        z_score = float(ret_1d) / float(vol) if vol else None

    table_rows = []
    for row in contributions[: config.MAX_TABLE_ROWS]:
        table_rows.append(
            {
                "Ticker": row.get("ticker"),
                "Contribution": round(_safe_float(row.get("contribution")), 6),
            }
        )

    metrics = {
        "anomaly": {
            "ret_1d": stats.get("ret_1d"),
            "vol_20d": stats.get("vol_20d"),
            "z_score": round(z_score, 2) if z_score is not None else None,
            "max_drawdown_252d": stats.get("max_drawdown_252d"),
        }
    }

    chart_data = {
        "type": "bar",
        "title": "Top daily contributions",
        "x": [row.get("ticker") for row in contributions],
        "series": [
            {"name": "Contribution", "values": [round(_safe_float(row.get("contribution")), 6) for row in contributions]}
        ],
        "caption": "Top contributors for the latest trading day.",
    }

    bundle = AnalysisBundle(
        report_type="ANOMALY",
        window_start=_fmt_date(trade_date),
        window_end=_fmt_date(trade_date),
        key_numbers={
            "ret_1d": stats.get("ret_1d"),
            "vol_20d": stats.get("vol_20d"),
            "z_score": round(z_score, 2) if z_score is not None else None,
            "max_drawdown_252d": stats.get("max_drawdown_252d"),
        },
        top_lists={
            "top_contributors": contributions[:8],
        },
        table_rows=table_rows,
        chart_data=chart_data,
        chart_caption_draft=chart_data["caption"],
        table_caption_draft="Largest daily contributions (no price data).",
        methodology_url=config.REPORT_METHOD_URL,
        safe_source_snippets=[],
        constraints={
            "no_prices": True,
            "no_advice": True,
            "tone": "research/education",
        },
        metrics=metrics,
        docx_tables=[{"title": "Top Contributions", "rows": table_rows}],
        docx_charts=[chart_data],
        csv_extracts={"top_contributions": _rows_to_csv(table_rows)},
    )
    return bundle


def build_weekly_bundle(stats_window: List[Dict[str, Any]], levels_window: List[Dict[str, Any]]) -> AnalysisBundle:
    stats_sorted = list(reversed(stats_window))
    levels_sorted = list(reversed(levels_window))
    window_start = stats_sorted[0]["trade_date"] if stats_sorted else None
    window_end = stats_sorted[-1]["trade_date"] if stats_sorted else None

    cumulative_return = None
    if levels_sorted:
        start_level = levels_sorted[0]["level_tr"]
        end_level = levels_sorted[-1]["level_tr"]
        if start_level and end_level:
            cumulative_return = (end_level / start_level) - 1

    table_rows = []
    for row in stats_sorted[-config.MAX_TABLE_ROWS :]:
        table_rows.append(
            {
                "Date": _fmt_date(row.get("trade_date")),
                "Ret 1D": row.get("ret_1d"),
                "Vol 20D": row.get("vol_20d"),
                "Drawdown 252D": row.get("max_drawdown_252d"),
            }
        )

    chart_dates = [item["trade_date"].strftime("%Y-%m-%d") for item in levels_sorted]
    chart_levels = [item["level_tr"] for item in levels_sorted]

    chart_data = {
        "type": "line",
        "title": "Index level (total return)",
        "x": chart_dates,
        "series": [{"name": "Level", "values": chart_levels}],
        "caption": "Index total return levels over the review window.",
    }

    metrics = {
        "weekly": {
            "weekly_return": cumulative_return,
            "latest_vol_20d": stats_sorted[-1]["vol_20d"] if stats_sorted else None,
        }
    }

    bundle = AnalysisBundle(
        report_type="WEEKLY",
        window_start=_fmt_date(window_start),
        window_end=_fmt_date(window_end),
        key_numbers={
            "weekly_return": cumulative_return,
            "latest_vol_20d": stats_sorted[-1]["vol_20d"] if stats_sorted else None,
        },
        top_lists={},
        table_rows=table_rows,
        chart_data=chart_data,
        chart_caption_draft=chart_data["caption"],
        table_caption_draft="Daily summary metrics over the review window.",
        methodology_url=config.REPORT_METHOD_URL,
        safe_source_snippets=[],
        constraints={
            "no_prices": True,
            "no_advice": True,
            "tone": "research/education",
        },
        metrics=metrics,
        docx_tables=[{"title": "Weekly Metrics", "rows": table_rows}],
        docx_charts=[chart_data],
        csv_extracts={"weekly_metrics": _rows_to_csv(table_rows)},
    )
    return bundle


def build_period_close_bundle(stats_window: List[Dict[str, Any]], levels_window: List[Dict[str, Any]], label: str) -> AnalysisBundle:
    stats_sorted = list(reversed(stats_window))
    levels_sorted = list(reversed(levels_window))
    window_start = stats_sorted[0]["trade_date"] if stats_sorted else None
    window_end = stats_sorted[-1]["trade_date"] if stats_sorted else None

    cumulative_return = None
    if levels_sorted:
        start_level = levels_sorted[0]["level_tr"]
        end_level = levels_sorted[-1]["level_tr"]
        if start_level and end_level:
            cumulative_return = (end_level / start_level) - 1

    table_rows = []
    for row in stats_sorted[-config.MAX_TABLE_ROWS :]:
        table_rows.append(
            {
                "Date": _fmt_date(row.get("trade_date")),
                "Ret 1D": row.get("ret_1d"),
                "Ret 20D": row.get("ret_20d"),
                "Vol 20D": row.get("vol_20d"),
            }
        )

    chart_dates = [item["trade_date"].strftime("%Y-%m-%d") for item in levels_sorted]
    chart_levels = [item["level_tr"] for item in levels_sorted]

    chart_data = {
        "type": "line",
        "title": "Index level (total return)",
        "x": chart_dates,
        "series": [{"name": "Level", "values": chart_levels}],
        "caption": "Index total return levels over the period.",
    }

    metrics = {
        "period": {
            "period_return": cumulative_return,
            "period_label": label,
        }
    }

    bundle = AnalysisBundle(
        report_type="PERIOD_CLOSE",
        window_start=_fmt_date(window_start),
        window_end=_fmt_date(window_end),
        key_numbers={
            "period_return": cumulative_return,
            "period_label": label,
        },
        top_lists={},
        table_rows=table_rows,
        chart_data=chart_data,
        chart_caption_draft=chart_data["caption"],
        table_caption_draft="Daily summary metrics for the period close.",
        methodology_url=config.REPORT_METHOD_URL,
        safe_source_snippets=[],
        constraints={
            "no_prices": True,
            "no_advice": True,
            "tone": "research/education",
        },
        metrics=metrics,
        docx_tables=[{"title": "Period Metrics", "rows": table_rows}],
        docx_charts=[chart_data],
        csv_extracts={"period_metrics": _rows_to_csv(table_rows)},
    )
    return bundle


def build_company_spotlight_bundle(
    latest_date: dt.date,
    latest_rows: List[Dict[str, Any]],
    ticker: str,
) -> AnalysisBundle:
    ticker_upper = ticker.upper()
    target = None
    scores = []
    for row in latest_rows:
        if row.get("ticker"):
            scores.append(row.get("aiges"))
        if row.get("ticker") and row.get("ticker").upper() == ticker_upper:
            target = row
    avg_score = None
    valid_scores = [float(x) for x in scores if x is not None]
    if valid_scores:
        avg_score = sum(valid_scores) / len(valid_scores)

    table_rows = []
    if target:
        table_rows.append(
            {
                "Company": target.get("company"),
                "Ticker": target.get("ticker"),
                "Sector": target.get("sector"),
                "Weight %": round(_weight_fraction(target.get("weight")) * 100, 2),
                "AIGES": round(_safe_float(target.get("aiges")), 2),
            }
        )

    chart_data = {
        "type": "bar",
        "title": "Company vs index average AIGES",
        "x": ["Company", "Index Avg"],
        "series": [
            {
                "name": "AIGES",
                "values": [
                    round(_safe_float(target.get("aiges")) if target else 0, 2),
                    round(_safe_float(avg_score or 0), 2),
                ],
            }
        ],
        "caption": "Company AIGES compared with index average.",
    }

    safe_snippets = []
    if target and target.get("summary"):
        safe_snippets.append(f"public source summary field: {target.get('summary').strip()}")

    metrics = {
        "spotlight": {
            "company": target.get("company") if target else ticker_upper,
            "company_aiges": round(_safe_float(target.get("aiges")) if target else 0, 2),
            "index_avg_aiges": round(_safe_float(avg_score or 0), 2),
        }
    }

    bundle = AnalysisBundle(
        report_type="COMPANY_SPOTLIGHT",
        window_start=latest_date.strftime("%Y-%m-%d"),
        window_end=latest_date.strftime("%Y-%m-%d"),
        key_numbers={
            "company": target.get("company") if target else ticker_upper,
            "company_aiges": round(_safe_float(target.get("aiges")) if target else 0, 2),
            "index_avg_aiges": round(_safe_float(avg_score or 0), 2),
        },
        top_lists={},
        table_rows=table_rows,
        chart_data=chart_data,
        chart_caption_draft=chart_data["caption"],
        table_caption_draft="Company snapshot for the latest rebalance.",
        methodology_url=config.REPORT_METHOD_URL,
        safe_source_snippets=safe_snippets,
        constraints={
            "no_prices": True,
            "no_advice": True,
            "tone": "research/education",
        },
        metrics=metrics,
        docx_tables=[{"title": "Company Snapshot", "rows": table_rows}],
        docx_charts=[chart_data],
        csv_extracts={"company_snapshot": _rows_to_csv(table_rows)},
    )
    return bundle


def build_anomaly_inputs(conn) -> Tuple[Optional[AnalysisBundle], Optional[str]]:
    stats = fetch_stats_latest(conn)
    if not stats:
        return None, "Missing stats data"
    trade_date = stats.get("trade_date")
    if not trade_date:
        return None, "Missing trade_date"
    contributions = fetch_contributions(conn, trade_date, limit=8)
    return build_anomaly_bundle(stats, contributions), None


def build_weekly_inputs(conn) -> Tuple[Optional[AnalysisBundle], Optional[str]]:
    stats_window = fetch_stats_window(conn, days=7)
    levels_window = fetch_levels_window(conn, days=7)
    if not stats_window or not levels_window:
        return None, "Missing weekly data"
    return build_weekly_bundle(stats_window, levels_window), None


def build_period_close_inputs(conn, label: str) -> Tuple[Optional[AnalysisBundle], Optional[str]]:
    stats_window = fetch_stats_window(conn, days=22)
    levels_window = fetch_levels_window(conn, days=22)
    if not stats_window or not levels_window:
        return None, "Missing period data"
    return build_period_close_bundle(stats_window, levels_window, label), None
