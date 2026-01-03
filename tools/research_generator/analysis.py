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


def _split_core_coverage(rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    core = [row for row in rows if _safe_float(row.get("weight")) > config.CORE_WEIGHT_THRESHOLD]
    return core, rows


def _compute_sector_exposure(rows: List[Dict[str, Any]], *, weight_key: str) -> Dict[str, float]:
    exposure: Dict[str, float] = {}
    for row in rows:
        sector = row.get("sector") or "Unknown"
        weight = _safe_float(row.get(weight_key))
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
    current_weights = {row.get("ticker"): _safe_float(row.get("weight")) for row in current if row.get("ticker")}
    prev_weights = {row.get("ticker"): _safe_float(row.get("weight")) for row in previous if row.get("ticker")}
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


def _build_top_movers(
    latest: List[Dict[str, Any]],
    previous: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    prev_by_ticker = {row.get("ticker"): row for row in previous if row.get("ticker")}
    latest_by_ticker = {row.get("ticker"): row for row in latest if row.get("ticker")}
    all_tickers = set(prev_by_ticker) | set(latest_by_ticker)
    weight_moves: List[Dict[str, Any]] = []
    score_moves: List[Dict[str, Any]] = []
    for ticker in all_tickers:
        row = latest_by_ticker.get(ticker) or prev_by_ticker.get(ticker) or {}
        prev = prev_by_ticker.get(ticker)
        latest_row = latest_by_ticker.get(ticker)
        weight_old = _safe_float(prev.get("weight")) if prev else 0.0
        weight_new = _safe_float(latest_row.get("weight") if latest_row else 0.0)
        score_old = _safe_float(prev.get("aiges")) if prev else 0.0
        score_new = _safe_float(latest_row.get("aiges") if latest_row else 0.0)
        weight_moves.append(
            {
                "ticker": ticker,
                "company": row.get("company"),
                "sector": row.get("sector") or "Unknown",
                "delta_weight": round(weight_new - weight_old, 6),
                "weight_prev": round(weight_old, 6),
                "weight_new": round(weight_new, 6),
                "summary": row.get("summary") or "",
            }
        )
        if row.get("aiges") is not None:
            score_moves.append(
                {
                    "ticker": ticker,
                    "company": row.get("company"),
                    "sector": row.get("sector") or "Unknown",
                    "delta_aiges": round(score_new - score_old, 2),
                    "aiges_prev": round(score_old, 2),
                    "aiges_new": round(score_new, 2),
                    "summary": row.get("summary") or "",
                }
            )
    weight_moves.sort(key=lambda r: abs(_safe_float(r.get("delta_weight"))), reverse=True)
    score_moves.sort(key=lambda r: abs(_safe_float(r.get("delta_aiges"))), reverse=True)
    return weight_moves[:10], score_moves[:10]


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

    weights_core = [_safe_float(row.get("weight")) for row in latest_core]
    weighted_mean_core = _weighted_mean(scores_core_float, weights_core)

    top5_weight_share = None
    hhi = None
    total_weight = sum(weights_core)
    if total_weight:
        sorted_weights = sorted(weights_core, reverse=True)
        top5_weight_share = round(sum(sorted_weights[:5]) / total_weight, 4)
        hhi = round(sum((w / total_weight) ** 2 for w in weights_core), 4)

    breadth_pct = _compute_breadth(latest_core, prev_core)
    turnover = _compute_turnover(latest_core, prev_core)

    weight_moves, score_moves = _build_top_movers(latest_core, prev_core)

    avg_aiges = weighted_mean_core
    key_numbers = {
        "core_constituents": len(latest_core),
        "coverage_constituents": len(latest_cov),
        "avg_aiges_weighted": round(avg_aiges, 2) if avg_aiges is not None else None,
        "entrants": len(entrants),
        "exits": len(exits),
        "turnover": turnover,
    }

    table_rows = []
    for row in latest_core[: config.MAX_TABLE_ROWS]:
        table_rows.append(
            {
                "Company": row.get("company"),
                "Ticker": row.get("ticker"),
                "Sector": row.get("sector"),
                "Weight": round(_safe_float(row.get("weight")), 4),
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
            "Mean Composite (weighted)": round(weighted_mean_core, 2) if weighted_mean_core else "",
            "Median Composite": round(median(scores_core_float), 2) if scores_core_float else "",
            "IQR Composite": round(_iqr(scores_core_float), 2) if scores_core_float else "",
            "Top5 Weight Share": top5_weight_share if top5_weight_share is not None else "",
            "HHI": hhi if hhi is not None else "",
        },
        {
            "Segment": "Coverage (All 100)",
            "N": len(latest_cov),
            "Mean Composite (unweighted)": round(mean(scores_cov_float), 2) if scores_cov_float else "",
            "Median Composite": round(median(scores_cov_float), 2) if scores_cov_float else "",
            "IQR Composite": round(_iqr(scores_cov_float), 2) if scores_cov_float else "",
            "Top5 Weight Share": "",
            "HHI": "",
        },
    ]

    sector_rows = []
    sectors = sorted(set(weighted_sector_latest) | set(weighted_sector_prev) | set(coverage_count_latest))
    for sector in sectors:
        count_delta = coverage_count_delta.get(sector, 0.0)
        count_delta_display = round(count_delta, 2)
        if sector in inconsistent_sectors:
            count_delta_display = "FLAG"
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

    movers_rows = []
    for idx, row in enumerate(score_moves, start=1):
        support = row.get("summary", "")
        movers_rows.append(
            {
                "Type": "Score",
                "Rank": idx,
                "Ticker": row.get("ticker"),
                "Company": row.get("company"),
                "Sector": row.get("sector"),
                "Delta": row.get("delta_aiges"),
                "Support": support[:120] if idx <= 3 and support else "",
            }
        )
    for idx, row in enumerate(weight_moves, start=1):
        support = row.get("summary", "")
        movers_rows.append(
            {
                "Type": "Weight",
                "Rank": idx,
                "Ticker": row.get("ticker"),
                "Company": row.get("company"),
                "Sector": row.get("sector"),
                "Delta": row.get("delta_weight"),
                "Support": support[:120] if idx <= 3 and support else "",
            }
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
            "top5_weight_share": top5_weight_share,
            "hhi": hhi,
            "breadth_pct": breadth_pct,
        },
        "coverage": {
            "n": len(latest_cov),
            "mean_aiges": round(mean(scores_cov_float), 2) if scores_cov_float else None,
            "median_aiges": round(median(scores_cov_float), 2) if scores_cov_float else None,
            "iqr_aiges": round(_iqr(scores_cov_float), 2) if scores_cov_float else None,
        },
        "pillars": _pillar_stats(latest_cov),
        "rebalance": {
            "turnover": turnover,
            "entrants": entrants[:10],
            "exits": exits[:10],
        },
        "sector_exposure": {
            "core_weighted_latest": weighted_sector_latest,
            "core_weighted_prev": weighted_sector_prev,
            "coverage_count_latest": coverage_count_latest,
            "coverage_count_prev": coverage_count_prev,
            "core_count_delta_flags": inconsistent_sectors,
        },
        "top_movers": {
            "weight": weight_moves,
            "score": score_moves,
        },
        "core_rank_by_aiges": _rank_core_by_aiges(latest_core),
    }

    csv_extracts = {
        "sector_exposure": _rows_to_csv(sector_rows),
        "top_movers": _rows_to_csv(movers_rows),
        "summary": _rows_to_csv(summary_table),
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
            {"title": "Core vs Coverage Summary", "rows": summary_table},
            {"title": "Sector Exposure Comparison", "rows": sector_rows},
            {"title": "Top Movers", "rows": movers_rows},
        ],
        docx_charts=[chart_sector, chart_aiges, chart_breadth],
        csv_extracts=csv_extracts,
    )
    return bundle


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
                "Weight": round(_safe_float(target.get("weight")), 4),
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
