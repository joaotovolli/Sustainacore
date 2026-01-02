"""Analysis bundle generation for research reports."""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

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
        }


def _fmt_date(value: Optional[dt.date]) -> str:
    if not value:
        return ""
    return value.strftime("%Y-%m-%d")


def _compute_sector_exposure(rows: List[Dict[str, Any]]) -> Dict[str, float]:
    exposure: Dict[str, float] = {}
    for row in rows:
        sector = row.get("sector") or "Unknown"
        weight = float(row.get("weight") or 0)
        exposure[sector] = exposure.get(sector, 0.0) + weight
    return exposure


def _build_sector_delta(latest: List[Dict[str, Any]], previous: List[Dict[str, Any]]) -> Dict[str, float]:
    latest_exposure = _compute_sector_exposure(latest)
    prev_exposure = _compute_sector_exposure(previous)
    sectors = set(latest_exposure) | set(prev_exposure)
    return {sector: latest_exposure.get(sector, 0.0) - prev_exposure.get(sector, 0.0) for sector in sectors}


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


def build_rebalance_bundle(
    latest_date: dt.date,
    previous_date: Optional[dt.date],
    latest_rows: List[Dict[str, Any]],
    previous_rows: List[Dict[str, Any]],
) -> AnalysisBundle:
    tickers_latest = {row.get("ticker") for row in latest_rows if row.get("ticker")}
    tickers_prev = {row.get("ticker") for row in previous_rows if row.get("ticker")}
    entrants = sorted(tickers_latest - tickers_prev)
    exits = sorted(tickers_prev - tickers_latest)
    sector_delta = _build_sector_delta(latest_rows, previous_rows)

    avg_aiges = None
    scores = [row.get("aiges") for row in latest_rows if row.get("aiges") is not None]
    if scores:
        avg_aiges = sum(float(x) for x in scores) / len(scores)

    table_rows = []
    for row in latest_rows[: config.MAX_TABLE_ROWS]:
        table_rows.append(
            {
                "Company": row.get("company"),
                "Ticker": row.get("ticker"),
                "Sector": row.get("sector"),
                "Weight": round(float(row.get("weight") or 0), 4),
                "AIGES": round(float(row.get("aiges") or 0), 2),
            }
        )

    safe_snippets = []
    for row in latest_rows:
        summary = row.get("summary")
        if summary:
            safe_snippets.append(f"public source summary field: {summary.strip()}")
        if len(safe_snippets) >= 2:
            break

    bundle = AnalysisBundle(
        report_type="REBALANCE",
        window_start=_fmt_date(previous_date),
        window_end=_fmt_date(latest_date),
        key_numbers={
            "constituents": len(latest_rows),
            "avg_aiges": round(avg_aiges, 2) if avg_aiges is not None else None,
            "entrants": len(entrants),
            "exits": len(exits),
        },
        top_lists={
            "entrants": entrants[:10],
            "exits": exits[:10],
            "governance_buckets": _bucket_aiges(latest_rows),
            "top_sectors_delta": dict(sorted(sector_delta.items(), key=lambda kv: abs(kv[1]), reverse=True)[:6]),
        },
        table_rows=table_rows,
        chart_data={
            "type": "bar",
            "title": "Sector exposure delta (weights)",
            "x": list(sector_delta.keys()),
            "y": [round(sector_delta[k], 4) for k in sector_delta.keys()],
        },
        chart_caption_draft="Sector exposure deltas based on latest rebalance weights.",
        table_caption_draft="Top constituents by weight with AI governance scores.",
        methodology_url=config.REPORT_METHOD_URL,
        safe_source_snippets=safe_snippets,
        constraints={
            "no_prices": True,
            "no_advice": True,
            "tone": "research/education",
        },
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
                "Contribution": round(float(row.get("contribution") or 0), 6),
            }
        )

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
        chart_data={
            "type": "bar",
            "title": "Top daily contributions",
            "x": [row.get("ticker") for row in contributions],
            "y": [round(float(row.get("contribution") or 0), 6) for row in contributions],
        },
        chart_caption_draft="Top contributors for the latest trading day.",
        table_caption_draft="Largest daily contributions (no price data).",
        methodology_url=config.REPORT_METHOD_URL,
        safe_source_snippets=[],
        constraints={
            "no_prices": True,
            "no_advice": True,
            "tone": "research/education",
        },
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
        chart_data={
            "type": "line",
            "title": "Index level (total return)",
            "x": chart_dates,
            "y": chart_levels,
        },
        chart_caption_draft="Index total return levels over the review window.",
        table_caption_draft="Daily summary metrics over the review window.",
        methodology_url=config.REPORT_METHOD_URL,
        safe_source_snippets=[],
        constraints={
            "no_prices": True,
            "no_advice": True,
            "tone": "research/education",
        },
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
        chart_data={
            "type": "line",
            "title": "Index level (total return)",
            "x": chart_dates,
            "y": chart_levels,
        },
        chart_caption_draft="Index total return levels over the period.",
        table_caption_draft="Daily summary metrics for the period close.",
        methodology_url=config.REPORT_METHOD_URL,
        safe_source_snippets=[],
        constraints={
            "no_prices": True,
            "no_advice": True,
            "tone": "research/education",
        },
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
