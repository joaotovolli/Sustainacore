from __future__ import annotations

import datetime as dt
import logging
from typing import Optional

from django.shortcuts import render
from django.views.decorators.cache import cache_page
from django.views.decorators.http import require_safe

from core.tech100_portfolio_data import (
    MODEL_ORDER,
    build_timeseries_payload,
    format_constraint,
    get_constraints,
    get_data_mode,
    get_deferred_analytics,
    get_latest_trade_date,
    get_model_definitions,
    get_optimizer_summary,
    get_position_rows,
    get_sector_rows,
    get_snapshot_rows,
    get_supported_analytics,
    get_timeseries_rows,
    summarize_contribution_windows,
)

logger = logging.getLogger(__name__)

PAGE_CACHE_SECONDS = 60


def _pct(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value) * 100.0
    except (TypeError, ValueError):
        return None


def _bp(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value) * 10000.0
    except (TypeError, ValueError):
        return None


def _score_delta(selected: Optional[float], benchmark: Optional[float]) -> Optional[float]:
    if selected is None or benchmark is None:
        return None
    return float(selected) - float(benchmark)


def _days_stale(trade_date: Optional[dt.date]) -> Optional[int]:
    if trade_date is None:
        return None
    delta = dt.date.today() - trade_date
    return max(delta.days, 0)


def _decorate_summary(row: dict[str, object], benchmark_row: Optional[dict[str, object]]) -> dict[str, object]:
    model_code = str(row.get("model_code"))
    benchmark_top5 = benchmark_row.get("top5_weight") if benchmark_row else None
    benchmark_ytd = benchmark_row.get("ret_ytd") if benchmark_row else None
    benchmark_governance = benchmark_row.get("avg_governance_score") if benchmark_row else None
    return {
        **row,
        "level_tr_fmt": row.get("level_tr"),
        "ret_1d_pct": _pct(row.get("ret_1d")),
        "ret_5d_pct": _pct(row.get("ret_5d")),
        "ret_20d_pct": _pct(row.get("ret_20d")),
        "ret_mtd_pct": _pct(row.get("ret_mtd")),
        "ret_ytd_pct": _pct(row.get("ret_ytd")),
        "vol_20d_pct": _pct(row.get("vol_20d")),
        "vol_60d_pct": _pct(row.get("vol_60d")),
        "drawdown_to_date_pct": _pct(row.get("drawdown_to_date")),
        "max_drawdown_252d_pct": _pct(row.get("max_drawdown_252d")),
        "top1_weight_pct": _pct(row.get("top1_weight")),
        "top5_weight_pct": _pct(row.get("top5_weight")),
        "factor_sector_tilt_abs_pct": _pct(row.get("factor_sector_tilt_abs")),
        "avg_momentum_20d_pct": _pct(row.get("avg_momentum_20d")),
        "avg_low_vol_60d_pct": _pct(row.get("avg_low_vol_60d")),
        "vs_benchmark_ytd_pct": _pct(_score_delta(row.get("ret_ytd"), benchmark_ytd)),
        "vs_benchmark_top5_pct": _pct(_score_delta(row.get("top5_weight"), benchmark_top5)),
        "vs_benchmark_governance": _score_delta(
            row.get("avg_governance_score"),
            benchmark_governance,
        ),
        "is_official": model_code == "TECH100",
    }


def _decorate_positions(rows: list[dict[str, object]], limit: int = 12) -> list[dict[str, object]]:
    output = []
    for row in rows[:limit]:
        output.append(
            {
                **row,
                "model_weight_pct": _pct(row.get("model_weight")),
                "benchmark_weight_pct": _pct(row.get("benchmark_weight")),
                "active_weight_pct": _pct(row.get("active_weight")),
                "ret_1d_pct": _pct(row.get("ret_1d")),
                "contrib_1d_bp": _bp(row.get("contrib_1d")),
                "contrib_5d_bp": _bp(row.get("contrib_5d")),
                "contrib_20d_bp": _bp(row.get("contrib_20d")),
                "contrib_mtd_bp": _bp(row.get("contrib_mtd")),
                "contrib_ytd_bp": _bp(row.get("contrib_ytd")),
                "momentum_20d_pct": _pct(row.get("momentum_20d")),
                "low_vol_60d_pct": _pct(row.get("low_vol_60d")),
            }
        )
    return output


def _decorate_sectors(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    output = []
    for row in rows:
        output.append(
            {
                **row,
                "sector_weight_pct": _pct(row.get("sector_weight")),
                "benchmark_sector_weight_pct": _pct(row.get("benchmark_sector_weight")),
                "active_sector_weight_pct": _pct(row.get("active_sector_weight")),
                "contrib_1d_bp": _bp(row.get("contrib_1d")),
                "contrib_5d_bp": _bp(row.get("contrib_5d")),
                "contrib_20d_bp": _bp(row.get("contrib_20d")),
                "contrib_mtd_bp": _bp(row.get("contrib_mtd")),
                "contrib_ytd_bp": _bp(row.get("contrib_ytd")),
            }
        )
    return output


def _build_factor_cards(
    selected_row: dict[str, object],
    benchmark_row: Optional[dict[str, object]],
) -> list[dict[str, object]]:
    governance_delta = _score_delta(
        selected_row.get("avg_governance_score"),
        benchmark_row.get("avg_governance_score") if benchmark_row else None,
    )
    top5_delta = _score_delta(
        selected_row.get("top5_weight"),
        benchmark_row.get("top5_weight") if benchmark_row else None,
    )
    return [
        {
            "label": "Governance composite",
            "value": selected_row.get("avg_governance_score"),
            "suffix": "pts",
            "detail": (
                f"{governance_delta:+.2f} vs official TECH100"
                if governance_delta is not None and not selected_row.get("is_official")
                else "Weighted average from the live TECH100 governance dataset"
            ),
        },
        {
            "label": "Momentum lens",
            "value": selected_row.get("avg_momentum_20d_pct"),
            "suffix": "%",
            "detail": "Uses the supported 20-day momentum signal only",
        },
        {
            "label": "Low-vol lens",
            "value": selected_row.get("avg_low_vol_60d_pct"),
            "suffix": "%",
            "detail": "Uses the supported 60-day low-vol signal only",
        },
        {
            "label": "Concentration",
            "value": selected_row.get("top5_weight_pct"),
            "suffix": "%",
            "detail": (
                f"{top5_delta * 100:+.2f} pts vs official TECH100"
                if top5_delta is not None and not selected_row.get("is_official")
                else "Top 5 weight share"
            ),
        },
        {
            "label": "Sector tilt",
            "value": selected_row.get("factor_sector_tilt_abs_pct"),
            "suffix": "%",
            "detail": "Absolute sector deviation from the official TECH100 mix",
        },
    ]


def _build_kpi_cards(selected_row: dict[str, object], stale_days: Optional[int]) -> list[dict[str, object]]:
    freshness = "Current"
    if stale_days and stale_days > 1:
        freshness = f"{stale_days} days old"
    return [
        {"label": "Index level", "value": selected_row.get("level_tr_fmt"), "suffix": "", "decimals": 2},
        {"label": "20D return", "value": selected_row.get("ret_20d_pct"), "suffix": "%", "decimals": 2},
        {"label": "YTD return", "value": selected_row.get("ret_ytd_pct"), "suffix": "%", "decimals": 2},
        {"label": "20D vol", "value": selected_row.get("vol_20d_pct"), "suffix": "%", "decimals": 2},
        {
            "label": "Drawdown to date",
            "value": selected_row.get("drawdown_to_date_pct"),
            "suffix": "%",
            "decimals": 2,
        },
        {"label": "Top 5 weight", "value": selected_row.get("top5_weight_pct"), "suffix": "%", "decimals": 2},
        {
            "label": "Governance composite",
            "value": selected_row.get("avg_governance_score"),
            "suffix": "pts",
            "decimals": 1,
        },
        {
            "label": "Data freshness",
            "value": freshness,
            "suffix": "",
            "decimals": None,
        },
    ]


@require_safe
@cache_page(PAGE_CACHE_SECONDS)
def tech100_portfolio(request):
    data_mode = get_data_mode()
    try:
        latest_trade_date = get_latest_trade_date()
        snapshot_rows = get_snapshot_rows(latest_trade_date)
        if not latest_trade_date or not snapshot_rows:
            context = {
                "data_mode": data_mode,
                "data_error": False,
                "data_empty": True,
                "model_definitions": get_model_definitions(),
                "supported_analytics": get_supported_analytics(),
                "deferred_analytics": get_deferred_analytics(),
            }
            return render(request, "tech100_portfolio.html", context)

        snapshot_by_code = {str(row["model_code"]): row for row in snapshot_rows}
        available_codes = [code for code in MODEL_ORDER if code in snapshot_by_code]
        requested_model = (request.GET.get("model") or "TECH100").strip().upper()
        selected_code = requested_model if requested_model in snapshot_by_code else available_codes[0]

        benchmark_row = snapshot_by_code.get("TECH100")
        decorated_summaries = [
            _decorate_summary(snapshot_by_code[code], benchmark_row)
            for code in available_codes
        ]
        selected_row = next(row for row in decorated_summaries if row["model_code"] == selected_code)

        position_rows = get_position_rows(trade_date=latest_trade_date, model_code=selected_code)
        sector_rows = get_sector_rows(trade_date=latest_trade_date, model_code=selected_code)
        optimizer_summary = get_optimizer_summary(latest_trade_date)
        constraint_rows = [format_constraint(row) for row in get_constraints(selected_code)]
        timeseries_payload = build_timeseries_payload(get_timeseries_rows())
        contribution_windows = summarize_contribution_windows(position_rows)

        model_definitions = []
        selected_meta = {}
        for meta in get_model_definitions():
            code = meta["code"]
            enriched = {
                **meta,
                "is_selected": code == selected_code,
                "is_available": code in snapshot_by_code,
                "summary": next((row for row in decorated_summaries if row["model_code"] == code), None),
            }
            model_definitions.append(enriched)
            if code == selected_code:
                selected_meta = enriched

        context = {
            "data_mode": data_mode,
            "data_error": False,
            "data_empty": False,
            "latest_trade_date": latest_trade_date.isoformat(),
            "latest_rebalance_date": selected_row.get("rebalance_date").isoformat()
            if selected_row.get("rebalance_date")
            else None,
            "stale_days": _days_stale(latest_trade_date),
            "selected_model_code": selected_code,
            "selected_model": selected_meta,
            "model_definitions": model_definitions,
            "comparison_rows": decorated_summaries,
            "selected_summary": selected_row,
            "selected_kpis": _build_kpi_cards(selected_row, _days_stale(latest_trade_date)),
            "factor_cards": _build_factor_cards(selected_row, benchmark_row),
            "positions": _decorate_positions(position_rows),
            "positions_count": len(position_rows),
            "sector_rows": _decorate_sectors(sector_rows),
            "sector_count": len(sector_rows),
            "contribution_windows": contribution_windows,
            "constraints": constraint_rows,
            "constraints_count": len(constraint_rows),
            "optimizer_summary": {
                **optimizer_summary,
                "avg_momentum_20d_pct": _pct(optimizer_summary.get("avg_momentum_20d")),
                "avg_low_vol_60d_pct": _pct(optimizer_summary.get("avg_low_vol_60d")),
            },
            "supported_analytics": get_supported_analytics(),
            "deferred_analytics": get_deferred_analytics(),
            "timeseries_payload": timeseries_payload,
            "model_chart_meta": [
                {"code": meta["code"], "label": meta["short_label"], "color": meta["color"]}
                for meta in get_model_definitions()
                if meta["code"] in available_codes
            ],
        }
    except Exception:
        logger.exception("TECH100 portfolio analytics page failed.")
        context = {
            "data_mode": data_mode,
            "data_error": True,
            "data_empty": False,
            "model_definitions": get_model_definitions(),
            "supported_analytics": get_supported_analytics(),
            "deferred_analytics": get_deferred_analytics(),
        }
    return render(request, "tech100_portfolio.html", context)
