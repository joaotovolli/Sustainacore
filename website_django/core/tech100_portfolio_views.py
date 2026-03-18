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

RANGE_OPTIONS = [
    {"code": "3m", "label": "3M"},
    {"code": "6m", "label": "6M"},
    {"code": "ytd", "label": "YTD"},
    {"code": "1y", "label": "1Y"},
    {"code": "max", "label": "Max"},
]

CHART_METRIC_OPTIONS = [
    {"code": "relative", "label": "Relative"},
    {"code": "volatility", "label": "Volatility"},
    {"code": "drawdown", "label": "Drawdown"},
]

VIEW_MODE_OPTIONS = [
    {"code": "absolute", "label": "Absolute"},
    {"code": "active", "label": "Vs benchmark"},
]

ATTRIBUTION_OPTIONS = [
    {"code": "contrib_1d", "label": "1D"},
    {"code": "contrib_5d", "label": "5D"},
    {"code": "contrib_20d", "label": "20D"},
    {"code": "contrib_mtd", "label": "MTD"},
    {"code": "contrib_ytd", "label": "YTD"},
]

HOLDINGS_VIEW_OPTIONS = [
    {"code": "core", "label": "Core"},
    {"code": "signals", "label": "Signals"},
    {"code": "attribution", "label": "Attribution"},
]

MODEL_NOTES = {
    "TECH100": "Official benchmark portfolio derived from the live TECH100 index methodology.",
    "TECH100_EQ": "Equal-weight alternative that strips benchmark concentration without introducing unsupported factors.",
    "TECH100_GOV": "Governance-aware tilt using the existing composite score already present in the TECH100 dataset.",
    "TECH100_MOM": "Momentum-aware tilt using only the supported 20-day momentum signal.",
    "TECH100_LOWVOL": "Low-volatility tilt using only the supported 60-day low-vol signal.",
    "TECH100_GOV_MOM": "Hybrid portfolio blending the existing governance and momentum ranks.",
}


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


def _pick_option(raw_value: Optional[str], *, valid_values: set[str], default: str, upper: bool = False) -> str:
    if raw_value is None:
        return default
    value = raw_value.strip()
    value = value.upper() if upper else value.lower()
    return value if value in valid_values else default


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


def _decorate_positions(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    output = []
    for row in rows:
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
                f"{governance_delta:+.2f} vs benchmark"
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
                f"{top5_delta * 100:+.2f} pts vs benchmark"
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


def _build_snapshot_cards(selected_row: dict[str, object]) -> list[dict[str, object]]:
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
    ]


def _build_delta_cards(selected_row: dict[str, object]) -> list[dict[str, object]]:
    return [
        {
            "label": "Vs benchmark YTD",
            "value": selected_row.get("vs_benchmark_ytd_pct"),
            "suffix": "%",
            "decimals": 2,
            "detail": "Active return versus the selected benchmark.",
        },
        {
            "label": "Top 5 delta",
            "value": selected_row.get("vs_benchmark_top5_pct"),
            "suffix": "%",
            "decimals": 2,
            "detail": "Difference in top-5 concentration versus the selected benchmark.",
        },
        {
            "label": "Governance delta",
            "value": selected_row.get("vs_benchmark_governance"),
            "suffix": "pts",
            "decimals": 1,
            "detail": "Difference in the weighted governance composite.",
        },
    ]


def _build_attribution_rankings(rows: list[dict[str, object]], limit: int = 5) -> dict[str, dict[str, object]]:
    output: dict[str, dict[str, object]] = {}
    for option in ATTRIBUTION_OPTIONS:
        metric_key = option["code"]
        ranked = []
        for row in rows:
            value = _bp(row.get(metric_key))
            if value is None:
                continue
            ranked.append(
                {
                    **row,
                    "metric_key": metric_key,
                    "metric_label": option["label"],
                    "metric_bp": value,
                }
            )
        ranked.sort(key=lambda item: float(item.get("metric_bp") or 0.0), reverse=True)
        output[metric_key] = {
            "label": option["label"],
            "top": ranked[:limit],
            "bottom": sorted(ranked, key=lambda item: float(item.get("metric_bp") or 0.0))[:limit],
        }
    return output


def _build_model_payload(
    meta: dict[str, object],
    summary: dict[str, object],
    positions: list[dict[str, object]],
    sectors: list[dict[str, object]],
    constraints: list[dict[str, str]],
    benchmark_code: str,
) -> dict[str, object]:
    return {
        "code": meta["code"],
        "label": meta["label"],
        "short_label": meta["short_label"],
        "description": meta["description"],
        "color": meta["color"],
        "benchmark_code": benchmark_code,
        "summary": summary,
        "positions": positions,
        "sectors": sectors,
        "constraints": constraints,
        "model_note": MODEL_NOTES.get(str(meta["code"]), meta["description"]),
    }


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

        timeseries_rows = get_timeseries_rows()
        history_dates = [
            row["trade_date"]
            for row in timeseries_rows
            if isinstance(row.get("trade_date"), dt.date)
        ]
        snapshot_by_code = {str(row["model_code"]): row for row in snapshot_rows}
        available_codes = [code for code in MODEL_ORDER if code in snapshot_by_code]
        requested_model = (request.GET.get("model") or "TECH100").strip().upper()
        selected_code = requested_model if requested_model in snapshot_by_code else available_codes[0]
        benchmark_code = _pick_option(
            request.GET.get("benchmark"),
            valid_values=set(available_codes),
            default="TECH100" if "TECH100" in available_codes else selected_code,
            upper=True,
        )
        range_key = _pick_option(
            request.GET.get("range"),
            valid_values={item["code"] for item in RANGE_OPTIONS},
            default="6m",
        )
        metric_key = _pick_option(
            request.GET.get("metric"),
            valid_values={item["code"] for item in CHART_METRIC_OPTIONS},
            default="relative",
        )
        view_mode = _pick_option(
            request.GET.get("view"),
            valid_values={item["code"] for item in VIEW_MODE_OPTIONS},
            default="absolute",
        )
        attribution_key = _pick_option(
            request.GET.get("window"),
            valid_values={item["code"] for item in ATTRIBUTION_OPTIONS},
            default="contrib_ytd",
        )
        holdings_view = _pick_option(
            request.GET.get("holdings"),
            valid_values={item["code"] for item in HOLDINGS_VIEW_OPTIONS},
            default="core",
        )

        benchmark_row = snapshot_by_code.get(benchmark_code)
        decorated_summaries = [
            _decorate_summary(snapshot_by_code[code], benchmark_row)
            for code in available_codes
        ]
        summary_by_code = {str(row["model_code"]): row for row in decorated_summaries}
        optimizer_summary = get_optimizer_summary(latest_trade_date)
        model_definitions = []
        selected_meta = {}
        model_payloads: dict[str, dict[str, object]] = {}
        for meta in get_model_definitions():
            code = meta["code"]
            if code not in snapshot_by_code:
                model_definitions.append(
                    {
                        **meta,
                        "is_selected": code == selected_code,
                        "is_available": False,
                        "summary": None,
                    }
                )
                continue
            positions = _decorate_positions(
                get_position_rows(trade_date=latest_trade_date, model_code=code)
            )
            sectors = _decorate_sectors(
                get_sector_rows(trade_date=latest_trade_date, model_code=code)
            )
            constraints = [
                format_constraint(row) for row in get_constraints(code)
            ]
            enriched = {
                **meta,
                "is_selected": code == selected_code,
                "is_available": True,
                "summary": summary_by_code[code],
            }
            model_definitions.append(enriched)
            if code == selected_code:
                selected_meta = enriched
            model_payloads[code] = _build_model_payload(
                meta=enriched,
                summary=summary_by_code[code],
                positions=positions,
                sectors=sectors,
                constraints=constraints,
                benchmark_code=benchmark_code,
            )

        selected_row = summary_by_code[selected_code]
        selected_payload = model_payloads[selected_code]
        selected_attribution = _build_attribution_rankings(selected_payload["positions"]).get(
            attribution_key,
            {"top": [], "bottom": [], "label": "YTD"},
        )
        timeseries_payload = build_timeseries_payload(timeseries_rows)

        context = {
            "data_mode": data_mode,
            "data_error": False,
            "data_empty": False,
            "latest_trade_date": latest_trade_date.isoformat(),
            "history_start_date": min(history_dates).isoformat() if history_dates else None,
            "stale_days": _days_stale(latest_trade_date),
            "selected_model_code": selected_code,
            "selected_model": selected_meta,
            "model_definitions": model_definitions,
            "comparison_rows": decorated_summaries,
            "selected_summary": selected_row,
            "selected_snapshot_cards": _build_snapshot_cards(selected_row),
            "selected_delta_cards": _build_delta_cards(selected_row),
            "factor_cards": _build_factor_cards(selected_row, summary_by_code.get(benchmark_code)),
            "selected_model_note": selected_payload["model_note"],
            "positions": selected_payload["positions"][:12],
            "positions_count": len(selected_payload["positions"]),
            "sector_rows": selected_payload["sectors"],
            "sector_count": len(selected_payload["sectors"]),
            "selected_attribution": selected_attribution,
            "selected_attribution_key": attribution_key,
            "constraints": selected_payload["constraints"],
            "constraints_count": len(selected_payload["constraints"]),
            "benchmark_code": benchmark_code,
            "range_key": range_key,
            "metric_key": metric_key,
            "view_mode": view_mode,
            "holdings_view": holdings_view,
            "optimizer_summary": {
                **optimizer_summary,
                "avg_momentum_20d_pct": _pct(optimizer_summary.get("avg_momentum_20d")),
                "avg_low_vol_60d_pct": _pct(optimizer_summary.get("avg_low_vol_60d")),
            },
            "supported_analytics": get_supported_analytics(),
            "deferred_analytics": get_deferred_analytics(),
            "workspace_payload": {
                "latestTradeDate": latest_trade_date.isoformat(),
                "historyStartDate": min(history_dates).isoformat() if history_dates else None,
                "selectedModelCode": selected_code,
                "benchmarkCode": benchmark_code,
                "rangeKey": range_key,
                "metricKey": metric_key,
                "viewMode": view_mode,
                "attributionKey": attribution_key,
                "holdingsView": holdings_view,
                "attributionKeyLabels": {
                    item["code"]: item["label"] for item in ATTRIBUTION_OPTIONS
                },
                "modelOrder": available_codes,
                "chartMeta": [
                    {"code": meta["code"], "label": meta["short_label"], "color": meta["color"]}
                    for meta in get_model_definitions()
                    if meta["code"] in available_codes
                ],
                "seriesByModel": timeseries_payload,
                "models": model_payloads,
                "optimizerSummary": {
                    **optimizer_summary,
                    "avg_momentum_20d_pct": _pct(optimizer_summary.get("avg_momentum_20d")),
                    "avg_low_vol_60d_pct": _pct(optimizer_summary.get("avg_low_vol_60d")),
                },
            },
            "range_options": RANGE_OPTIONS,
            "metric_options": CHART_METRIC_OPTIONS,
            "view_options": VIEW_MODE_OPTIONS,
            "attribution_options": ATTRIBUTION_OPTIONS,
            "holdings_view_options": HOLDINGS_VIEW_OPTIONS,
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
