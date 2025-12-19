from __future__ import annotations

import datetime as dt
import logging
from typing import Optional

from django.http import JsonResponse
from django.shortcuts import render
from django.views.decorators.http import require_GET

from core.tech100_index_data import (
    DrawdownResult,
    get_data_mode,
    get_contribution,
    get_constituents,
    get_index_levels,
    get_index_returns,
    get_imputed_overview,
    get_kpis,
    get_latest_trade_date,
    get_max_drawdown,
    get_rolling_vol,
    get_stats,
    get_trade_date_bounds,
)

logger = logging.getLogger(__name__)

RANGE_WINDOWS = {
    "1m": 30,
    "3m": 90,
    "6m": 180,
    "1y": 365,
}


def _parse_date(value: Optional[str], fallback: Optional[dt.date]) -> Optional[dt.date]:
    if not value:
        return fallback
    try:
        return dt.date.fromisoformat(value)
    except ValueError:
        return fallback


def _range_start(latest: dt.date, range_key: str) -> dt.date:
    if range_key == "ytd":
        return dt.date(latest.year, 1, 1)
    if range_key == "max":
        min_date, _ = get_trade_date_bounds()
        return min_date or latest
    days = RANGE_WINDOWS.get(range_key, RANGE_WINDOWS["1y"])
    return latest - dt.timedelta(days=days)


def _format_pct(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value) * 100.0
    except (TypeError, ValueError):
        return None


def _format_bp(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value) * 10000.0
    except (TypeError, ValueError):
        return None


def _best_worst(returns: list[tuple[dt.date, float]]) -> tuple[Optional[tuple], Optional[tuple]]:
    if not returns:
        return None, None
    best = max(returns, key=lambda item: item[1])
    worst = min(returns, key=lambda item: item[1])
    return best, worst


@require_GET
def tech100_index_overview(request):
    data_mode = get_data_mode()
    try:
        latest = get_latest_trade_date()
        if latest is None:
            raise ValueError("No TECH100 trade dates available.")

        range_key = (request.GET.get("range") or "1y").lower()
        if range_key not in {"1m", "3m", "6m", "ytd", "1y", "max"}:
            range_key = "1y"
        start_date = _range_start(latest, range_key)

        levels = get_index_levels(start_date, latest)
        returns = get_index_returns(start_date, latest)
        kpis = get_kpis(latest)
        stats = get_stats(latest)
        vol_20d = get_rolling_vol(latest)
        drawdown = get_max_drawdown(start_date, latest)
        best_day, worst_day = _best_worst(returns)

        constituents = get_constituents(latest)
        top_constituents = [
            {
                **row,
                "weight_pct": _format_pct(row.get("weight")),
                "ret_1d_pct": _format_pct(row.get("ret_1d")),
                "contribution_bp": _format_bp(row.get("contribution")),
            }
            for row in constituents[:25]
        ]
        imputed_count = stats.get("n_imputed") if stats else None
        if imputed_count is None:
            imputed_count = sum(1 for row in constituents if row.get("quality") == "IMPUTED")

        imputed_overview = get_imputed_overview(latest)
        levels_payload = [
            {"date": trade_date.isoformat(), "level": level} for trade_date, level in levels
        ]

        context = {
            "data_mode": data_mode,
            "data_error": False,
            "range_key": range_key,
            "latest_date": latest.isoformat(),
            "levels_payload": levels_payload,
            "levels_count": len(levels_payload),
            "constituents_count": len(top_constituents),
            "kpis": {
                "level": kpis.get("level"),
                "ret_1d": _format_pct(kpis.get("ret_1d")),
                "ret_1w": _format_pct(kpis.get("ret_1w")),
                "ret_1m": _format_pct(kpis.get("ret_1m")),
                "ret_ytd": _format_pct(kpis.get("ret_ytd")),
                "ret_since_inception": _format_pct(kpis.get("ret_since_inception")),
            },
            "risk": {
                "vol_20d": _format_pct(vol_20d),
                "max_drawdown": _format_pct(drawdown.drawdown),
                "max_drawdown_peak": drawdown.peak_date.isoformat() if drawdown.peak_date else None,
                "max_drawdown_trough": drawdown.trough_date.isoformat()
                if drawdown.trough_date
                else None,
                "best_day": best_day[0].isoformat() if best_day else None,
                "best_day_return": _format_pct(best_day[1]) if best_day else None,
                "worst_day": worst_day[0].isoformat() if worst_day else None,
                "worst_day_return": _format_pct(worst_day[1]) if worst_day else None,
            },
            "imputed_count": imputed_count,
            "imputed_url": f"/tech100/constituents/?date={latest.isoformat()}&imputed=1",
            "top_constituents": top_constituents,
            "imputed_overview": imputed_overview,
        }
    except Exception:
        logger.exception("TECH100 index overview failed.")
        context = {
            "data_mode": data_mode,
            "data_error": True,
            "levels_payload": [],
            "levels_count": 0,
            "constituents_count": 0,
        }
    return render(request, "tech100_index_overview.html", context)


@require_GET
def tech100_constituents(request):
    latest = get_latest_trade_date()
    selected_date = _parse_date(request.GET.get("date"), latest)
    min_date, max_date = get_trade_date_bounds()
    show_imputed_only = request.GET.get("imputed") == "1"

    rows = get_constituents(selected_date) if selected_date else []
    if show_imputed_only:
        rows = [row for row in rows if row.get("quality") == "IMPUTED"]
    weight_sum = sum(float(row.get("weight") or 0.0) for row in rows)

    context = {
        "selected_date": selected_date.isoformat() if selected_date else "",
        "min_date": min_date.isoformat() if min_date else "",
        "max_date": max_date.isoformat() if max_date else "",
        "show_imputed_only": show_imputed_only,
        "rows": rows,
        "weight_sum": weight_sum,
    }
    return render(request, "tech100_constituents.html", context)


@require_GET
def tech100_attribution(request):
    latest = get_latest_trade_date()
    selected_date = _parse_date(request.GET.get("date"), latest)
    min_date, max_date = get_trade_date_bounds()
    rows = get_contribution(selected_date) if selected_date else []

    top = [row for row in rows if (row.get("contribution") or 0) > 0][:15]
    bottom = [row for row in rows if (row.get("contribution") or 0) < 0][-15:]
    bottom = list(reversed(bottom))

    stats = get_stats(selected_date) if selected_date else {}
    index_return = stats.get("ret_1d") if stats else None
    contrib_sum = sum(float(row.get("contribution") or 0.0) for row in rows)

    context = {
        "selected_date": selected_date.isoformat() if selected_date else "",
        "min_date": min_date.isoformat() if min_date else "",
        "max_date": max_date.isoformat() if max_date else "",
        "top_rows": top,
        "bottom_rows": bottom,
        "index_return": _format_pct(index_return),
        "contrib_sum": _format_pct(contrib_sum),
    }
    return render(request, "tech100_attribution.html", context)


@require_GET
def tech100_stats(request):
    latest = get_latest_trade_date()
    selected_date = _parse_date(request.GET.get("date"), latest)
    min_date, max_date = get_trade_date_bounds()
    stats = get_stats(selected_date) if selected_date else {}

    context = {
        "selected_date": selected_date.isoformat() if selected_date else "",
        "min_date": min_date.isoformat() if min_date else "",
        "max_date": max_date.isoformat() if max_date else "",
        "stats": stats,
    }
    return render(request, "tech100_stats.html", context)


@require_GET
def api_tech100_index_levels(request):
    latest = get_latest_trade_date()
    if latest is None:
        return JsonResponse({"error": "no_data"}, status=503)

    range_key = (request.GET.get("range") or "1y").lower()
    if range_key not in {"1m", "3m", "6m", "ytd", "1y", "max"}:
        range_key = "1y"
    start_date = _range_start(latest, range_key)

    levels = get_index_levels(start_date, latest)
    payload = [
        {"date": trade_date.isoformat(), "level": level} for trade_date, level in levels
    ]
    return JsonResponse(
        {"range": range_key, "start_date": start_date.isoformat(), "end_date": latest.isoformat(), "levels": payload}
    )


@require_GET
def api_tech100_kpis(request):
    latest = get_latest_trade_date()
    selected_date = _parse_date(request.GET.get("date"), latest)
    if selected_date is None:
        return JsonResponse({"error": "no_data"}, status=503)

    kpis = get_kpis(selected_date)
    return JsonResponse(
        {
            "as_of": selected_date.isoformat(),
            "level": kpis.get("level"),
            "ret_1d": _format_pct(kpis.get("ret_1d")),
            "ret_1w": _format_pct(kpis.get("ret_1w")),
            "ret_1m": _format_pct(kpis.get("ret_1m")),
            "ret_ytd": _format_pct(kpis.get("ret_ytd")),
            "ret_since_inception": _format_pct(kpis.get("ret_since_inception")),
        }
    )


@require_GET
def api_tech100_constituents(request):
    latest = get_latest_trade_date()
    selected_date = _parse_date(request.GET.get("date"), latest)
    if selected_date is None:
        return JsonResponse({"error": "no_data"}, status=503)

    rows = get_constituents(selected_date)
    return JsonResponse({"as_of": selected_date.isoformat(), "rows": rows})


@require_GET
def api_tech100_attribution(request):
    latest = get_latest_trade_date()
    selected_date = _parse_date(request.GET.get("date"), latest)
    if selected_date is None:
        return JsonResponse({"error": "no_data"}, status=503)

    rows = get_contribution(selected_date)
    stats = get_stats(selected_date)
    return JsonResponse(
        {
            "as_of": selected_date.isoformat(),
            "index_return": _format_pct(stats.get("ret_1d") if stats else None),
            "rows": rows,
        }
    )


@require_GET
def api_tech100_stats(request):
    latest = get_latest_trade_date()
    selected_date = _parse_date(request.GET.get("date"), latest)
    if selected_date is None:
        return JsonResponse({"error": "no_data"}, status=503)

    stats = get_stats(selected_date)
    if not stats:
        return JsonResponse({"error": "no_stats", "as_of": selected_date.isoformat()}, status=404)
    return JsonResponse({"as_of": selected_date.isoformat(), "stats": stats})
