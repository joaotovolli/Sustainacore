from __future__ import annotations

import datetime as dt
import logging
import csv
from typing import Optional

from django.http import JsonResponse, HttpResponse
from django.shortcuts import render
from django.views.decorators.cache import cache_page
from django.views.decorators.http import require_GET

from core.tech100_index_data import (
    DrawdownResult,
    get_data_mode,
    get_contribution,
    get_contribution_summary,
    get_constituents,
    get_drawdown_series,
    get_index_levels,
    get_index_returns,
    get_kpis,
    get_latest_rebalance_date,
    get_latest_trade_date,
    get_max_drawdown,
    get_quality_counts,
    get_return_between,
    get_ytd_return,
    get_rolling_vol,
    get_rolling_vol_series,
    get_holdings_with_meta,
    get_sector_breakdown,
    get_stats,
    get_trade_date_bounds,
    get_attribution_table,
)
from core.downloads import require_login_for_download

logger = logging.getLogger(__name__)

PAGE_CACHE_SECONDS = 60

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
@cache_page(PAGE_CACHE_SECONDS)
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
@cache_page(PAGE_CACHE_SECONDS)
def tech100_performance(request):
    data_mode = get_data_mode()
    try:
        latest = get_latest_trade_date()
        if latest is None:
            raise ValueError("No TECH100 trade dates available.")

        range_key = (request.GET.get("range") or "1y").lower()
        if range_key not in {"1m", "3m", "6m", "ytd", "1y", "max"}:
            range_key = "1y"
        start_date = _range_start(latest, range_key)
        mtd_start = dt.date(latest.year, latest.month, 1)
        ytd_start = dt.date(latest.year, 1, 1)

        levels = get_index_levels(start_date, latest)
        drawdowns = get_drawdown_series(start_date, latest)
        vols = get_rolling_vol_series(start_date, latest, window=30)
        kpis = get_kpis(latest)
        vol_30d = get_rolling_vol(latest, window=30)
        drawdown_ytd = get_max_drawdown(ytd_start, latest)
        ret_ytd, _ = get_ytd_return(latest)
        quality_counts = get_quality_counts(latest)
        rebalance_date = get_latest_rebalance_date()

        holdings = get_holdings_with_meta(latest)
        sector_breakdown = get_sector_breakdown(holdings)
        attribution_rows = get_attribution_table(latest, mtd_start, ytd_start)

        top_1d = get_contribution_summary(latest, latest, limit=8, direction="desc")
        worst_1d = get_contribution_summary(latest, latest, limit=8, direction="asc")
        top_mtd = get_contribution_summary(latest, mtd_start, limit=8, direction="desc")
        worst_mtd = get_contribution_summary(latest, mtd_start, limit=8, direction="asc")
        top_ytd = get_contribution_summary(latest, ytd_start, limit=8, direction="desc")
        worst_ytd = get_contribution_summary(latest, ytd_start, limit=8, direction="asc")

        levels_payload = [
            {"date": trade_date.isoformat(), "level": level} for trade_date, level in levels
        ]
        drawdown_payload = [
            {"date": trade_date.isoformat(), "drawdown": value}
            for trade_date, value in drawdowns
        ]
        vol_payload = [
            {"date": trade_date.isoformat(), "vol": value} for trade_date, value in vols
        ]

        context = {
            "data_mode": data_mode,
            "data_error": False,
            "range_key": range_key,
            "latest_date": latest.isoformat(),
            "levels_payload": levels_payload,
            "drawdown_payload": drawdown_payload,
            "vol_payload": vol_payload,
            "levels_count": len(levels_payload),
            "holdings_count": len(holdings),
            "attribution_count": len(attribution_rows),
            "kpis": {
                "level": kpis.get("level"),
                "ret_1d": _format_pct(kpis.get("ret_1d")),
                "ret_mtd": _format_pct(get_return_between(latest, mtd_start)),
                "ret_ytd": _format_pct(ret_ytd),
                "vol_30d": _format_pct(vol_30d),
                "drawdown_ytd": _format_pct(drawdown_ytd.drawdown),
            },
            "quality_counts": quality_counts,
            "rebalance_date": rebalance_date.isoformat() if rebalance_date else None,
            "universe_date": latest.isoformat(),
            "holdings": holdings,
            "sector_breakdown": sector_breakdown,
            "attribution_rows": attribution_rows,
            "top_1d": top_1d,
            "worst_1d": worst_1d,
            "top_mtd": top_mtd,
            "worst_mtd": worst_mtd,
            "top_ytd": top_ytd,
            "worst_ytd": worst_ytd,
        }
    except Exception:
        logger.exception("TECH100 performance page failed.")
        context = {
            "data_mode": data_mode,
            "data_error": True,
            "levels_payload": [],
            "drawdown_payload": [],
            "vol_payload": [],
            "levels_count": 0,
            "holdings_count": 0,
            "attribution_count": 0,
        }

    return render(request, "tech100_performance.html", context)


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
def api_tech100_performance_attribution(request):
    latest = get_latest_trade_date()
    if latest is None:
        return JsonResponse({"error": "no_data"}, status=503)
    range_key = (request.GET.get("range") or "1d").lower()
    if range_key == "mtd":
        start_date = dt.date(latest.year, latest.month, 1)
    elif range_key == "ytd":
        start_date = dt.date(latest.year, 1, 1)
    else:
        start_date = latest

    top = get_contribution_summary(latest, start_date, limit=8, direction="desc")
    worst = get_contribution_summary(latest, start_date, limit=8, direction="asc")
    return JsonResponse(
        {
            "range": range_key,
            "as_of": latest.isoformat(),
            "top": top,
            "worst": worst,
        }
    )


@require_GET
def api_tech100_holdings(request):
    latest = get_latest_trade_date()
    selected_date = _parse_date(request.GET.get("date"), latest)
    if selected_date is None:
        return JsonResponse({"error": "no_data"}, status=503)

    holdings = get_holdings_with_meta(selected_date)
    return JsonResponse({"as_of": selected_date.isoformat(), "rows": holdings})


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


@require_GET
@require_login_for_download
def tech100_index_export(request):
    latest = get_latest_trade_date()
    if latest is None:
        return HttpResponse("No TECH100 data available.", status=503)
    kind = (request.GET.get("kind") or "levels").strip().lower()
    if kind not in {"levels", "constituents"}:
        return HttpResponse("Unknown export type.", status=400)

    response = HttpResponse(content_type="text/csv")
    response["Content-Disposition"] = f"attachment; filename=tech100_{kind}.csv"
    writer = csv.writer(response)

    if kind == "levels":
        min_date, _ = get_trade_date_bounds()
        start_date = min_date or latest
        rows = get_index_levels(start_date, latest)
        writer.writerow(["DATE", "LEVEL"])
        for trade_date, level in rows:
            writer.writerow([trade_date.isoformat(), level])
        return response

    rows = get_constituents(latest)
    writer.writerow(["TICKER", "NAME", "WEIGHT", "RET_1D", "CONTRIBUTION", "PRICE", "QUALITY"])
    for row in rows:
        writer.writerow(
            [
                row.get("ticker") or "",
                row.get("name") or "",
                row.get("weight") or "",
                row.get("ret_1d") or "",
                row.get("contribution") or "",
                row.get("price") or "",
                row.get("quality") or "",
            ]
        )
    return response


@require_GET
@require_login_for_download
def tech100_performance_export(request):
    latest = get_latest_trade_date()
    if latest is None:
        return HttpResponse("No TECH100 data available.", status=503)
    kind = (request.GET.get("kind") or "holdings").strip().lower()
    if kind not in {"holdings", "attribution"}:
        return HttpResponse("Unknown export type.", status=400)

    response = HttpResponse(content_type="text/csv")
    response["Content-Disposition"] = f"attachment; filename=tech100_{kind}.csv"
    writer = csv.writer(response)

    if kind == "holdings":
        rows = get_holdings_with_meta(latest)
        writer.writerow(["TICKER", "NAME", "SECTOR", "WEIGHT", "RET_1D", "CONTRIBUTION"])
        for row in rows:
            writer.writerow(
                [
                    row.get("ticker") or "",
                    row.get("name") or "",
                    row.get("sector") or "",
                    row.get("weight") or "",
                    row.get("ret_1d") or "",
                    row.get("contribution") or "",
                ]
            )
        return response

    mtd_start = dt.date(latest.year, latest.month, 1)
    ytd_start = dt.date(latest.year, 1, 1)
    rows = get_attribution_table(latest, mtd_start, ytd_start)
    writer.writerow(
        [
            "TICKER",
            "NAME",
            "SECTOR",
            "WEIGHT",
            "RET_1D",
            "CONTRIBUTION",
            "CONTRIB_MTD",
            "CONTRIB_YTD",
        ]
    )
    for row in rows:
        writer.writerow(
            [
                row.get("ticker") or "",
                row.get("name") or "",
                row.get("sector") or "",
                row.get("weight") or "",
                row.get("ret_1d") or "",
                row.get("contribution") or "",
                row.get("contrib_mtd") or "",
                row.get("contrib_ytd") or "",
            ]
        )
    return response
