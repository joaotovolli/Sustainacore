from __future__ import annotations

import datetime as dt
from typing import Optional

from django.http import JsonResponse
from django.shortcuts import render

from ai_reg import data as ai_reg_data


def _parse_as_of(value: Optional[str]) -> Optional[dt.date]:
    if not value:
        return None
    try:
        return dt.date.fromisoformat(value)
    except ValueError:
        return None


def ai_regulation_page(request):
    try:
        as_of_dates = ai_reg_data.fetch_as_of_dates()
    except ai_reg_data.AiRegDataError:
        as_of_dates = []
    context = {
        "as_of_dates": as_of_dates,
        "latest_as_of": as_of_dates[0] if as_of_dates else None,
    }
    return render(request, "ai_reg/ai_regulation.html", context)


def ai_reg_as_of_dates(request):
    try:
        return JsonResponse({"as_of_dates": ai_reg_data.fetch_as_of_dates()})
    except ai_reg_data.AiRegDataError:
        return JsonResponse({"error": "data_unavailable"}, status=503)


def ai_reg_heatmap(request):
    as_of = _parse_as_of(request.GET.get("as_of"))
    if not as_of:
        return JsonResponse({"error": "invalid_as_of"}, status=400)
    try:
        data = ai_reg_data.fetch_heatmap(as_of)
    except ai_reg_data.AiRegDataError:
        return JsonResponse({"error": "data_unavailable"}, status=503)
    return JsonResponse({"as_of": as_of.isoformat(), "jurisdictions": data})


def ai_reg_jurisdiction(request, iso2: str):
    as_of = _parse_as_of(request.GET.get("as_of"))
    if not as_of:
        return JsonResponse({"error": "invalid_as_of"}, status=400)
    iso2_norm = iso2.strip().upper()
    try:
        summary = ai_reg_data.fetch_jurisdiction_summary(iso2_norm, as_of)
    except ai_reg_data.AiRegDataError:
        return JsonResponse({"error": "data_unavailable"}, status=503)
    if summary is None:
        return JsonResponse({"error": "jurisdiction_not_found"}, status=404)
    try:
        instruments = ai_reg_data.fetch_jurisdiction_instruments(iso2_norm, as_of)
        timeline = ai_reg_data.fetch_jurisdiction_timeline(iso2_norm, as_of)
        sources = ai_reg_data.fetch_jurisdiction_sources(iso2_norm, as_of)
    except ai_reg_data.AiRegDataError:
        return JsonResponse({"error": "data_unavailable"}, status=503)
    payload = {
        "as_of": as_of.isoformat(),
        "jurisdiction": summary,
        "instruments": instruments,
        "milestones": timeline,
        "sources": sources,
    }
    return JsonResponse(payload)


def ai_reg_jurisdiction_instruments(request, iso2: str):
    as_of = _parse_as_of(request.GET.get("as_of"))
    if not as_of:
        return JsonResponse({"error": "invalid_as_of"}, status=400)
    iso2_norm = iso2.strip().upper()
    try:
        instruments = ai_reg_data.fetch_jurisdiction_instruments(iso2_norm, as_of)
    except ai_reg_data.AiRegDataError:
        return JsonResponse({"error": "data_unavailable"}, status=503)
    return JsonResponse(
        {"as_of": as_of.isoformat(), "jurisdiction": iso2_norm, "instruments": instruments}
    )


def ai_reg_jurisdiction_timeline(request, iso2: str):
    as_of = _parse_as_of(request.GET.get("as_of"))
    if not as_of:
        return JsonResponse({"error": "invalid_as_of"}, status=400)
    iso2_norm = iso2.strip().upper()
    try:
        timeline = ai_reg_data.fetch_jurisdiction_timeline(iso2_norm, as_of)
    except ai_reg_data.AiRegDataError:
        return JsonResponse({"error": "data_unavailable"}, status=503)
    return JsonResponse(
        {"as_of": as_of.isoformat(), "jurisdiction": iso2_norm, "milestones": timeline}
    )
