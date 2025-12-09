from datetime import datetime
import csv
from typing import Dict, Iterable, List
import logging

from django.contrib.auth.decorators import login_required
from django.http import HttpResponse
from django.shortcuts import render

from core.api_client import create_news_item_admin, fetch_news, fetch_tech100


logger = logging.getLogger(__name__)


def _format_port_weight(value) -> str:
    if value is None or value == "":
        return ""
    try:
        num = float(value)
    except (TypeError, ValueError):
        return ""
    decimals = 1 if abs(num) >= 10 else 2
    return f"{num:.{decimals}f}%"


def _format_score(value) -> str:
    if value is None or value == "":
        return ""
    try:
        num = float(value)
    except (TypeError, ValueError):
        return ""
    formatted = f"{num:.2f}".rstrip("0").rstrip(".")
    return formatted


def _filter_companies(companies: Iterable[Dict], filters: Dict[str, str]) -> List[Dict]:
    filtered: List[Dict] = []
    port_date_filter = (filters.get("port_date") or "").strip()
    sector_filter = (filters.get("sector") or "").strip()
    search_term = (filters.get("q") or filters.get("search") or "").lower().strip()

    for company in companies:
        port_date_value = str(company.get("port_date") or "").strip()
        if port_date_filter and port_date_value != port_date_filter:
            continue

        sector_value = str(company.get("gics_sector") or company.get("sector") or "").strip()
        if sector_filter and sector_value != sector_filter:
            continue

        if search_term:
            company_name = (company.get("company_name") or "").lower()
            ticker = (company.get("ticker") or "").lower()
            if search_term not in company_name and search_term not in ticker:
                continue

        filtered.append(company)

    return filtered


def _parse_port_date(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None


def _company_history_key(company: Dict) -> str:
    for key_field in ("ticker", "company_name"):
        key_value = str(company.get(key_field) or "").strip()
        if key_value:
            return key_value.lower()
    return ""


def _build_company_history(companies: Iterable[Dict]) -> Dict[str, List[Dict]]:
    history: Dict[str, List[Dict]] = {}
    for company in companies:
        key = _company_history_key(company)
        if not key:
            continue
        history.setdefault(key, []).append(dict(company))

    def _history_sort(item: Dict):
        parsed_date = _parse_port_date(item.get("port_date"))
        return parsed_date or datetime.min

    for entries in history.values():
        entries.sort(key=_history_sort, reverse=True)

    return history


def _extract_aiges_score(row: Dict):
    for key in (
        "aiges_composite_average",
        "aiges_composite",
        "aiges_composite_score",
        "aiges_composite_index",
    ):
        value = row.get(key)
        if value not in ("", None):
            return value
    return None


def home(request):
    tech100_response = fetch_tech100()
    news_response = fetch_news()

    tech100_items = tech100_response.get("items", [])
    news_items = news_response.get("items", [])

    context = {
        "year": datetime.now().year,
        "tech100_preview": tech100_items[:3],
        "news_preview": news_items[:3],
    }
    return render(request, "home.html", context)


def tech100(request):
    search_term = (request.GET.get("q") or request.GET.get("search") or "").strip()
    filters = {
        "port_date": (request.GET.get("port_date") or "").strip(),
        "sector": (request.GET.get("sector") or "").strip(),
        "q": search_term,
        "search": search_term,
    }

    tech100_response = fetch_tech100()
    companies = tech100_response.get("items", []) or []
    for company in companies:
        if "sector" not in company:
            company["sector"] = company.get("gics_sector")
        if "gics_sector" not in company:
            company["gics_sector"] = company.get("sector")

    filtered_rows = _filter_companies(companies, filters)

    def sort_key(item):
        parsed_date = _parse_port_date(item.get("port_date"))
        date_sort = -(parsed_date.timestamp()) if parsed_date else float("inf")
        rank_sort = item.get("rank_index")
        try:
            rank_sort = float(rank_sort)
        except (TypeError, ValueError):
            rank_sort = float("inf")
        return (date_sort, rank_sort)

    grouped_companies: Dict[str, List[Dict]] = {}
    for idx, row in enumerate(filtered_rows):
        key = _company_history_key(row) or f"row-{idx}"
        grouped_companies.setdefault(key, []).append(row)

    display_companies: List[Dict] = []
    for rows in grouped_companies.values():
        sorted_rows = sorted(rows, key=sort_key)
        latest = sorted_rows[0]
        aiges_score = _extract_aiges_score(latest)
        company_summary = {
            "company_name": latest.get("company_name"),
            "ticker": latest.get("ticker"),
            "port_date": latest.get("port_date"),
            "rank_index": latest.get("rank_index"),
            "gics_sector": latest.get("gics_sector") or latest.get("sector"),
            "sector": latest.get("sector") or latest.get("gics_sector"),
            "summary": latest.get("summary"),
            "aiges_composite_average": aiges_score,
        }
        company_summary["history"] = [
            {
                "port_date": entry.get("port_date"),
                "transparency": entry.get("transparency"),
                "ethical_principles": entry.get("ethical_principles"),
                "governance_structure": entry.get("governance_structure"),
                "regulatory_alignment": entry.get("regulatory_alignment"),
                "stakeholder_engagement": entry.get("stakeholder_engagement"),
                "aiges_composite_average": _extract_aiges_score(entry),
            }
            for entry in sorted_rows
        ]
        display_companies.append(company_summary)

    display_companies = sorted(display_companies, key=sort_key)
    if display_companies:
        sample = display_companies[0]
        logger.info(
            "TECH100 sample company: name=%s ticker=%s port_date=%s rank=%s aiges=%s history_len=%s",
            sample.get("company_name"),
            sample.get("ticker"),
            sample.get("port_date"),
            sample.get("rank_index"),
            sample.get("aiges_composite_average"),
            len(sample.get("history") or []),
        )

    port_date_options = sorted(
        {item.get("port_date") for item in companies if item.get("port_date")}, reverse=True
    )
    sector_options = sorted({item.get("gics_sector") for item in companies if item.get("gics_sector")})

    context = {
        "year": datetime.now().year,
        "companies": display_companies,
        "all_companies": companies,
        "tech100_error": tech100_response.get("error"),
        "port_date_options": port_date_options,
        "sector_options": sector_options,
        "filters": filters,
        "visible_count": len(display_companies),
        "total_count": len({(_company_history_key(c) or f"row-{idx}") for idx, c in enumerate(companies)}),
    }
    return render(request, "tech100.html", context)


def tech100_export(request):
    search_term = (request.GET.get("q") or request.GET.get("search") or "").strip()
    filters = {
        "port_date": (request.GET.get("port_date") or "").strip(),
        "sector": (request.GET.get("sector") or "").strip(),
        "q": search_term,
        "search": search_term,
    }

    tech100_response = fetch_tech100()
    if tech100_response.get("error"):
        return HttpResponse("Unable to export TECH100 data right now.", status=502)

    companies = _filter_companies(tech100_response.get("items", []) or [], filters)

    response = HttpResponse(content_type="text/csv")
    response["Content-Disposition"] = "attachment; filename=tech100.csv"

    headers = [
        "PORT_DATE",
        "RANK_INDEX",
        "COMPANY_NAME",
        "TICKER",
        "PORT_WEIGHT",
        "GICS_SECTOR",
        "TRANSPARENCY",
        "ETHICAL_PRINCIPLES",
        "GOVERNANCE_STRUCTURE",
        "REGULATORY_ALIGNMENT",
        "STAKEHOLDER_ENGAGEMENT",
        "AIGES_COMPOSITE_AVERAGE",
        "SUMMARY",
    ]

    writer = csv.writer(response)
    writer.writerow(headers)

    for company in companies:
        writer.writerow(
            [
                company.get("port_date") or "",
                company.get("rank_index") or "",
                company.get("company_name") or "",
                company.get("ticker") or "",
                _format_port_weight(company.get("port_weight")),
                (company.get("gics_sector") or company.get("sector") or ""),
                _format_score(company.get("transparency")),
                _format_score(company.get("ethical_principles")),
                _format_score(company.get("governance_structure")),
                _format_score(company.get("regulatory_alignment")),
                _format_score(company.get("stakeholder_engagement")),
                _format_score(company.get("aiges_composite_average")),
                company.get("summary") or "",
            ]
        )

    return response


def news(request):
    raw_date_range = (request.GET.get("date_range") or "all").strip()
    date_range = raw_date_range if raw_date_range in {"all", "7", "30", "90"} else "all"
    filters = {
        "source": (request.GET.get("source") or "").strip(),
        "tag": (request.GET.get("tag") or "").strip(),
        "date_range": date_range,
    }

    date_mapping = {"7": 7, "30": 30, "90": 90}
    days = date_mapping.get(date_range)

    news_response = fetch_news(
        source=filters["source"] or None,
        tag=filters["tag"] or None,
        days=days,
        limit=20,
    )

    news_items = news_response.get("items", [])
    sources = sorted({item.get("source") for item in news_items if item.get("source")})
    tags = sorted({tag for item in news_items for tag in item.get("tags", [])})

    context = {
        "year": datetime.now().year,
        "articles": news_items,
        "news_error": news_response.get("error"),
        "news_meta": news_response.get("meta", {}),
        "filters": filters,
        "source_options": sources,
        "tag_options": tags,
        "date_range_options": [
            {"value": "all", "label": "All time"},
            {"value": "7", "label": "Last 7 days"},
            {"value": "30", "label": "Last 30 days"},
            {"value": "90", "label": "Last 90 days"},
        ],
    }
    return render(request, "news.html", context)


@login_required
def news_admin(request):
    default_form_values = {
        "title": "",
        "url": "",
        "source": "",
        "summary": "",
        "published_at": "",
        "pillar_tags": [],
        "categories": [],
        "tags": [],
        "tickers": [],
    }

    context = {
        "year": datetime.now().year,
        "form_values": default_form_values,
        "created_item": None,
        "admin_error": None,
    }

    if request.method == "POST":
        def parse_list(field_name: str):
            raw_value = (request.POST.get(field_name) or "").strip()
            return [part.strip() for part in raw_value.split(",") if part.strip()]

        form_values = {
            "title": (request.POST.get("title") or "").strip(),
            "url": (request.POST.get("url") or "").strip(),
            "source": (request.POST.get("source") or "").strip(),
            "summary": (request.POST.get("summary") or "").strip(),
            "published_at": (request.POST.get("dt_pub") or "").strip(),
            "pillar_tags": parse_list("pillar_tags"),
            "categories": parse_list("categories"),
            "tags": parse_list("tags"),
            "tickers": parse_list("tickers"),
        }

        context["form_values"] = form_values

        if not form_values["title"] or not form_values["url"]:
            context["admin_error"] = "Title and URL are required."
        else:
            create_response = create_news_item_admin(
                title=form_values["title"],
                url=form_values["url"],
                source=form_values["source"] or None,
                summary=form_values["summary"] or None,
                published_at=form_values["published_at"] or None,
                pillar_tags=form_values["pillar_tags"],
                categories=form_values["categories"],
                tags=form_values["tags"],
                tickers=form_values["tickers"],
            )

            context["created_item"] = create_response.get("item")
            context["admin_error"] = create_response.get("error")

        if context["created_item"]:
            context["form_values"] = default_form_values

    return render(request, "news_admin.html", context)
