from datetime import datetime
import csv
from typing import Dict, Iterable, List

from django.contrib.auth.decorators import login_required
from django.http import HttpResponse
from django.shortcuts import render

from core.api_client import create_news_item_admin, fetch_news, fetch_tech100


def _format_port_weight(value) -> str:
    if value is None or value == "":
        return ""
    try:
        num = float(value)
    except (TypeError, ValueError):
        return str(value)
    decimals = 1 if abs(num) >= 10 else 2
    return f"{num:.{decimals}f}%"


def _format_score(value) -> str:
    if value is None or value == "":
        return ""
    try:
        num = float(value)
    except (TypeError, ValueError):
        return str(value)
    return f"{num:.2f}"


def _filter_companies(companies: Iterable[Dict], filters: Dict[str, str]) -> List[Dict]:
    filtered: List[Dict] = []
    search_term = (filters.get("q") or filters.get("search") or "").lower()

def _filter_companies(companies: Iterable[Dict], filters: Dict[str, str]) -> List[Dict]:
    filtered: List[Dict] = []
    search_term = filters.get("search", "").lower()
    for company in companies:
        port_date = (company.get("port_date") or "").strip()
        if filters.get("port_date") and port_date != filters["port_date"]:
            continue

        sector_value = (company.get("gics_sector") or company.get("sector") or "").strip()
        sector_value = company.get("gics_sector") or company.get("sector") or ""
        if filters.get("sector") and sector_value != filters["sector"]:
            continue

        if search_term:
            company_name = (company.get("company_name") or "").lower()
            ticker = (company.get("ticker") or "").lower()
            if search_term not in company_name and search_term not in ticker:
                continue

        filtered.append(company)

    return filtered


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
    filters = {
        "port_date": (request.GET.get("port_date") or "").strip(),
        "sector": (request.GET.get("sector") or "").strip(),
        "q": (request.GET.get("q") or request.GET.get("search") or "").strip(),
    }

    tech100_response = fetch_tech100()
        "search": (request.GET.get("search") or "").strip(),
    }

    tech100_response = fetch_tech100(
        port_date=filters["port_date"] or None,
        sector=filters["sector"] or None,
        search=filters["search"] or None,
    )

    companies = tech100_response.get("items", [])
    filtered_companies = _filter_companies(companies, filters)
    port_date_options = sorted(
        {item.get("port_date") for item in companies if item.get("port_date")}, reverse=True
    )
    sector_options = sorted(
        {
            item.get("gics_sector") or item.get("sector")
            for item in companies
            if item.get("gics_sector") or item.get("sector")
        }
    )
    sector_options = sorted({item.get("gics_sector") for item in companies if item.get("gics_sector")})

    context = {
        "year": datetime.now().year,
        "companies": filtered_companies,
        "all_companies": companies,
        "tech100_error": tech100_response.get("error"),
        "port_date_options": port_date_options,
        "sector_options": sector_options,
        "filters": filters,
        "visible_count": len(filtered_companies),
        "total_count": len(companies),
    }
    return render(request, "tech100.html", context)


def tech100_export(request):
    filters = {
        "port_date": (request.GET.get("port_date") or "").strip(),
        "sector": (request.GET.get("sector") or "").strip(),
        "q": (request.GET.get("q") or request.GET.get("search") or "").strip(),
    }

    tech100_response = fetch_tech100()

    if tech100_response.get("error"):
        return HttpResponse("Unable to export TECH100 data right now.", status=502)
        "search": (request.GET.get("search") or "").strip(),
    }

    tech100_response = fetch_tech100(
        port_date=filters["port_date"] or None,
        sector=filters["sector"] or None,
        search=filters["search"] or None,
    )

    companies = _filter_companies(tech100_response.get("items", []), filters)

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
        "SOURCE_LINKS",
    ]

    writer = csv.writer(response)
    writer.writerow(headers)
    for company in companies:
        writer.writerow(
            [
                (company.get("port_date") or ""),
                (company.get("rank_index") or ""),
                (company.get("company_name") or ""),
                (company.get("ticker") or ""),
                _format_port_weight(company.get("port_weight")),
                (company.get("gics_sector") or company.get("sector") or ""),
                _format_score(company.get("transparency")),
                _format_score(company.get("ethical_principles")),
                _format_score(company.get("governance_structure")),
                _format_score(company.get("regulatory_alignment")),
                _format_score(company.get("stakeholder_engagement")),
                _format_score(company.get("aiges_composite_average")),
                (company.get("summary") or ""),
            ]
        )
        writer.writerow([company.get(key.lower()) or company.get(key) or "" for key in headers])

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

