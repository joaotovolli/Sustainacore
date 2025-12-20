from datetime import datetime, date
import csv
import json
from typing import Dict, Iterable, List
import logging

from django.conf import settings
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


def _format_port_weight_whole(value) -> str:
    if value in (None, ""):
        return ""
    try:
        num = float(value)
    except (TypeError, ValueError):
        return ""
    rounded = int(round(num))
    return f"{rounded}%"


def _format_score(value) -> str:
    if value is None or value == "":
        return ""
    try:
        num = float(value)
    except (TypeError, ValueError):
        return ""
    formatted = f"{num:.2f}".rstrip("0").rstrip(".")
    return formatted


def _port_date_to_string(value) -> str:
    parsed = _parse_port_date(value)
    if parsed:
        return parsed.date().isoformat()
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value or "").strip()


def _format_port_date_display(value) -> str:
    parsed = _parse_port_date(value)
    if parsed:
        return parsed.strftime("%b-%Y")
    if isinstance(value, (datetime, date)):
        return datetime.combine(value, datetime.min.time()).strftime("%b-%Y")
    text = str(value or "").strip()
    return text


def _filter_companies(companies: Iterable[Dict], filters: Dict[str, str]) -> List[Dict]:
    filtered: List[Dict] = []
    port_date_filter = (filters.get("port_date") or "").strip()
    sector_filter = (filters.get("sector") or "").strip()
    search_term = (filters.get("q") or filters.get("search") or "").lower().strip()

    for company in companies:
        port_date_value = _port_date_to_string(
            company.get("port_date")
            or company.get("port_date_str")
            or company.get("updated_at")
            or company.get("as_of_date")
        )
        if port_date_filter and port_date_value != port_date_filter:
            continue

        sector_value = str(company.get("sector") or company.get("gics_sector") or "").strip()
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
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    text = str(value).strip()
    if not text:
        return None
    for candidate in (text, text.replace("Z", ""), text.split("T")[0]):
        try:
            parsed = datetime.fromisoformat(candidate)
            if parsed.tzinfo:
                parsed = parsed.replace(tzinfo=None)
            return parsed
        except (TypeError, ValueError):
            continue
    return None


def _safe_float(value, default=None):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_weight_value(value, default=None):
    """Parse weight values that may arrive as raw numbers or strings with a % suffix."""
    if isinstance(value, str):
        candidate = value.strip().rstrip("%").strip()
    else:
        candidate = value
    return _safe_float(candidate, default)


def _lower_key_map(row: Dict) -> Dict[str, any]:
    return {str(key).lower(): value for key, value in row.items()}


def _get_value(row_map: Dict[str, any], keys) -> any:
    for key in keys:
        if key is None:
            continue
        candidate = row_map.get(str(key).lower())
        if candidate in ("", None):
            continue
        if isinstance(candidate, str):
            candidate_stripped = candidate.strip()
            if candidate_stripped == "":
                continue
            return candidate_stripped
        return candidate
    return None


def _company_history_key(company: Dict) -> str:
    ticker = str(company.get("ticker") or "").strip().lower()
    name = str(company.get("company_name") or company.get("company") or "").strip().lower()
    if ticker and name:
        return f"{ticker}::{name}"
    if ticker:
        return ticker
    if name:
        return name
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
        "aiges_composite",
        "aiges_composite_average",
        "aiges_composite_score",
        "aiges_composite_index",
        "aiges_score",
        "aiges",
        "overall",
    ):
        value = row.get(key)
        if value not in ("", None):
            return value
    return None


def _first_value(row: Dict, keys):
    for key in keys:
        value = row.get(key)
        if value not in ("", None):
            return value
    return None


def _assign_rank_indexes(rows: List[Dict]) -> None:
    """Backfill rank_index per port_date based on AIGES composite if backend omits rank."""
    per_date: Dict[str, List[Dict]] = {}
    for row in rows:
        date_key = row.get("port_date_str") or _port_date_to_string(row.get("port_date")) or "undated"
        score = _safe_float(row.get("aiges_composite"))
        per_date.setdefault(date_key, []).append({"score": score, "row": row})

    for date_key, items in per_date.items():
        items.sort(key=lambda item: (-_safe_float(item["score"], float("-inf")), str(item["row"].get("company_name") or "")))
        for idx, item in enumerate(items, start=1):
            row = item["row"]
            if row.get("rank_index") in ("", None):
                row["rank_index"] = idx


def _normalize_row(raw: Dict) -> Dict:
    # Normalize varied API field names into a canonical TECH100 schema.
    row = dict(raw)
    lower_map = _lower_key_map(row)

    row["company_name"] = _get_value(lower_map, ["company_name", "company"])
    row["ticker"] = _get_value(lower_map, ["ticker", "symbol"])

    sector_value = _get_value(lower_map, ["gics_sector", "gics_sector_name", "sector", "industry_group"])
    row["gics_sector"] = sector_value
    row["sector"] = sector_value or row.get("sector")

    raw_port_date = _get_value(lower_map, ["port_date", "rebalance_date", "as_of_date", "updated_at", "dt", "date"])
    parsed_port_date = _parse_port_date(raw_port_date)
    row["port_date"] = parsed_port_date
    row["port_date_str"] = parsed_port_date.date().isoformat() if parsed_port_date else _port_date_to_string(raw_port_date)

    rank_val = _get_value(lower_map, ["rank_index", "rank", "index_rank", "rnk"])
    row["rank_index"] = _safe_float(rank_val, rank_val)
    port_weight_value = _parse_weight_value(
        _get_value(
            lower_map,
            ["port_weight", "weight", "portfolio_weight", "index_weight", "port_wt", "weight_percent"],
        )
    )
    row["port_weight"] = port_weight_value
    row["weight"] = _parse_weight_value(_get_value(lower_map, ["weight"]), port_weight_value)

    row["transparency"] = _safe_float(
        _get_value(lower_map, ["transparency", "transparency_score", "trs"]), _get_value(lower_map, ["transparency"])
    )
    row["ethical_principles"] = _safe_float(
        _get_value(lower_map, ["ethical_principles", "ethics", "ethics_score", "ethical_score"]),
        _get_value(lower_map, ["ethical_principles"]),
    )
    row["governance_structure"] = _safe_float(
        _get_value(lower_map, ["governance_structure", "governance", "accountability", "accountability_score", "gov_score"])
    )
    row["regulatory_alignment"] = _safe_float(
        _get_value(lower_map, ["regulatory_alignment", "regulation_alignment", "regulatory", "regulation", "regulatory_score"])
    )
    row["stakeholder_engagement"] = _safe_float(
        _get_value(lower_map, ["stakeholder_engagement", "stakeholder", "stakeholder_score"])
    )
    row["aiges_composite"] = _safe_float(
        _get_value(
            lower_map,
            [
                "aiges_composite",
                "aiges_composite_average",
                "aiges_composite_score",
                "aiges_composite_index",
                "aiges_score",
                "aiges",
                "overall",
                "composite",
            ],
        )
    )
    row["aiges_composite_average"] = row["aiges_composite"]
    row["summary"] = _get_value(
        lower_map, ["summary", "company_summary", "aiges_summary", "overall_summary", "description"]
    )
    return row


def home(request):
    tech100_response = fetch_tech100()
    news_response = fetch_news()

    raw_tech100_items = tech100_response.get("items", []) or []
    tech100_items = [_normalize_row(item) for item in raw_tech100_items if isinstance(item, dict)]
    tech100_preview = [
        {
            "company": item.get("company_name") or item.get("company"),
            "sector": item.get("sector") or item.get("gics_sector"),
            "region": item.get("region"),
            "overall": item.get("aiges_composite") or item.get("overall"),
        }
        for item in tech100_items[:3]
    ]
    news_items = news_response.get("items", [])

    context = {
        "year": datetime.now().year,
        "tech100_preview": tech100_preview,
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

    tech100_response = fetch_tech100(
        port_date=filters["port_date"] or None,
        sector=filters["sector"] or None,
        search=search_term or None,
    )
    raw_companies = tech100_response.get("items", []) or []
    companies = [_normalize_row(c) for c in raw_companies if isinstance(c, dict)]
    _assign_rank_indexes(companies)

    def history_sort(item):
        parsed_date = item.get("port_date") or _parse_port_date(item.get("port_date_str"))
        return parsed_date or datetime.min

    def matches_filters(rows: List[Dict]) -> bool:
        if filters["port_date"]:
            if not any((_port_date_to_string(r.get("port_date") or r.get("port_date_str")) == filters["port_date"]) for r in rows):
                return False
        if filters["sector"]:
            if not any(((r.get("sector") or r.get("gics_sector")) == filters["sector"]) for r in rows):
                return False
        if filters["search"]:
            term = filters["search"].lower()
            row = rows[-1]
            name = (row.get("company_name") or "").lower()
            ticker = (row.get("ticker") or "").lower()
            if term not in name and term not in ticker:
                return False
        return True

    grouped_companies: Dict[str, List[Dict]] = {}
    for idx, row in enumerate(companies):
        key = _company_history_key(row) or f"row-{idx}"
        grouped_companies.setdefault(key, []).append(row)

    display_companies: List[Dict] = []
    for rows in grouped_companies.values():
        rows_sorted = sorted(rows, key=history_sort)
        if not matches_filters(rows_sorted):
            continue
        latest = rows_sorted[-1]
        summary_value = next((r.get("summary") for r in reversed(rows_sorted) if r.get("summary")), "")
        weight_value = _safe_float(latest.get("weight"), None)
        port_weight = _safe_float(latest.get("port_weight"), None)
        if weight_value is None:
            weight_value = port_weight
        formatted_port_weight = _format_port_weight(weight_value) or None
        formatted_port_weight_whole = _format_port_weight_whole(weight_value) or None
        port_date_display = _format_port_date_display(latest.get("port_date") or latest.get("port_date_str"))
        history_list = [
            {
                "port_date": entry.get("port_date"),
                "port_date_str": entry.get("port_date_str"),
                "transparency": entry.get("transparency"),
                "ethical_principles": entry.get("ethical_principles"),
                "governance_structure": entry.get("governance_structure"),
                "regulatory_alignment": entry.get("regulatory_alignment"),
                "stakeholder_engagement": entry.get("stakeholder_engagement"),
                "aiges_composite": entry.get("aiges_composite") or _extract_aiges_score(entry),
            }
            for entry in rows_sorted
        ]
        display_companies.append(
            {
                "company_name": latest.get("company_name"),
                "ticker": latest.get("ticker"),
                "port_date": latest.get("port_date"),
                "port_date_str": latest.get("port_date_str"),
                "port_date_display": port_date_display,
                "rank_index": latest.get("rank_index"),
                "gics_sector": latest.get("gics_sector"),
                "sector": latest.get("sector"),
                "summary": summary_value,
                "transparency": latest.get("transparency"),
                "ethical_principles": latest.get("ethical_principles"),
                "governance_structure": latest.get("governance_structure"),
                "regulatory_alignment": latest.get("regulatory_alignment"),
                "stakeholder_engagement": latest.get("stakeholder_engagement"),
                "aiges_composite": latest.get("aiges_composite") or _extract_aiges_score(latest),
                "port_weight": port_weight,
                "weight": weight_value,
                "port_weight_display": formatted_port_weight,
                "weight_display": formatted_port_weight,
                "weight_display_whole": formatted_port_weight_whole,
                "history": history_list,
            }
        )

    def company_sort(item: Dict):
        parsed_date = item.get("port_date") or _parse_port_date(item.get("port_date_str"))
        date_sort = -(parsed_date.timestamp()) if parsed_date else float("inf")
        rank_sort = _safe_float(item.get("rank_index"), float("inf"))
        name_sort = (item.get("company_name") or "").lower()
        return (date_sort, rank_sort, name_sort)

    display_companies = sorted(display_companies, key=company_sort)
    if display_companies:
        sample = display_companies[0]
        logger.info(
            "TECH100 sample company: name=%s ticker=%s port_date=%s rank=%s aiges=%s history_len=%s",
            sample.get("company_name"),
            sample.get("ticker"),
            sample.get("port_date_str") or sample.get("port_date"),
            sample.get("rank_index"),
            sample.get("aiges_composite"),
            len(sample.get("history") or []),
        )

    port_date_options = sorted(
        {item.get("port_date_str") for item in companies if item.get("port_date_str")}, reverse=True
    )
    sector_options = sorted({item.get("sector") for item in companies if item.get("sector")})

    context = {
        "year": datetime.now().year,
        "companies": display_companies,
        "all_companies": companies,
        "tech100_error": tech100_response.get("error"),
        "port_date_options": port_date_options,
        "sector_options": sector_options,
        "filters": filters,
        "visible_count": len(display_companies),
        "total_count": len(grouped_companies),
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
        "WEIGHT",
        "COMPANY_NAME",
        "TICKER",
        "GICS_SECTOR",
        "AIGES_COMPOSITE_AVERAGE",
        "TRANSPARENCY",
        "ETHICAL_PRINCIPLES",
        "GOVERNANCE_STRUCTURE",
        "REGULATORY_ALIGNMENT",
        "STAKEHOLDER_ENGAGEMENT",
        "SUMMARY",
    ]

    writer = csv.writer(response)
    writer.writerow(headers)

    for company in companies:
        weight_value = _safe_float(company.get("weight"), None)
        if weight_value is None:
            weight_value = _safe_float(company.get("port_weight"), None)

        writer.writerow(
            [
                company.get("port_date") or "",
                company.get("rank_index") or "",
                _format_port_weight(weight_value),
                company.get("company_name") or "",
                company.get("ticker") or "",
                (company.get("gics_sector") or company.get("sector") or ""),
                _format_score(company.get("aiges_composite_average")),
                _format_score(company.get("transparency")),
                _format_score(company.get("ethical_principles")),
                _format_score(company.get("governance_structure")),
                _format_score(company.get("regulatory_alignment")),
                _format_score(company.get("stakeholder_engagement")),
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

    news_structured = []
    for item in news_items:
        headline = (item.get("title") or "").strip()
        url = (item.get("url") or "").strip()
        if not headline or not url:
            continue
        article = {
            "@context": "https://schema.org",
            "@type": "NewsArticle",
            "headline": headline,
            "url": url,
            "publisher": {
                "@type": "Organization",
                "name": (item.get("source") or "SustainaCore").strip(),
            },
            "datePublished": item.get("published_at") or None,
            "description": (item.get("summary") or "").strip() or None,
        }
        cleaned = {key: value for key, value in article.items() if value is not None}
        news_structured.append(cleaned)

    context = {
        "year": datetime.now().year,
        "articles": news_items,
        "news_error": news_response.get("error"),
        "news_meta": news_response.get("meta", {}),
        "news_json_ld": json.dumps(news_structured, ensure_ascii=True) if news_structured else "",
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


def robots_txt(request):
    lines = [
        "User-agent: *",
        "Allow: /",
        "Disallow: /admin/",
        "Disallow: /news/admin/",
        "Disallow: /api/",
        "Disallow: /ask2/api/",
        f"Sitemap: {settings.SITE_URL}/sitemap.xml",
        "",
    ]
    return HttpResponse("\n".join(lines), content_type="text/plain")
