from datetime import datetime

from django.contrib.auth.decorators import login_required
from django.shortcuts import render

from core.api_client import create_news_item_admin, fetch_news, fetch_tech100


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
    tech100_response = fetch_tech100()
    context = {
        "year": datetime.now().year,
        "companies": tech100_response.get("items", []),
        "tech100_error": tech100_response.get("error"),
    }
    return render(request, "tech100.html", context)


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

