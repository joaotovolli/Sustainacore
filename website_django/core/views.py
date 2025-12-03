from datetime import datetime

from django.shortcuts import render

from core.api_client import fetch_news, fetch_tech100


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

