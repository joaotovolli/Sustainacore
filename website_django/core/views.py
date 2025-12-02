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
    news_response = fetch_news()
    context = {
        "year": datetime.now().year,
        "articles": news_response.get("items", []),
        "news_error": news_response.get("error"),
    }
    return render(request, "news.html", context)

