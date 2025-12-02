from datetime import datetime

from django.shortcuts import render


def _sample_tech100():
    return [
        {
            "company": "Aurora Tech",
            "sector": "AI Infrastructure",
            "region": "North America",
            "overall": 84,
            "transparency": 88,
            "accountability": 80,
            "updated_at": "2025-01-11",
        },
        {
            "company": "Northstar Systems",
            "sector": "Cloud Platforms",
            "region": "Europe",
            "overall": 79,
            "transparency": 81,
            "accountability": 77,
            "updated_at": "2025-01-04",
        },
        {
            "company": "CivicAI Labs",
            "sector": "Public Sector AI",
            "region": "North America",
            "overall": 73,
            "transparency": 75,
            "accountability": 71,
            "updated_at": "2024-12-20",
        },
        {
            "company": "Helix Robotics",
            "sector": "Autonomy & Robotics",
            "region": "Asia-Pacific",
            "overall": 70,
            "transparency": 72,
            "accountability": 68,
            "updated_at": "2024-12-10",
        },
        {
            "company": "SignalForge",
            "sector": "Cyber & Safety",
            "region": "North America",
            "overall": 68,
            "transparency": 70,
            "accountability": 67,
            "updated_at": "2024-12-02",
        },
    ]


def _sample_news():
    return [
        {
            "title": "EU releases updated AI governance guidance for high-risk systems",
            "source": "RegWatch Europe",
            "published_at": "2025-01-15T09:00:00Z",
            "summary": "New rules call for transparent model cards, independent audits, and clearer incident response plans.",
            "url": "https://example.com/eu-guidance",
            "tags": ["AI Governance", "Regulation"],
        },
        {
            "title": "Tech100 firms expand transparency reporting ahead of 2025 cycle",
            "source": "SustainaCore Insights",
            "published_at": "2025-01-12T14:30:00Z",
            "summary": "A majority of the TECH100 cohort now publish annual AI responsibility updates with board oversight details.",
            "url": "https://example.com/tech100-transparency",
            "tags": ["Transparency", "TECH100"],
        },
        {
            "title": "Global bank pilots AI ethics dashboard for risk committees",
            "source": "Financial Ledger",
            "published_at": "2025-01-08T08:10:00Z",
            "summary": "The dashboard aligns AI model risk controls with existing operational risk frameworks and regulatory filings.",
            "url": "https://example.com/bank-ethics-dashboard",
            "tags": ["Governance", "Risk"],
        },
        {
            "title": "Pacific regulators propose accountability checkpoints for frontier models",
            "source": "APAC Policy",
            "published_at": "2024-12-18T18:00:00Z",
            "summary": "Consultation outlines board certification, third-party assurance, and public reporting requirements.",
            "url": "https://example.com/frontier-checkpoints",
            "tags": ["Accountability", "Policy"],
        },
    ]


def home(request):
    tech100_items = _sample_tech100()
    news_items = _sample_news()

    context = {
        "year": datetime.now().year,
        "tech100_preview": tech100_items[:3],
        "news_preview": news_items[:3],
    }
    return render(request, "home.html", context)


def tech100(request):
    context = {
        "year": datetime.now().year,
        "companies": _sample_tech100(),
    }
    return render(request, "tech100.html", context)


def news(request):
    context = {
        "year": datetime.now().year,
        "articles": _sample_news(),
    }
    return render(request, "news.html", context)
from datetime import datetime

from django.shortcuts import render

