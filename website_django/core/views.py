from django.shortcuts import render
from datetime import datetime


def home(request):
    current_year = datetime.now().year
    sample_companies = [
        {"name": "OpenAI", "score": "82", "focus": "Transparency & safety"},
        {"name": "DeepMind", "score": "79", "focus": "Research governance"},
        {"name": "Anthropic", "score": "77", "focus": "Constitutional AI"},
        {"name": "Microsoft", "score": "73", "focus": "Responsible AI program"},
        {"name": "Google", "score": "71", "focus": "AI principles rollout"},
        {"name": "Meta", "score": "66", "focus": "Open source disclosures"},
    ]
    return render(
        request,
        "home.html",
        {
            "current_year": current_year,
            "sample_companies": sample_companies,
        },
    )


def lab(request):
    current_year = datetime.now().year
    return render(request, "lab.html", {"current_year": current_year})


def methodology(request):
    current_year = datetime.now().year
    return render(request, "methodology.html", {"current_year": current_year})


def privacy(request):
    current_year = datetime.now().year
    return render(request, "privacy.html", {"current_year": current_year})
