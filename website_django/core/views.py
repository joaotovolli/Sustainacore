from datetime import datetime

from django.shortcuts import render


def home(request):
    sample_companies = [
        {"name": "Aurora Tech", "score": 82},
        {"name": "Northstar Systems", "score": 76},
        {"name": "CivicAI Labs", "score": 71},
        {"name": "Helix Robotics", "score": 69},
    ]

    context = {
        "year": datetime.now().year,
        "companies": sample_companies,
    }
    return render(request, "home.html", context)
