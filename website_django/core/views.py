from datetime import datetime

from django.http import JsonResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt


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


def ask2_chat_page(request):
    """Render the AI chat UI page (Ask2 front-end stub)."""
    context = {
        "year": datetime.now().year,
    }
    return render(request, "ask2.html", context)


@csrf_exempt
def ask2_chat_api(request):
    """Stub API endpoint for the Ask2 chat interface.

    For now, this returns a placeholder reply. In future, replace
    the placeholder with a call to the real Ask2 backend.
    """
    if request.method != "POST":
        return JsonResponse({"error": "Method not allowed."}, status=405)

    message = (request.POST.get("message") or "").strip()
    if not message:
        return JsonResponse({"error": "Message is required."}, status=400)

    reply = (
        "This is a placeholder response from SustainaCore Ask2. "
        "In production, this endpoint will route your question to the AI "
        "governance analytics backend and return a grounded answer."
    )

    return JsonResponse({"reply": reply})
