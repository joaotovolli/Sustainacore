from importlib import util
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


_module_path = Path(__file__).resolve().parents[1] / "app" / "retrieval" / "app.py"
_spec = util.spec_from_file_location("retrieval_app", _module_path)
assert _spec and _spec.loader
_module = util.module_from_spec(_spec)
_spec.loader.exec_module(_module)


client = TestClient(_module.app)


def test_smalltalk_routing_uses_gemini(monkeypatch):
    prompts: list[str] = []

    def fake_gemini(prompt: str, *, timeout=_module.GEMINI_TIMEOUT, model=None):
        prompts.append(prompt)
        return "Hello from Gemini!"

    def fail_search(query: str, k: int):  # pragma: no cover - guard against accidental calls
        raise AssertionError("search_vectors should not run for small talk")

    monkeypatch.setattr(_module, "gemini_call", fake_gemini)
    monkeypatch.setattr(_module, "search_vectors", fail_search)

    response = client.get("/ask2", params={"q": "hello there"})
    payload = response.json()

    assert payload["meta"]["routing"] == "smalltalk"
    assert payload["meta"]["gemini_used"] is True
    assert payload["meta"]["top_score"] is None
    assert payload["sources"] == []
    assert payload["answer"] == "Hello from Gemini!"
    assert prompts and "hello there" in prompts[0]


def test_no_vectors_off_topic(monkeypatch):
    monkeypatch.setattr(_module, "search_vectors", lambda query, k: [])

    def fake_gemini(prompt: str, *, timeout=_module.GEMINI_TIMEOUT, model=None):
        raise AssertionError("gemini_call should not be used when no vectors are returned")

    monkeypatch.setattr(_module, "gemini_call", fake_gemini)

    response = client.get("/ask2", params={"q": "Tell me about cooking."})
    payload = response.json()

    assert payload["meta"]["routing"] == "no_hit"
    assert payload["meta"]["gemini_used"] is False
    assert payload["sources"] == []
    text = payload["answer"].lower()
    assert "sustainacore" in text
    assert "tech100" in text
    assert "report" in text


def test_low_confidence_path(monkeypatch):
    hits = [
        {
            "id": "doc-1",
            "title": "Climate Policy Overview",
            "url": "https://example.com/policy",
            "score": 0.4,
            "snippet": "Policy summary.",
        }
    ]

    monkeypatch.setattr(_module, "search_vectors", lambda query, k: [hit.copy() for hit in hits])

    def fake_gemini(prompt: str, *, timeout=_module.GEMINI_TIMEOUT, model=None):
        raise AssertionError("gemini_call should not run for low confidence routing")

    monkeypatch.setattr(_module, "gemini_call", fake_gemini)

    response = client.get("/ask2", params={"q": "What is the climate policy?"})
    payload = response.json()

    assert payload["meta"]["routing"] == "low_conf"
    assert payload["meta"]["gemini_used"] is False
    assert payload["meta"]["top_score"] == pytest.approx(0.4)
    assert payload["sources"]
    assert "climate policy overview" in payload["sources"][0].lower()
    assert "inconclusive" in payload["answer"].lower()
    assert "organisation" in payload["answer"].lower()


def test_high_confidence_uses_gemini(monkeypatch):
    hits = [
        {
            "id": "doc-1",
            "title": "Impact Report 2024",
            "url": "https://example.com/impact",
            "score": 0.78,
            "snippet": "Details about Sustainacore’s 2024 impact report.",
        },
        {
            "id": "doc-2",
            "title": "ESG Metrics Overview",
            "url": "https://example.com/metrics",
            "score": 0.72,
            "snippet": "Key ESG metrics tracked during 2024.",
        },
    ]

    monkeypatch.setattr(_module, "search_vectors", lambda query, k: [hit.copy() for hit in hits])

    prompts: list[str] = []

    def fake_gemini(prompt: str, *, timeout=_module.GEMINI_TIMEOUT, model=None):
        prompts.append(prompt)
        return "The programme delivered measurable progress. [Source 1] [Source 2]"

    monkeypatch.setattr(_module, "gemini_call", fake_gemini)

    response = client.get("/ask2", params={"q": "Summarise the ESG impact for 2024."})
    payload = response.json()

    assert payload["meta"]["routing"] == "high_conf"
    assert payload["meta"]["gemini_used"] is True
    assert payload["meta"]["top_score"] == pytest.approx(0.78)
    assert len(payload["sources"]) == 2
    assert "[source 1]" in payload["answer"].lower()
    assert prompts and "Passages" in prompts[0]

