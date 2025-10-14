import importlib
import importlib.machinery
import sys
import types
from pathlib import Path

from retrieval.config import INSUFFICIENT_CONTEXT_MESSAGE

# Ensure the legacy ``app`` module exposes the ``app.rag`` namespace without
# importing heavy runtime dependencies.
pkg_path = Path(__file__).resolve().parents[1] / "app"
module = sys.modules.get("app")
if module is None:
    module = types.ModuleType("app")
    module.__path__ = [str(pkg_path)]  # type: ignore[attr-defined]
    spec = importlib.machinery.ModuleSpec("app", loader=None, is_package=True)
    spec.submodule_search_locations = [str(pkg_path)]
    module.__spec__ = spec  # type: ignore[attr-defined]
    sys.modules["app"] = module
else:
    module.__path__ = [str(pkg_path)]  # type: ignore[attr-defined]
    if getattr(module, "__spec__", None):
        module.__spec__.submodule_search_locations = [str(pkg_path)]  # type: ignore[attr-defined]

routing = importlib.import_module("app.rag.routing")


def _stub_vector(results):
    def _inner(_query, _k):
        return results

    return _inner


def _stub_gemini(response):
    def _inner(_prompt, _timeout=None, _model=None):
        return response

    return _inner


def test_smalltalk_returns_static_greeting():
    def _gemini_should_not_run(*_args, **_kwargs):
        raise AssertionError("smalltalk must not call Gemini")

    result = routing.route_ask2(
        "hello there",
        k=2,
        vector_fn=_stub_vector([]),
        gemini_fn=_gemini_should_not_run,
    )
    assert result["meta"]["routing"] == "smalltalk"
    assert result["answer"] == "Hello! I can help with Sustainacore, TECH100, and ESG questions."
    assert result["sources"] == []
    assert result["meta"]["gemini_used"] is False
    assert result["meta"]["k"] == 2


def test_smalltalk_with_fallback():
    result = routing.route_ask2(
        "thanks!",
        k=1,
        vector_fn=_stub_vector([]),
        gemini_fn=_stub_gemini(None),
    )
    assert result["meta"]["routing"] == "smalltalk"
    assert result["answer"] == "Hello! I can help with Sustainacore, TECH100, and ESG questions."
    assert result["sources"] == []
    assert result["meta"]["gemini_used"] is False


def test_no_hits_path_gemini_success():
    result = routing.route_ask2(
        "Some off-topic thing",
        k=3,
        vector_fn=_stub_vector([]),
        gemini_fn=_stub_gemini("We do not have that Sustainacore info yet."),
    )
    assert result["meta"]["routing"] == "no_hit"
    assert result["sources"] == []
    assert result["meta"]["gemini_used"] is True


def test_no_hits_path_fallback():
    result = routing.route_ask2(
        "Unknown question",
        vector_fn=_stub_vector([]),
        gemini_fn=_stub_gemini(None),
    )
    assert result["meta"]["routing"] == "no_hit"
    assert "organization" in result["answer"].lower()
    assert "topic" in result["answer"].lower()
    assert "report" in result["answer"].lower()
    assert result["sources"] == []
    assert result["meta"]["gemini_used"] is False


def test_low_confidence_with_gemini():
    hits = [
        {"title": "Doc A", "url": "http://a", "snippet": "Snippet A", "score": 0.40},
        {"title": "Doc B", "url": "http://b", "snippet": "Snippet B", "score": 0.32},
    ]
    result = routing.route_ask2(
        "Tell me about Doc A",
        k=5,
        vector_fn=_stub_vector(hits),
        gemini_fn=_stub_gemini("Answer is inconclusive but here are options."),
    )
    assert result["meta"]["routing"] == "insufficient_context"
    assert result["meta"]["gemini_used"] is False
    assert result["meta"]["top_score"] == 0.4
    assert result["sources"] == []


def test_low_confidence_fallback():
    hits = [
        {"title": "Doc A", "url": "http://a", "snippet": "Snippet A", "score": 0.40},
    ]
    result = routing.route_ask2(
        "Tell me about Doc A",
        k=5,
        vector_fn=_stub_vector(hits),
        gemini_fn=_stub_gemini(None),
    )
    assert result["meta"]["routing"] == "insufficient_context"
    assert result["meta"]["gemini_used"] is False
    assert result["answer"] == INSUFFICIENT_CONTEXT_MESSAGE
    assert result["sources"] == []


def test_high_confidence_with_gemini():
    hits = [
        {"title": "Doc A", "url": "http://a", "snippet": "Snippet A", "score": 0.78},
        {"title": "Doc B", "url": "http://b", "snippet": "Snippet B", "score": 0.72},
    ]
    result = routing.route_ask2(
        "High confidence question",
        k=2,
        vector_fn=_stub_vector(hits),
        gemini_fn=_stub_gemini("Synthesized answer [Source 1]"),
    )
    assert result["meta"]["routing"] == "high_conf"
    assert result["meta"]["gemini_used"] is True
    assert "[source 1]" in result["answer"].lower()
    assert len(result["sources"]) == 2


def test_high_confidence_fallback_builds_summary():
    hits = [
        {"title": "Doc A", "url": "http://a", "snippet": "Snippet A", "score": 0.85},
        {"title": "Doc B", "url": "http://b", "snippet": "Snippet B", "score": 0.80},
    ]
    result = routing.route_ask2(
        "High confidence question",
        k=1,
        vector_fn=_stub_vector(hits),
        gemini_fn=_stub_gemini(None),
    )
    assert result["meta"]["routing"] == "high_conf"
    assert result["meta"]["gemini_used"] is False
    assert "[source 1]" in result["answer"].lower()
    assert result["sources"][0].startswith("Source 1: Doc A")


def test_empty_query_short_circuit():
    result = routing.route_ask2("   ", vector_fn=_stub_vector([]), gemini_fn=_stub_gemini("unused"))
    assert result["meta"]["routing"] == "empty"
    assert result["answer"]
    assert result["sources"] == []
