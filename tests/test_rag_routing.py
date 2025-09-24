import importlib
import importlib.machinery
import sys
import types
from pathlib import Path

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
FULL_MODE_META_KEY = getattr(routing, "FULL_MODE_META_KEY", "_full_mode_payload")


def _stub_vector(results):
    def _inner(_query, _k):
        return results

    return _inner


def _stub_gemini(response):
    def _inner(_prompt, _timeout=None, _model=None):
        return response

    return _inner


def test_smalltalk_with_gemini_success():
    result = routing.route_ask2(
        "hello there",
        k=2,
        vector_fn=_stub_vector([]),
        gemini_fn=_stub_gemini("Hi! How can I help with Sustainacore today?"),
    )
    assert result["meta"]["routing"] == "smalltalk"
    assert result["answer"]
    assert result["sources"] == []
    assert result["meta"]["gemini_used"] is True
    assert result["meta"]["k"] == 2
    extras = result["meta"].get(FULL_MODE_META_KEY)
    assert isinstance(extras, dict)
    assert extras.get("type") == "greeting"


def test_smalltalk_with_fallback():
    result = routing.route_ask2(
        "thanks!",
        k=1,
        vector_fn=_stub_vector([]),
        gemini_fn=_stub_gemini(None),
    )
    assert result["meta"]["routing"] == "smalltalk"
    assert result["answer"]
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
    extras = result["meta"].get(FULL_MODE_META_KEY)
    assert extras and extras.get("type") == "no_hit"


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
    assert result["meta"]["routing"] == "low_conf"
    assert result["meta"]["gemini_used"] is True
    assert result["meta"]["top_score"] == 0.4
    assert len(result["sources"]) >= 1


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
    assert result["meta"]["routing"] == "low_conf"
    assert result["meta"]["gemini_used"] is False
    assert "inconclusive" in result["answer"].lower()
    assert "organization" in result["answer"].lower()
    assert len(result["sources"]) == 1


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
    extras = result["meta"].get(FULL_MODE_META_KEY)
    assert extras and isinstance(extras.get("contexts"), list)


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
    extras = result["meta"].get(FULL_MODE_META_KEY)
    assert extras and isinstance(extras.get("contexts"), list)


def test_empty_query_short_circuit():
    result = routing.route_ask2("   ", vector_fn=_stub_vector([]), gemini_fn=_stub_gemini("unused"))
    assert result["meta"]["routing"] == "empty"
    assert result["answer"]
    assert result["sources"] == []


def test_full_mode_metadata_available_in_meta():
    hits = [
        {"title": "Doc A", "url": "http://a", "snippet": "Snippet A", "score": 0.9},
    ]
    result = routing.route_ask2(
        "full mode via facade",
        k=1,
        vector_fn=_stub_vector(hits),
        gemini_fn=_stub_gemini("Answer [Source 1]"),
    )
    extras = result["meta"].get(FULL_MODE_META_KEY)
    assert isinstance(extras, dict)
    assert "contexts" in extras
    assert "type" not in result and "contexts" not in result
