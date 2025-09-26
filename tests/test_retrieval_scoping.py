import pytest

from retrieval.scope import dedupe_contexts, infer_scope
from app.rag import routing as rag_routing
from retrieval.config import INSUFFICIENT_CONTEXT_MESSAGE


def test_membership_scope_detected():
    scope = infer_scope("is Microsoft in tech100?", entities=None)
    assert scope.label == "membership" or "membership" in scope.source_types
    assert scope.applied_filters.get("source_id") == ("MICROSOFT",)


def test_about_site_scope_detected():
    scope = infer_scope("what is this website?", entities=None)
    assert scope.label == "about_site"
    assert "site" in scope.source_types


def test_similarity_floor_returns_insufficient(monkeypatch):
    monkeypatch.setattr(rag_routing, "SIMILARITY_FLOOR", 0.9)

    def low_conf_vector_fn(query: str, k: int):
        return [{"title": "Doc", "url": "", "snippet": "", "score": 0.1}]

    result = rag_routing.route_ask2("test question", k=1, vector_fn=low_conf_vector_fn, gemini_fn=None)
    assert result["answer"] == INSUFFICIENT_CONTEXT_MESSAGE
    assert result["meta"]["routing"] == "insufficient_context"


def test_dedupe_contexts_removes_duplicates():
    chunks = [
        {"title": "Doc", "source_url": "https://a", "chunk_text": "one"},
        {"title": "Doc", "source_url": "https://a", "chunk_text": "two"},
        {"title": "Doc2", "source_url": "https://b", "chunk_text": "three"},
    ]
    deduped = dedupe_contexts(chunks)
    assert len(deduped) == 2
    assert any(c["chunk_text"] == "one" for c in deduped)
