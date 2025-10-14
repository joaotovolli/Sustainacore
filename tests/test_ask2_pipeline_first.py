import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.retrieval import adapter


@pytest.fixture(autouse=True)
def reset_defaults(monkeypatch):
    # Ensure deterministic defaults for tests.
    monkeypatch.setenv("GEMINI_MODEL", "gemini-test")


def test_pipeline_success(monkeypatch):
    contexts = [{"doc_id": "1", "title": "Doc", "source_url": "https://sustainacore.ai", "chunk_text": "text"}]

    monkeypatch.setattr(adapter.oracle_retriever, "retrieve", lambda q, k, filters=None: contexts)

    def fake_compose(question, contexts, *, model=None):
        assert question == "Ping"
        assert contexts
        return {"answer": "Hello", "sources": ["Doc - https://sustainacore.ai"]}

    monkeypatch.setattr(adapter, "_compose_with_gemini", fake_compose)

    payload, status = adapter.ask2_pipeline_first("Ping", 4, client_ip="1.2.3.4")
    assert status == 200
    assert payload["contexts"] == contexts
    assert payload["meta"]["routing"] == "gemini_first"
    assert payload["answer"] == "Hello"
    assert "Sources:" not in payload["answer"]


def test_pipeline_compose_failure(monkeypatch):
    contexts = [{"doc_id": "1", "title": "Doc", "source_url": "https://sustainacore.ai", "chunk_text": "text"}]
    monkeypatch.setattr(adapter.oracle_retriever, "retrieve", lambda q, k, filters=None: contexts)

    def boom(*args, **kwargs):
        raise RuntimeError("gemini down")

    monkeypatch.setattr(adapter, "_compose_with_gemini", boom)

    payload, status = adapter.ask2_pipeline_first("Ping", 4)
    assert status == 200
    assert payload["contexts"] == contexts
    assert payload["meta"]["routing"] == "gemini_first_fail"
    assert payload["answer"]


def test_pipeline_no_contexts(monkeypatch):
    monkeypatch.setattr(adapter.oracle_retriever, "retrieve", lambda q, k, filters=None: [])
    payload, status = adapter.ask2_pipeline_first("Ping", 3)
    assert status == 200
    assert payload["contexts"] == []
    assert payload["meta"]["note"] == "no_contexts"
