import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.retrieval import adapter
from app.retrieval.db_capability import Capability
from app.retrieval.oracle_retriever import RetrievalResult


@pytest.fixture(autouse=True)
def _reset_gateway(monkeypatch):
    adapter.gemini_gateway._last_meta = {}
    monkeypatch.setenv("GEMINI_MODEL", "gemini-test")
    monkeypatch.setattr(adapter, 'capability_snapshot', lambda: {'vector_supported': True, 'text_mode': 'vector'})


def _result(contexts, mode="vector"):
    return RetrievalResult(
        contexts=list(contexts),
        mode=mode,
        latency_ms=12,
        capability=Capability(db_version='test', vector_supported=True, vec_col='EMBEDDING', vec_dim=384, vector_rows=len(contexts), esg_docs_count=len(contexts), oracle_text_supported=True),
        note=f"{mode}_search",
    )


def test_pipeline_success(monkeypatch):
    contexts = [
        {
            "doc_id": "1",
            "title": "Doc",
            "source_url": "https://sustainacore.ai",
            "chunk_text": "text",
            "score": 0.9,
        }
    ]

    monkeypatch.setattr(adapter.retriever, "retrieve", lambda question, k, prefer_vector=None: _result(contexts))

    def fake_compose(question, retriever_result, plan, hop_count):
        adapter.gemini_gateway._last_meta = {"status": "ok", "model": "test", "lat_ms": 15}
        assert retriever_result["facts"]
        return {"answer": "Hello", "sources": ["Doc - https://sustainacore.ai"]}

    monkeypatch.setattr(adapter.gemini_gateway, "compose_answer", fake_compose)

    payload, status = adapter.ask2_pipeline_first("Ping", 4, client_ip="1.2.3.4")
    assert status == 200
    assert payload["contexts"] == contexts
    assert payload["meta"]["routing"] == "gemini_first"
    assert payload["answer"] == "Hello"
    debug_block = payload["meta"].get("debug")
    assert isinstance(debug_block, dict)
    assert debug_block.get("capability", {}).get("vector_supported") is True


def test_pipeline_compose_failure(monkeypatch):
    contexts = [
        {
            "doc_id": "1",
            "title": "Doc",
            "source_url": "https://sustainacore.ai",
            "chunk_text": "text",
        }
    ]
    monkeypatch.setattr(adapter.retriever, "retrieve", lambda *args, **kwargs: _result(contexts))

    def boom(*args, **kwargs):
        raise RuntimeError("gemini down")

    monkeypatch.setattr(adapter.gemini_gateway, "compose_answer", boom)

    payload, status = adapter.ask2_pipeline_first("Ping", 4)
    assert status == 200
    assert payload["contexts"] == contexts
    assert payload["meta"]["routing"] == "gemini_first_fail"
    assert payload["answer"].startswith("Gemini is momentarily unavailable")


def test_pipeline_no_contexts(monkeypatch):
    monkeypatch.setattr(adapter.retriever, "retrieve", lambda *args, **kwargs: _result([], mode="like"))
    payload, status = adapter.ask2_pipeline_first("Ping", 3)
    assert status == 200
    assert payload["contexts"] == []
    assert payload["meta"].get("note") == "no_contexts"
