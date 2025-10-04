import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app import app as flask_app
import app as app_mod
from app.retrieval import adapter


class DummyRetrieval(SimpleNamespace):
    pass


@pytest.fixture(autouse=True)
def clear_fs_env(monkeypatch):
    monkeypatch.delenv("FS_FALLBACK", raising=False)


def make_retrieval(facts, note="- oracle" , latency=42, candidates=1, deduped=1, hop_count=1):
    return DummyRetrieval(
        facts=facts,
        context_note=note,
        latency_ms=latency,
        candidates=candidates,
        deduped=deduped,
        hop_count=hop_count,
    )


def _call_ask2(payload):
    with flask_app.test_request_context('/ask2', method='POST', json={"q": "Ping", "top_k": 4}):
        response = app_mod.ask2()
        if isinstance(response, tuple):
            resp_obj, status = response
            return resp_obj.get_json(), status
        return response.get_json(), response.status_code


def test_pipeline_success(monkeypatch):
    facts = [
        {
            "title": "Doc",
            "snippet": "Snippet",
            "url": "https://example.com",
            "citation_id": "C1",
            "score": 0.9,
            "source_name": "Example",
        }
    ]

    monkeypatch.setattr(adapter.oracle_retriever, "retrieve", lambda *_args, **_kwargs: make_retrieval(facts))

    def fake_compose(question, retriever_result, plan, hop_count):
        adapter.gemini_gateway._last_meta = {"status": "ok", "model": "test", "lat_ms": 15}
        return {"answer": "Yes", "sources": ["Doc [C1]"]}

    monkeypatch.setattr(adapter.gemini_gateway, "compose_answer", fake_compose)

    shaped, status = adapter.ask2_pipeline_first("Is Microsoft in the TECH100 Index?", 4, client_ip="1.2.3.4")

    assert status == 200
    assert shaped["contexts"]
    assert shaped["meta"]["routing"] == "gemini_first"
    assert "Sources:" not in shaped["answer"]
    assert shaped["meta"]["latency_breakdown"]["oracle_ms"] >= 0


def test_pipeline_compose_failure(monkeypatch):
    facts = [
        {
            "title": "Doc",
            "snippet": "Snippet",
            "url": "https://example.com",
            "citation_id": "C1",
            "score": 0.9,
            "source_name": "Example",
        }
    ]

    monkeypatch.setattr(adapter.oracle_retriever, "retrieve", lambda *_args, **_kwargs: make_retrieval(facts))

    def failing_compose(question, retriever_result, plan, hop_count):
        adapter.gemini_gateway._last_meta = {"status": "fail", "code": None}
        return None

    monkeypatch.setattr(adapter.gemini_gateway, "compose_answer", failing_compose)

    shaped, status = adapter.ask2_pipeline_first("Ping", 4)

    assert status == 200
    assert shaped["meta"]["routing"] == "gemini_first_fail"
    assert shaped["contexts"]
    assert "Sources:" not in shaped["answer"]
    assert shaped["meta"].get("note") == "gemini_compose_failed"


def test_fs_backfill_enabled(monkeypatch):
    monkeypatch.setenv("FS_FALLBACK", "1")

    def retrieval_no_facts(*_args, **_kwargs):
        return make_retrieval([], note="- oracle_empty", candidates=0, deduped=0)

    monkeypatch.setattr(adapter.oracle_retriever, "retrieve", retrieval_no_facts)
    monkeypatch.setattr(adapter, "_fs_backfill", lambda question, top_k: [{"title": "FS Doc", "snippet": "FS", "source_url": "https://fs"}] )

    def fake_compose(question, retriever_result, plan, hop_count):
        adapter.gemini_gateway._last_meta = {"status": "ok", "model": "test", "lat_ms": 5}
        return {"answer": "FS answer", "sources": ["FS Doc"]}

    monkeypatch.setattr(adapter.gemini_gateway, "compose_answer", fake_compose)

    shaped, status = adapter.ask2_pipeline_first("Ping", 2)
    assert status == 200
    assert shaped["contexts"]
    assert shaped["answer"].startswith("FS")


def test_route_fallback_on_pipeline_error(monkeypatch):
    def boom(*_args, **_kwargs):
        raise RuntimeError("boom")

    def legacy(question, k_value, **_kwargs):
        return (
            {
                "answer": "Legacy answer",
                "sources": [],
                "contexts": [{"id": "legacy"}],
                "meta": {"routing": "legacy"},
            },
            200,
        )

    monkeypatch.setattr(app_mod, "ask2_pipeline_first", boom, raising=False)
    monkeypatch.setattr(app_mod, "_call_route_ask2_facade", legacy, raising=False)

    payload, status = _call_ask2({})
    assert status == 200
    assert payload["meta"].get("routing") == "legacy"
