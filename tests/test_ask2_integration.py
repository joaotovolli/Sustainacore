import sys
from pathlib import Path

import pytest
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import app as app_mod
from app import app as flask_app


@pytest.fixture(autouse=True)
def reset_env(monkeypatch):
    monkeypatch.setenv("ASK2_MAX_SOURCES", "3")
    monkeypatch.setenv("GEMINI_MODEL", "gemini-test")


def _invoke():
    with flask_app.test_request_context('/ask2', method='POST', json={"q": "Ping", "k": 3}):
        response = app_mod.ask2()
        if isinstance(response, tuple):
            resp_obj, status = response
            return resp_obj.get_json(), status
        return response.get_json(), response.status_code


def test_pipeline_response_includes_capability(monkeypatch):
    def fake_pipeline(question, k, client_ip):
        return (
            {
                "answer": "hello",
                "sources": ["Doc"],
                "contexts": [
                    {
                        "title": "Doc",
                        "source_url": "https://example", 
                        "chunk_text": "Snippet",
                        "score": 0.9,
                    }
                ],
                "meta": {"debug": {"capability": {"vector_supported": True}}},
            },
            200,
        )

    monkeypatch.setattr(app_mod, "ask2_pipeline_first", fake_pipeline, raising=False)
    payload, status = _invoke()
    assert status == 200
    assert payload["contexts"]
    capability = payload["meta"].get("debug", {}).get("capability", {})
    assert capability.get("vector_supported") is True


def test_low_confidence_guard(monkeypatch):
    def fake_pipeline(question, k, client_ip):
        return (
            {
                "answer": "",
                "sources": [],
                "contexts": [
                    {
                        "title": "Doc",
                        "source_url": "https://example",
                        "chunk_text": "Snippet",
                        "score": 0.05,
                    }
                ],
                "meta": {"debug": {"capability": {}}},
            },
            200,
        )

    monkeypatch.setattr(app_mod, "ask2_pipeline_first", fake_pipeline, raising=False)
    monkeypatch.setattr(app_mod, "_below_similarity_floor", lambda score: True, raising=False)

    payload, status = _invoke()
    assert status == 200
    assert payload["meta"].get("routing") == "low_conf"
    assert payload["answer"].startswith("I didn’t find enough")


def test_pipeline_error_falls_back(monkeypatch):
    def boom(*args, **kwargs):
        raise RuntimeError("pipeline failure")

    def legacy(question, k_value, **kwargs):
        return (
            {
                "answer": "Legacy answer",
                "sources": [],
                "contexts": [{"title": "Legacy"}],
                "meta": {"routing": "legacy"},
            },
            200,
        )

    monkeypatch.setattr(app_mod, "ask2_pipeline_first", boom, raising=False)
    monkeypatch.setattr(app_mod, "_call_route_ask2_facade", legacy, raising=False)

    payload, status = _invoke()
    assert status == 200
    assert payload["meta"].get("routing") == "legacy"
