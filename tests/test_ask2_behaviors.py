import json
import sys
from pathlib import Path
from typing import Dict, Tuple

import pytest
from flask import Response

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import app as ask_app


@pytest.fixture(autouse=True)
def disable_external_calls(monkeypatch):
    from app.retrieval import adapter as retrieval_adapter

    def _raise_pipeline(*_args, **_kwargs):
        raise RuntimeError("pipeline disabled")

    monkeypatch.setattr(retrieval_adapter, "ask2_pipeline_first", _raise_pipeline)
    monkeypatch.setattr(ask_app, "_gemini_run_pipeline", None)
    monkeypatch.setattr(ask_app, "embed_text", lambda text, settings=None: [0.0])
    monkeypatch.setattr(ask_app, "_top_k_by_vector", lambda *args, **kwargs: [])
    monkeypatch.setattr(ask_app, "smalltalk_response", lambda _q: None)
    return


def _stub_route(monkeypatch, payload: Dict[str, object]):
    def _inner(_question: str, _k: int) -> Dict[str, object]:
        return payload

    monkeypatch.setattr(ask_app, "_route_ask2", _inner)


def _stub_route_raising(monkeypatch, exc: Exception):
    def _inner(_question: str, _k: int):
        raise exc

    monkeypatch.setattr(ask_app, "_route_ask2", _inner)


def _invoke_ask2(query: str, *, headers=None, method: str = "GET") -> Tuple[dict, int]:
    headers = headers or {}
    context_args = {
        "path": "/ask2",
        "method": method,
        "headers": headers,
    }
    if method == "POST":
        context_args["json"] = {"q": query}
    else:
        context_args["query_string"] = {"q": query}

    with ask_app.app.test_request_context(**context_args):
        response, status = ask_app.ask2()
        assert isinstance(response, Response)
        return response.get_json(), status


def test_header_robustness(monkeypatch):
    payload = {
        "answer": "Header path answer",
        "contexts": [{"title": "Doc", "snippet": "Snippet", "score": 0.9}],
        "sources": [{"title": "Doc", "url": "http://example.com"}],
    }
    _stub_route(monkeypatch, payload)

    body, status = _invoke_ask2("test question")
    assert status == 200
    assert body["answer"] == "Header path answer"
    assert isinstance(body["contexts"], list)

    body_with_headers, status_with_headers = _invoke_ask2(
        "test question",
        headers={
            "X-Unexpected": "value",
            "X-Forwarded-For": "1.2.3.4",
            "X-Ask2-Hints": json.dumps({"scope": "tech100"}),
        },
    )
    assert status_with_headers == 200
    assert body_with_headers["answer"] == "Header path answer"
    assert body_with_headers["meta"]["request_hints"]["hints"] == {"scope": "tech100"}


def test_small_talk_short_circuit(monkeypatch):
    _stub_route_raising(monkeypatch, RuntimeError("should not call legacy router"))

    for phrase in ["hi", "hello!", "thanks", "help", "goodbye"]:
        body, status = _invoke_ask2(phrase)
        assert status == 200
        assert isinstance(body.get("contexts"), list)
        assert "Suggested follow-ups" in body["answer"]
        suggestion_lines = [line for line in body["answer"].splitlines() if line.startswith("- ")]
        assert 2 <= len(suggestion_lines) <= 4
        assert body["meta"]["routing"] == "smalltalk"


def test_sources_removed_from_answer(monkeypatch):
    payload = {
        "answer": "Result\nWhy this answer:\n- detail\nSources:\n- one\nHere’s the best supported answer",
        "contexts": [{"title": "Doc", "score": 0.75}],
        "sources": [{"title": "Doc", "url": "https://example.com"}],
    }
    _stub_route(monkeypatch, payload)

    body, status = _invoke_ask2("tell me")
    assert status == 200
    assert "Why this answer" not in body["answer"]
    assert "Sources:" not in body["answer"]
    assert "Here’s the best supported answer" not in body["answer"]
    assert body["contexts"]
    assert body["sources"]


def test_similarity_floor_monitor_and_enforce(monkeypatch, caplog):
    payload = {
        "answer": "Low confidence answer",
        "contexts": [{"title": "Doc", "score": 0.2}],
        "sources": [{"title": "Doc", "url": "https://example.com"}],
    }
    _stub_route(monkeypatch, payload)

    monkeypatch.setattr(ask_app, "SIMILARITY_FLOOR", 0.5)
    monkeypatch.setattr(ask_app, "SIMILARITY_FLOOR_MODE", "monitor")

    caplog.clear()
    caplog.set_level("INFO", logger="app.ask2")
    monitor_body, monitor_status = _invoke_ask2("monitor mode")
    assert monitor_status == 200
    assert monitor_body["contexts"]
    assert any("floor_decision=below_floor" in record.message for record in caplog.records)

    monkeypatch.setattr(ask_app, "SIMILARITY_FLOOR_MODE", "enforce")
    caplog.clear()
    enforce_body, enforce_status = _invoke_ask2("enforce mode")
    assert enforce_status == 200
    assert isinstance(enforce_body.get("contexts"), list)
    assert enforce_body["contexts"]
    assert any("floor_decision=below_floor" in record.message for record in caplog.records)


def test_contract_shape_remains(monkeypatch):
    payload = {
        "answer": "Shape answer",
        "contexts": [{"title": "Doc", "score": 0.8}],
        "sources": [{"title": "Doc", "url": "https://example.com"}],
        "meta": {"show_debug_block": False},
    }
    _stub_route(monkeypatch, payload)

    body, status = _invoke_ask2("shape")
    assert status == 200
    assert set(["answer", "sources", "meta"]).issubset(body.keys())
    assert isinstance(body["sources"], list)
    assert isinstance(body.get("contexts"), list)
    assert isinstance(body["meta"], dict)
