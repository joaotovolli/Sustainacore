import sys
from pathlib import Path
from typing import Dict

import pytest
from flask import Response

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import app as ask_app
from app.rag import routing as ask_routing


@pytest.mark.parametrize(
    "phrase",
    ["hi", "hello", "oi", "olá", "ciao", "thanks"],
)
def test_smalltalk_short_circuit_via_router(monkeypatch, phrase):
    def _raise_pipeline(*_args, **_kwargs):
        raise AssertionError("pipeline should not be invoked for smalltalk")

    monkeypatch.setattr(ask_app, "ask2_pipeline_first", _raise_pipeline)
    monkeypatch.setattr(ask_app, "_ASK2_ENABLE_SMALLTALK", True)
    monkeypatch.setattr(ask_app, "_router_is_smalltalk", ask_routing._is_smalltalk)

    def _fake_route(question: str, _k: int) -> Dict[str, object]:
        assert ask_routing._is_smalltalk(question)
        return {
            "answer": f"Olá! ({question.strip()})",
            "sources": [],
            "meta": {"routing": "smalltalk", "gemini_used": False},
        }

    monkeypatch.setattr(ask_app, "_route_ask2", _fake_route)

    with ask_app.app.test_request_context("/ask2", method="POST", json={"q": phrase}):
        response, status = ask_app.ask2()
        assert isinstance(response, Response)
        body = response.get_json()

    assert status == 200
    assert body["sources"] == []
    assert body["meta"]["routing"] == "smalltalk"
    assert body.get("contexts") == []
    assert "best supported answer" not in body["answer"].lower()
