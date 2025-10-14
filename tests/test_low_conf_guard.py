import sys
from pathlib import Path

import pytest
from flask import Response

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import app as ask_app


def _invoke_low_conf(monkeypatch):
    def _low_conf_pipeline(_question: str, _k: int, *, client_ip: str = ""):  # noqa: D401
        payload = {
            "answer": "Potential answer with low confidence.",
            "contexts": [
                {
                    "title": "Sparse Context",
                    "score": 0.05,
                    "source_url": "https://example.com/low",
                }
            ],
            "sources": ["Legacy source"],
            "meta": {"routing": "gemini_first", "gemini_used": True},
        }
        return payload, 200

    monkeypatch.setattr(ask_app, "ask2_pipeline_first", _low_conf_pipeline)
    monkeypatch.setattr(ask_app, "_ASK2_ENABLE_SMALLTALK", False)

    with ask_app.app.test_request_context("/ask2", method="GET", query_string={"q": "unknown"}):
        response, status = ask_app.ask2()
        assert isinstance(response, Response)
        return response.get_json(), status


def test_low_confidence_guard(monkeypatch):
    body, status = _invoke_low_conf(monkeypatch)

    assert status == 200
    assert body["answer"] == ask_app._LOW_CONFIDENCE_MESSAGE
    assert body["sources"] == []
    assert body.get("contexts") == []
    assert body["meta"]["routing"] == "low_conf"
    assert body["meta"].get("floor_warning")
    assert body["meta"].get("top_score") is not None
