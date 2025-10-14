import sys
from pathlib import Path

import pytest
from flask import Response

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import app as ask_app


def _invoke_sources(monkeypatch):
    def _pipeline(_question: str, _k: int, *, client_ip: str = ""):
        payload = {
            "answer": "Here’s the best supported answer\nSources:\n- old",
            "contexts": [
                {
                    "title": "Products & Segments",
                    "section": "North America",
                    "score": 0.82,
                    "source_url": "https://example.com/report#products",
                },
                {
                    "title": "Products & Segments",
                    "section": "North America",
                    "score": 0.80,
                    "source_url": "https://example.com/report#products",
                },
                {
                    "title": "ESG Overview",
                    "section": "Highlights",
                    "score": 0.78,
                    "source_url": "https://example.com/report#overview",
                },
            ],
            "sources": ["legacy 1", "legacy 2"],
            "meta": {"routing": "gemini_first", "gemini_used": True},
        }
        return payload, 200

    monkeypatch.setattr(ask_app, "ask2_pipeline_first", _pipeline)
    monkeypatch.setattr(ask_app, "_ASK2_ENABLE_SMALLTALK", False)
    monkeypatch.setattr(ask_app, "_ASK2_MAX_SOURCES", 2)
    monkeypatch.setattr(ask_app, "_ASK2_SOURCE_LABEL_MODE", "concise")

    with ask_app.app.test_request_context("/ask2", method="POST", json={"q": "esg"}):
        response, status = ask_app.ask2()
        assert isinstance(response, Response)
        return response.get_json(), status


def test_sources_dedup_and_format(monkeypatch):
    body, status = _invoke_sources(monkeypatch)

    assert status == 200
    assert body["meta"]["routing"] == "gemini_first"
    assert len(body["sources"]) <= 2
    assert all(source.startswith("Source ") for source in body["sources"])
    assert "›" in body["sources"][0]
    assert body["sources"][0].count("(") <= 1
    assert len(body.get("contexts", [])) == 2
    assert "best supported answer" not in body["answer"].lower()
