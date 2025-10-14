import importlib
from pathlib import Path

import pytest


@pytest.fixture
def client(monkeypatch):
    module_root = Path(__file__).resolve().parents[1]
    monkeypatch.syspath_prepend(str(module_root))
    module = importlib.import_module("app")

    def fake_downstream(app_obj, body, extra_headers=None):
        return "200 OK", [
            ("Content-Type", "application/json"),
            ("Content-Length", "123"),
            ("X-Orig", "1"),
        ], {
            "answer": "fallback answer",
            "contexts": [{"title": "stub"}],
            "meta": {"note": "stub"},
        }

    monkeypatch.setattr(module, "_call_downstream_wsgipost", fake_downstream)
    return module.app.test_client()


@pytest.mark.parametrize(
    "payload, content_type",
    [
        ("{\"query\":\"Is Microsoft in the TECH100 index?\",\"top_k\":3}", "application/json"),
        ("{\"question\":null,\"top_k\":3}", "application/json"),
        ("{}", "application/json"),
        ("Is Microsoft in the TECH100 index?", "text/plain"),
    ],
    ids=["wrong_key", "null_question", "empty_json", "text_plain"],
)
def test_ask2_handles_malformed_payloads(client, payload, content_type):
    response = client.post("/ask2", data=payload, content_type=content_type)

    assert response.status_code == 200
    data = response.get_json()
    assert data["answer"].strip()
    assert isinstance(data["contexts"], list)
    assert response.headers['Content-Type'].startswith('application/json')
    assert response.headers.get("X-Orch") == "pass"
    assert response.headers.get("X-Orig") == "1"
    assert int(response.headers["Content-Length"]) == len(response.get_data())
