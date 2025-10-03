import importlib

import pytest
from fastapi.testclient import TestClient


def test_ask2_contract_nonempty():
    mod = importlib.import_module("app.retrieval.app")
    client = TestClient(mod.app)
    r = client.get("/ask2", params={"q": "hello", "k": 2})
    assert r.status_code == 200
    j = r.json()
    assert set(["answer", "sources", "meta"]).issubset(j.keys())
    assert isinstance(j["answer"], str) and j["answer"].strip() != ""
    assert isinstance(j["sources"], list)
    assert isinstance(j["meta"], dict)


def _client() -> TestClient:
    mod = importlib.import_module("app.retrieval.app")
    return TestClient(mod.app)


def test_health_endpoints():
    client = _client()
    response = client.get("/health")
    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is True
    assert "git" in body and isinstance(body["git"], str)
    assert "provider" in body and isinstance(body["provider"], str)
    assert "model" in body and isinstance(body["model"], str)
    assert "flags" in body and isinstance(body["flags"], dict)
    assert "PERSONA_V1" in body["flags"]
    assert "REQUEST_NORMALIZE" in body["flags"]

    healthz = client.get("/healthz")
    assert healthz.status_code == 200
    assert healthz.json() == {"ok": True}


@pytest.mark.parametrize(
    "request_kwargs",
    [
        {"json": {"question": "ping", "top_k": 1}},
        {
            "data": "{\"question\": \"ping\"",
            "headers": {"Content-Type": "application/json"},
        },
        {"data": "plain ping", "headers": {"Content-Type": "text/plain"}},
    ],
)
def test_ask2_post_always_json(request_kwargs):
    client = _client()
    response = client.post("/ask2", **request_kwargs)
    assert response.status_code == 200
    body = response.json()
    assert "answer" in body
    assert "contexts" in body and isinstance(body["contexts"], list)
