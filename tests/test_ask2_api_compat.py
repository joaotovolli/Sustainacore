import importlib

from fastapi.testclient import TestClient
import pytest


@pytest.mark.anyio
async def test_normalize_request_maps_user_message(monkeypatch):
    normalizer = importlib.import_module("app.request_normalizer")

    class FakeRequest:
        headers = {"content-type": "application/json"}

        async def body(self) -> bytes:
            return b""

    payload = {"user_message": "hi"}
    normalized, err = await normalizer.normalize_request(FakeRequest(), payload)
    assert err is None
    assert normalized["question"] == "hi"


def test_api_ask2_accepts_user_message_and_requires_auth(monkeypatch):
    monkeypatch.setenv("API_AUTH_TOKEN", "testtoken")

    mod = importlib.import_module("app.retrieval.app")
    mod = importlib.reload(mod)

    def fake_run_pipeline(question: str, *, k: int, client_ip=None):
        return {
            "answer": f"ok:{question}",
            "sources": ["Source 1: SustainaCore (https://sustainacore.org/)"],
            "contexts": [],
            "meta": {"intent": "TEST"},
        }

    monkeypatch.setattr(mod, "run_pipeline", fake_run_pipeline, raising=True)

    client = TestClient(mod.app)

    # Auth required on /api/ask2.
    resp = client.post("/api/ask2", json={"user_message": "hi", "k": 1})
    assert resp.status_code == 401

    resp = client.post(
        "/api/ask2",
        json={"user_message": "hi", "k": 1},
        headers={"Authorization": "Bearer testtoken"},
    )
    assert resp.status_code == 200
    payload = resp.json()
    assert "ok:hi" in (payload.get("answer") or "")
    assert payload.get("meta", {}).get("reason") != "empty_question"


def test_ask2_public_endpoint_accepts_user_message(monkeypatch):
    mod = importlib.import_module("app.retrieval.app")

    def fake_run_pipeline(question: str, *, k: int, client_ip=None):
        return {
            "answer": f"ok:{question}",
            "sources": [],
            "contexts": [],
            "meta": {"intent": "TEST"},
        }

    monkeypatch.setattr(mod, "run_pipeline", fake_run_pipeline, raising=True)
    client = TestClient(mod.app)
    resp = client.post("/ask2", json={"user_message": "hi", "k": 1})
    assert resp.status_code == 200
    assert "ok:hi" in (resp.json().get("answer") or "")
