import app as sustainacore_app
import pytest


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(
        sustainacore_app,
        "_route_ask2",
        lambda question, k: {"answer": "ok", "sources": [], "meta": {"k": k}},
    )
    monkeypatch.setattr(
        sustainacore_app,
        "embed",
        lambda text: [0.0] * sustainacore_app.EMBED_DIM,
    )
    monkeypatch.setattr(
        sustainacore_app,
        "_top_k_by_vector",
        lambda vec, k, filters=None: [],
    )
    return sustainacore_app.app.test_client()


def test_ask2_without_headers(client):
    response = client.get("/ask2", query_string={"q": "hello"})
    assert response.status_code == 200
    payload = response.get_json()
    assert payload["answer"]


def test_ask2_with_extra_headers(client):
    response = client.get(
        "/ask2",
        query_string={"q": "hello"},
        headers={"X-Unknown": "42"},
    )
    assert response.status_code == 200
    payload = response.get_json()
    assert payload["answer"]
