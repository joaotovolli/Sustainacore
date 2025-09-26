import os

import pytest

import app as sustainacore_app

if not os.getenv("ORACLE_TEST_DSN"):
    pytest.skip("ORACLE_TEST_DSN not set", allow_module_level=True)


@pytest.fixture(scope="module")
def oracle_client(monkeypatch):
    monkeypatch.setenv("DB_DSN", os.environ["ORACLE_TEST_DSN"])
    return sustainacore_app.app.test_client()


@pytest.mark.integration
def test_readyz_success(oracle_client):
    response = oracle_client.get("/readyz")
    assert response.status_code == 200
    payload = response.get_json()
    assert payload.get("ok") is True


@pytest.mark.integration
def test_ask2_membership_scope(oracle_client, monkeypatch):
    # Force router to return deterministic payload for testing scope metadata.
    def fake_route(question, k):
        return {
            "answer": "membership details",
            "sources": [
                {"title": "Membership", "url": "https://example", "score": 0.9}
            ],
            "meta": {"routing": "membership", "k": k},
        }

    monkeypatch.setattr(sustainacore_app, "_route_ask2", fake_route)
    response = oracle_client.get("/ask2", query_string={"q": "is Microsoft in TECH100?"})
    assert response.status_code == 200
    data = response.get_json()
    assert data["meta"]["routing"] == "membership"
