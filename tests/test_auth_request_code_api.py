from fastapi.testclient import TestClient

from app.retrieval import app as retrieval_app


def _client() -> TestClient:
    return TestClient(retrieval_app.app)


def test_request_code_returns_ok_on_success(monkeypatch):
    monkeypatch.setattr(
        retrieval_app,
        "request_login_code_status",
        lambda email_normalized, request_ip: (True, None),
    )

    response = _client().post("/api/auth/request-code", json={"email": "user@example.com"})

    assert response.status_code == 200
    assert response.json() == {"ok": True}


def test_request_code_returns_rate_limit(monkeypatch):
    monkeypatch.setattr(
        retrieval_app,
        "request_login_code_status",
        lambda email_normalized, request_ip: (False, "rate_limited"),
    )

    response = _client().post("/api/auth/request-code", json={"email": "user@example.com"})

    assert response.status_code == 429
    assert response.json()["error"] == "rate_limited"


def test_request_code_returns_email_failed(monkeypatch):
    monkeypatch.setattr(
        retrieval_app,
        "request_login_code_status",
        lambda email_normalized, request_ip: (False, "email_failed"),
    )

    response = _client().post("/api/auth/request-code", json={"email": "user@example.com"})

    assert response.status_code == 502
    assert response.json()["error"] == "email_failed"


def test_request_code_returns_db_error(monkeypatch):
    monkeypatch.setattr(
        retrieval_app,
        "request_login_code_status",
        lambda email_normalized, request_ip: (False, "db_error"),
    )

    response = _client().post("/api/auth/request-code", json={"email": "user@example.com"})

    assert response.status_code == 502
    assert response.json()["error"] == "db_error"
