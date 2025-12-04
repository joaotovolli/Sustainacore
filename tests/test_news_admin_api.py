from pathlib import Path
import sys
from typing import Any, Dict

from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.retrieval import app as retrieval_app  # noqa: E402


_API_TOKEN = "testtoken"


def _client() -> TestClient:
    retrieval_app._API_AUTH_TOKEN = _API_TOKEN
    retrieval_app._API_AUTH_WARNED = False
    return TestClient(retrieval_app.app)


def test_admin_requires_auth(monkeypatch):
    def fake_create_curated_news_item(payload: Dict[str, Any]):
        return {"id": "NEWS_ITEMS:1"}

    monkeypatch.setattr(
        retrieval_app, "create_curated_news_item", fake_create_curated_news_item
    )

    client = _client()
    response = client.post("/api/news/admin/items", json={"title": "t", "url": "u", "source": "s"})
    assert response.status_code == 401


def test_admin_creates_item(monkeypatch):
    created_item = {"id": "NEWS_ITEMS:2", "title": "Hello", "tags": ["t1"]}

    def fake_create_curated_news_item(payload: Dict[str, Any]):
        return created_item

    monkeypatch.setattr(
        retrieval_app, "create_curated_news_item", fake_create_curated_news_item
    )

    client = _client()
    response = client.post(
        "/api/news/admin/items",
        headers={"Authorization": f"Bearer {_API_TOKEN}"},
        json={"title": "Hello", "url": "https://example.com", "source": "Manual"},
    )

    assert response.status_code == 201
    assert response.json()["item"] == created_item
