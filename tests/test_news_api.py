import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import app.news_service as news_service  # noqa: E402
from app.retrieval import app as retrieval_app  # noqa: E402


_API_TOKEN = "testtoken"


def _enable_api_auth() -> None:
    retrieval_app._API_AUTH_TOKEN = _API_TOKEN
    retrieval_app._API_AUTH_WARNED = False


def _client() -> TestClient:
    _enable_api_auth()
    return TestClient(retrieval_app.app)


def test_news_endpoint_returns_items(monkeypatch):
    def fake_fetch_news_items(
        *,
        limit: Any = None,
        days: Any = None,
        source: Optional[str] = None,
        tags: Optional[List[str]] = None,
    ):
        return (
            [
                {
                    "id": 1,
                    "title": "Example headline",
                    "source": "Example",
                    "url": "https://example.com/article",
                    "summary": "Summary text",
                    "tags": ["esg", "tech"],
                    "published_at": "2024-01-01T00:00:00Z",
                    "company": "ABC",
                }
            ],
            False,
            20,
        )

    monkeypatch.setattr(news_service, "fetch_news_items", fake_fetch_news_items)
    monkeypatch.setattr(retrieval_app, "fetch_news_items", fake_fetch_news_items)

    client = _client()
    response = client.get("/api/news", headers={"Authorization": f"Bearer {_API_TOKEN}"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["meta"] == {"count": 1, "limit": 20, "has_more": False}
    assert payload["items"][0]["title"] == "Example headline"
    assert payload["items"][0]["tags"] == ["esg", "tech"]


def test_news_endpoint_applies_filters(monkeypatch):
    captured: Dict[str, Any] = {}

    def fake_fetch_news_items(
        *,
        limit: Any = None,
        days: Any = None,
        source: Optional[str] = None,
        tags: Optional[List[str]] = None,
    ):
        captured.update(
            {"limit": limit, "days": days, "source": source, "tags": tags}
        )
        return [], False, 5

    monkeypatch.setattr(news_service, "fetch_news_items", fake_fetch_news_items)
    monkeypatch.setattr(retrieval_app, "fetch_news_items", fake_fetch_news_items)

    client = _client()
    response = client.get(
        "/api/news?limit=5&days=10&source=Bloomberg&tag=ai&tag=ml",
        headers={"Authorization": f"Bearer {_API_TOKEN}"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["meta"]["limit"] == 5
    assert captured["limit"] == 5
    assert captured["days"] == 10
    assert captured["source"] == "Bloomberg"
    assert captured["tags"] == ["ai", "ml"]
