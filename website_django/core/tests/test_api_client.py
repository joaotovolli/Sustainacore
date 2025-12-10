from unittest import mock

import requests
from django.test import SimpleTestCase, override_settings

from core import api_client


class FetchNewsTests(SimpleTestCase):
    @override_settings(BACKEND_API_BASE="https://vm1.example", BACKEND_API_TOKEN="token")
    def test_fetch_news_success(self):
        response_payload = {
            "items": [
                {
                    "id": "1",
                    "title": "Sample",
                    "source": "Example",
                    "url": "https://example.com",
                    "summary": "Summary",
                    "tags": ["tag1"],
                    "published_at": "2025-01-02T15:04:05Z",
                }
            ],
            "meta": {"count": 1, "limit": 10, "has_more": False},
        }

        mock_response = mock.Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = response_payload

        with mock.patch("core.api_client.requests.get", return_value=mock_response) as get_mock:
            result = api_client.fetch_news(source="Example", tag="tag1", days=7, limit=10)

        get_mock.assert_called_once_with(
            "https://vm1.example/api/news",
            headers=mock.ANY,
            params={"limit": 10, "source": "Example", "tag": "tag1", "days": 7},
            timeout=mock.ANY,
        )
        self.assertEqual(result["items"], response_payload["items"])
        self.assertEqual(result["meta"], response_payload["meta"])
        self.assertIsNone(result["error"])

    @override_settings(BACKEND_API_BASE="https://vm1.example")
    def test_fetch_news_omits_days_when_none(self):
        mock_response = mock.Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {"items": [], "meta": {}}

        with mock.patch("core.api_client.requests.get", return_value=mock_response) as get_mock:
            api_client.fetch_news(source=None, tag=None, days=None)

        get_mock.assert_called_once_with(
            "https://vm1.example/api/news", headers=mock.ANY, params={"limit": 20}, timeout=mock.ANY
        )

    @override_settings(BACKEND_API_BASE="https://vm1.example")
    def test_fetch_news_includes_ticker(self):
        mock_response = mock.Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {"items": [], "meta": {"has_more": False}}

        with mock.patch("core.api_client.requests.get", return_value=mock_response) as get_mock:
            api_client.fetch_news(ticker="AAPL", limit=5)

        get_mock.assert_called_once_with(
            "https://vm1.example/api/news",
            headers=mock.ANY,
            params={"limit": 5, "ticker": "AAPL"},
            timeout=mock.ANY,
        )

    @override_settings(BACKEND_API_BASE="https://vm1.example")
    def test_fetch_news_handles_request_error(self):
        with mock.patch(
            "core.api_client.requests.get", side_effect=requests.RequestException("boom")
        ):
            result = api_client.fetch_news()

        self.assertEqual(result["items"], [])
        self.assertEqual(result["meta"], {})
        self.assertIsNotNone(result["error"])


class CreateNewsItemAdminTests(SimpleTestCase):
    @override_settings(BACKEND_API_BASE="https://vm1.example", BACKEND_API_TOKEN="token")
    def test_create_news_item_admin_success(self):
        response_payload = {"item": {"id": "abc", "title": "New item"}}

        mock_response = mock.Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = response_payload

        with mock.patch("core.api_client.requests.post", return_value=mock_response) as post_mock:
            result = api_client.create_news_item_admin(
                title="New item",
                url="https://example.com/news",
                source="Curated",
                tags=["tag1"],
            )

        post_mock.assert_called_once_with(
            "https://vm1.example/api/news/admin/items",
            headers=mock.ANY,
            json={"title": "New item", "url": "https://example.com/news", "source": "Curated", "tags": ["tag1"]},
            timeout=mock.ANY,
        )
        self.assertEqual(result["item"], response_payload["item"])
        self.assertIsNone(result["error"])

    @override_settings(BACKEND_API_BASE="https://vm1.example")
    def test_create_news_item_admin_handles_error(self):
        with mock.patch(
            "core.api_client.requests.post", side_effect=requests.RequestException("fail")
        ):
            result = api_client.create_news_item_admin(title="Bad", url="https://example.com")

        self.assertIsNone(result["item"])
        self.assertIsNotNone(result["error"])
