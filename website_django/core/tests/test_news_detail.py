import re
from unittest import mock

from django.test import SimpleTestCase
from django.urls import reverse


class NewsDetailSmokeTests(SimpleTestCase):
    @mock.patch("core.views.fetch_news")
    def test_news_detail_navigation(self, fetch_news):
        fetch_news.return_value = {
            "items": [
                {
                    "id": "NEWS_ITEMS:101",
                    "title": "Sample ESG headline",
                    "url": "https://example.com/story",
                    "summary": "A short summary of the story.",
                    "has_full_body": True,
                    "source": "Example News",
                    "published_at": "2025-01-02T12:30:00Z",
                    "tags": ["Regulation"],
                    "categories": ["AI"],
                    "pillar_tags": ["Transparency"],
                    "ticker": "MSFT",
                }
            ],
            "meta": {"count": 1},
            "error": None,
        }

        response = self.client.get(reverse("news"), HTTP_HOST="sustainacore.org")
        self.assertEqual(response.status_code, 200)
        content = response.content.decode("utf-8")

        match = re.search(r'<a[^>]*data-news-link[^>]*href="([^"]+)"', content)
        if not match:
            match = re.search(r'<a[^>]*href="([^"]+)"[^>]*data-news-link', content)
        self.assertIsNotNone(match)
        detail_url = match.group(1)

        if detail_url.startswith("http"):
            self.assertIn("example.com", detail_url)
            return

        with mock.patch("core.views.fetch_news_detail") as fetch_detail:
            fetch_detail.return_value = {
                "item": {
                    "id": "NEWS_ITEMS:101",
                    "title": "Sample ESG headline",
                    "url": "https://example.com/story",
                    "source": "Example News",
                    "published_at": "2025-01-02T12:30:00Z",
                    "body": "Full article body. " * 20,
                    "has_full_body": True,
                },
                "error": None,
            }
            detail_response = self.client.get(detail_url, HTTP_HOST="sustainacore.org")
            self.assertEqual(detail_response.status_code, 200)
            detail_content = detail_response.content.decode("utf-8")
            self.assertIn("data-news-title", detail_content)
            self.assertIn("Sample ESG headline", detail_content)
            self.assertRegex(detail_content, r"Full article body")
            self.assertRegex(detail_content, r'href=\"https://example.com/story\"')
