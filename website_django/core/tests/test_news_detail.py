import re
from unittest import mock

from django.test import SimpleTestCase
from django.test.utils import override_settings
from django.urls import reverse


class NewsDetailSmokeTests(SimpleTestCase):
    @mock.patch("core.views.fetch_filter_options")
    @mock.patch("core.views.fetch_news_list")
    def test_news_detail_navigation(self, fetch_news_list, fetch_filter_options):
        fetch_filter_options.return_value = {
            "source_options": [],
            "tag_options": [],
            "supports_source": False,
            "supports_tag": False,
            "supports_ticker": False,
        }
        fetch_news_list.return_value = {
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
            "meta": {"count": 1, "has_more": False},
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

        with mock.patch("core.views.fetch_news_detail_oracle") as fetch_detail:
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

    @mock.patch("core.views.fetch_filter_options")
    @mock.patch("core.views.fetch_news_detail_oracle")
    def test_news_detail_renders_fallback_summary(self, fetch_detail, fetch_filter_options):
        fetch_filter_options.return_value = {
            "source_options": [],
            "tag_options": [],
            "supports_source": False,
            "supports_tag": False,
            "supports_ticker": False,
        }
        fetch_detail.return_value = {
            "item": {
                "id": "NEWS_ITEMS:99",
                "title": "Fallback headline",
                "url": "https://example.com/story",
                "source": "Example News",
                "published_at": "2025-01-02T12:30:00Z",
                "summary": "Short fallback summary.",
            },
            "error": None,
        }
        response = self.client.get(reverse("news_detail", args=["NEWS_ITEMS:99"]), HTTP_HOST="sustainacore.org")
        self.assertEqual(response.status_code, 200)
        content = response.content.decode("utf-8")
        self.assertIn("Short fallback summary.", content)
        self.assertIn("Full text is not available", content)

    @mock.patch("core.views.fetch_filter_options")
    @mock.patch("core.views.fetch_news_detail_oracle")
    def test_news_detail_prefers_long_full_text(self, fetch_detail, fetch_filter_options):
        fetch_filter_options.return_value = {
            "source_options": [],
            "tag_options": [],
            "supports_source": False,
            "supports_tag": False,
            "supports_ticker": False,
        }
        long_text = "Long body text. " * 80
        fetch_detail.return_value = {
            "item": {
                "id": "NEWS_ITEMS:44",
                "title": "Long form headline",
                "url": "https://example.com/story",
                "source": "Example News",
                "published_at": "2025-01-02T12:30:00Z",
                "summary": "Short summary.",
                "full_text": long_text,
            },
            "error": None,
        }
        response = self.client.get(reverse("news_detail", args=["NEWS_ITEMS:44"]), HTTP_HOST="sustainacore.org")
        content = response.content.decode("utf-8")
        self.assertIn("Long body text.", content)

    @mock.patch("core.views.fetch_filter_options")
    @mock.patch("core.views.fetch_news_detail_oracle")
    def test_news_detail_sanitizes_html(self, fetch_detail, fetch_filter_options):
        fetch_filter_options.return_value = {
            "source_options": [],
            "tag_options": [],
            "supports_source": False,
            "supports_tag": False,
            "supports_ticker": False,
        }
        fetch_detail.return_value = {
            "item": {
                "id": "NEWS_ITEMS:55",
                "title": "HTML headline",
                "url": "https://example.com/story",
                "source": "Example News",
                "published_at": "2025-01-02T12:30:00Z",
                "content": "<p>Safe</p><script>alert(1)</script>",
            },
            "error": None,
        }
        response = self.client.get(reverse("news_detail", args=["NEWS_ITEMS:55"]), HTTP_HOST="sustainacore.org")
        content = response.content.decode("utf-8")
        self.assertIn("<p>Safe</p>", content)
        match = re.search(r'<div class="news-detail__rich">(.*?)</div>', content, re.DOTALL)
        self.assertIsNotNone(match)
        self.assertNotIn("<script>", match.group(1))

    @mock.patch("core.views.fetch_filter_options")
    @mock.patch("core.views.fetch_news_detail_oracle")
    def test_news_detail_hides_internal_source_link(self, fetch_detail, fetch_filter_options):
        fetch_filter_options.return_value = {
            "source_options": [],
            "tag_options": [],
            "supports_source": False,
            "supports_tag": False,
            "supports_ticker": False,
        }
        fetch_detail.return_value = {
            "item": {
                "id": "NEWS_ITEMS:77",
                "title": "Internal link headline",
                "url": "https://sustainacore.org/press/example",
                "source": "Example News",
                "published_at": "2025-01-02T12:30:00Z",
                "summary": "Short summary.",
            },
            "error": None,
        }
        response = self.client.get(reverse("news_detail", args=["NEWS_ITEMS:77"]), HTTP_HOST="sustainacore.org")
        content = response.content.decode("utf-8")
        self.assertNotIn("Read at source", content)
        self.assertNotIn("Source link not available", content)

    @mock.patch("core.views.fetch_filter_options")
    @mock.patch("core.views.fetch_news_detail_oracle")
    def test_news_detail_renders_tables_and_images(self, fetch_detail, fetch_filter_options):
        fetch_filter_options.return_value = {
            "source_options": [],
            "tag_options": [],
            "supports_source": False,
            "supports_tag": False,
            "supports_ticker": False,
        }
        fetch_detail.return_value = {
            "item": {
                "id": "NEWS_ITEMS:77",
                "title": "Table headline",
                "url": "https://example.com/story",
                "source": "Example News",
                "published_at": "2025-01-02T12:30:00Z",
                "content": (
                    "<p>Intro</p>"
                    "<table><thead><tr><th>Col</th></tr></thead>"
                    "<tbody><tr><td>Row</td></tr></tbody></table>"
                    '<img src="/news/assets/12/" alt="Chart">'
                ),
            },
            "error": None,
        }
        response = self.client.get(reverse("news_detail", args=["NEWS_ITEMS:77"]), HTTP_HOST="sustainacore.org")
        content = response.content.decode("utf-8")
        self.assertIn("news-detail__table-wrap", content)
        self.assertIn('/news/assets/12/', content)

    @mock.patch("core.views.fetch_filter_options")
    @mock.patch("core.views.fetch_news_detail_oracle")
    @override_settings(DEBUG=True)
    def test_news_detail_includes_debug_comment_when_enabled(self, fetch_detail, fetch_filter_options):
        fetch_filter_options.return_value = {
            "source_options": [],
            "tag_options": [],
            "supports_source": False,
            "supports_tag": False,
            "supports_ticker": False,
        }
        full_text = "Deep article text. " * 300
        fetch_detail.return_value = {
            "item": {
                "id": "NEWS_ITEMS:88",
                "title": "Debug headline",
                "url": "https://example.com/story",
                "source": "Example News",
                "published_at": "2025-01-02T12:30:00Z",
                "summary": "Short summary.",
                "full_text": full_text,
            },
            "error": None,
        }
        response = self.client.get(reverse("news_detail", args=["NEWS_ITEMS:88"]), HTTP_HOST="sustainacore.org")
        content = response.content.decode("utf-8")
        self.assertIn("news_debug", content)
        self.assertIn("field=full_text", content)
