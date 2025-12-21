import re
from unittest import mock

from django.test import SimpleTestCase
from django.urls import reverse


class SeoFoundationsTests(SimpleTestCase):
    def test_robots_txt(self):
        response = self.client.get("/robots.txt")

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response["Content-Type"].startswith("text/plain"))
        content = response.content.decode("utf-8")
        self.assertIn("Sitemap:", content)
        self.assertIn("Disallow: /admin/", content)

    def test_sitemap_xml(self):
        response = self.client.get("/sitemap.xml")

        self.assertEqual(response.status_code, 200)
        self.assertTrue("xml" in response["Content-Type"])
        self.assertNotIn("X-Robots-Tag", response.headers)
        content = response.content.decode("utf-8")
        self.assertIn("http://testserver/", content)
        self.assertIn("http://testserver/tech100/index/", content)
        self.assertIn("http://testserver/tech100/performance/", content)
        self.assertIn("http://testserver/tech100/constituents/", content)
        self.assertIn("http://testserver/tech100/", content)
        self.assertIn("http://testserver/news/", content)
        self.assertIn("http://testserver/press/", content)
        self.assertGreater(content.count("<loc>"), 10)

    @mock.patch("core.views.fetch_tech100")
    @mock.patch("core.views.fetch_news")
    def test_canonical_tag_ignores_querystring(self, fetch_news, fetch_tech100):
        fetch_news.return_value = {"items": [], "error": None, "meta": {}}
        fetch_tech100.return_value = {"items": [], "error": None, "meta": {}}

        response = self.client.get(reverse("tech100") + "?q=ai&sector=Software")

        self.assertEqual(response.status_code, 200)
        content = response.content.decode("utf-8")
        self.assertIn('rel="canonical"', content)
        match = re.search(r'rel="canonical" href="([^"]+)"', content)
        self.assertIsNotNone(match)
        canonical_url = match.group(1)
        self.assertEqual(canonical_url, "http://testserver/tech100/")

    @mock.patch("core.views.fetch_tech100")
    @mock.patch("core.views.fetch_news")
    def test_json_ld_present_on_home_and_news(self, fetch_news, fetch_tech100):
        fetch_tech100.return_value = {"items": [], "error": None, "meta": {}}
        fetch_news.return_value = {
            "items": [
                {
                    "title": "Sample headline",
                    "url": "https://example.com/article",
                    "summary": "Sample summary",
                    "published_at": "2025-01-01T00:00:00Z",
                    "source": "Example News",
                }
            ],
            "error": None,
            "meta": {},
        }

        home_response = self.client.get(reverse("home"))
        self.assertEqual(home_response.status_code, 200)
        self.assertIn('application/ld+json', home_response.content.decode("utf-8"))

        news_response = self.client.get(reverse("news"))
        self.assertEqual(news_response.status_code, 200)
        content = news_response.content.decode("utf-8")
        self.assertIn('application/ld+json', content)
        self.assertIn('"@type": "NewsArticle"', content)

    @mock.patch("core.context_processors.settings")
    def test_preview_mode_includes_noindex_meta(self, settings_mock):
        settings_mock.PREVIEW_MODE = True
        settings_mock.PREVIEW_HOSTS = ["preview.sustainacore.org"]
        settings_mock.DEFAULT_META_DESCRIPTION = "Preview description"
        settings_mock.SITE_URL = "https://preview.sustainacore.org"

        response = self.client.get(reverse("home"), HTTP_HOST="preview.sustainacore.org")
        self.assertEqual(response.status_code, 200)
        content = response.content.decode("utf-8")
        self.assertIn('name="robots"', content)
        self.assertIn("noindex", content)
