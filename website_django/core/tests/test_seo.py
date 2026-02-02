import re
from unittest import mock

from django.conf import settings
from django.test import SimpleTestCase
from django.urls import reverse


class SeoFoundationsTests(SimpleTestCase):
    def test_robots_txt(self):
        for host in ("sustainacore.org", "www.sustainacore.org"):
            with self.subTest(host=host):
                response = self.client.get("/robots.txt", HTTP_HOST=host, follow=True)

                if response.redirect_chain:
                    self.assertTrue(all(code == 301 for _, code in response.redirect_chain))

                self.assertEqual(response.status_code, 200)
                self.assertTrue(response["Content-Type"].startswith("text/plain"))
                content = response.content.decode("utf-8")
                self.assertIn("Sitemap:", content)
                self.assertIn("Sitemap: https://sustainacore.org/sitemap.xml", content)
                self.assertEqual(content.count("Sitemap:"), 1)
                self.assertIn("Disallow: /admin/", content)
                self.assertIn("Disallow: /_sc/admin/", content)

    def test_preview_robots_txt_blocks_all(self):
        response = self.client.get("/robots.txt", HTTP_HOST="preview.sustainacore.org", follow=True)
        self.assertEqual(response.status_code, 200)
        content = response.content.decode("utf-8")
        self.assertIn("Disallow: /", content)
        self.assertNotIn("Allow: /", content)

    def test_favicon_served(self):
        response = self.client.get("/favicon.ico", follow=True)
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response["Content-Type"].startswith("image/"))

    @mock.patch(
        "core.sitemaps._get_company_entries",
        return_value=[
            {
                "loc": "https://sustainacore.org/tech100/company/MSFT/",
                "lastmod": "2025-01-01",
                "changefreq": "daily",
                "priority": "0.9",
            }
        ],
    )
    @mock.patch(
        "core.sitemaps._get_news_entries",
        return_value=[
            {
                "loc": "https://sustainacore.org/news/NEWS_ITEMS:1/",
                "lastmod": "2025-01-01",
                "changefreq": "daily",
                "priority": "0.8",
            }
        ],
    )
    def test_sitemap_index(self, mock_news_entries, mock_company_entries):
        canonical_base = f"{settings.SITE_URL.rstrip('/')}/"
        for host in ("sustainacore.org", "www.sustainacore.org"):
            with self.subTest(host=host):
                response = self.client.get("/sitemap.xml", HTTP_HOST=host, follow=True)

                if response.redirect_chain:
                    self.assertTrue(all(code == 301 for _, code in response.redirect_chain))

                self.assertEqual(response.status_code, 200)
                self.assertTrue("xml" in response["Content-Type"])
                self.assertNotIn("X-Robots-Tag", response.headers)
                content = response.content.decode("utf-8")
                self.assertIn("<sitemapindex", content)
                self.assertIn(f"{canonical_base}sitemaps/static.xml", content)
                self.assertIn(f"{canonical_base}sitemaps/tech100.xml", content)
                self.assertIn(f"{canonical_base}sitemaps/news.xml", content)
                self.assertIn(f"{canonical_base}sitemaps/ai_regulation.xml", content)
                self.assertIn(f"{canonical_base}sitemaps/tech100_companies_1.xml", content)
                self.assertIn(f"{canonical_base}sitemaps/news_items_1.xml", content)

    @mock.patch(
        "core.sitemaps._get_company_entries",
        return_value=[
            {
                "loc": "https://sustainacore.org/tech100/company/MSFT/",
                "lastmod": "2025-01-01",
                "changefreq": "daily",
                "priority": "0.9",
            }
        ],
    )
    @mock.patch(
        "core.sitemaps._get_news_entries",
        return_value=[
            {
                "loc": "https://sustainacore.org/news/NEWS_ITEMS:1/",
                "lastmod": "2025-01-01",
                "changefreq": "daily",
                "priority": "0.8",
            }
        ],
    )
    def test_sitemap_sections(self, mock_news_entries, mock_company_entries):
        canonical_base = f"{settings.SITE_URL.rstrip('/')}/"
        response = self.client.get("/sitemaps/static.xml", HTTP_HOST="sustainacore.org", follow=True)
        self.assertEqual(response.status_code, 200)
        content = response.content.decode("utf-8")
        self.assertIn(f"{canonical_base}press/", content)
        self.assertIn(f"{canonical_base}privacy/", content)
        self.assertIn(f"{canonical_base}corrections/", content)

        response = self.client.get("/sitemaps/tech100.xml", HTTP_HOST="sustainacore.org", follow=True)
        self.assertEqual(response.status_code, 200)
        content = response.content.decode("utf-8")
        self.assertIn(f"{canonical_base}tech100/index/", content)
        self.assertIn(f"{canonical_base}tech100/performance/", content)
        self.assertIn(f"{canonical_base}tech100/constituents/", content)
        self.assertIn(f"{canonical_base}tech100/attribution/", content)
        self.assertIn(f"{canonical_base}tech100/scores/", content)
        self.assertIn(f"{canonical_base}tech100/stats/", content)
        self.assertIn(f"{canonical_base}tech100/methodology/", content)

        response = self.client.get("/sitemaps/news.xml", HTTP_HOST="sustainacore.org", follow=True)
        self.assertEqual(response.status_code, 200)
        content = response.content.decode("utf-8")
        self.assertIn(f"{canonical_base}news/", content)

        response = self.client.get("/sitemaps/ai_regulation.xml", HTTP_HOST="sustainacore.org", follow=True)
        self.assertEqual(response.status_code, 200)
        content = response.content.decode("utf-8")
        self.assertIn(f"{canonical_base}ai-regulation/", content)

        response = self.client.get(
            "/sitemaps/tech100_companies_1.xml", HTTP_HOST="sustainacore.org", follow=True
        )
        self.assertEqual(response.status_code, 200)
        content = response.content.decode("utf-8")
        self.assertIn(f"{canonical_base}tech100/company/MSFT/", content)

        response = self.client.get("/sitemaps/news_items_1.xml", HTTP_HOST="sustainacore.org", follow=True)
        self.assertEqual(response.status_code, 200)
        content = response.content.decode("utf-8")
        self.assertIn(f"{canonical_base}news/NEWS_ITEMS:1/", content)

    @mock.patch(
        "core.sitemaps._get_company_entries",
        return_value=[],
    )
    @mock.patch(
        "core.sitemaps._get_news_entries",
        return_value=[],
    )
    def test_sitemap_index_excludes_admin_routes(self, mock_news_entries, mock_company_entries):
        response = self.client.get("/sitemap.xml", HTTP_HOST="sustainacore.org", follow=True)
        self.assertEqual(response.status_code, 200)
        content = response.content.decode("utf-8")
        self.assertNotIn("_sc/admin", content)

    @mock.patch("core.views.fetch_tech100")
    @mock.patch("core.views.get_latest_trade_date")
    def test_canonical_tag_ignores_querystring(self, get_latest_trade_date, fetch_tech100):
        fetch_tech100.return_value = {"items": [], "error": None, "meta": {}}
        get_latest_trade_date.return_value = None

        response = self.client.get(reverse("tech100_scores") + "?q=ai&sector=Software")

        self.assertEqual(response.status_code, 200)
        content = response.content.decode("utf-8")
        self.assertIn('rel="canonical"', content)
        match = re.search(r'rel="canonical" href="([^"]+)"', content)
        self.assertIsNotNone(match)
        canonical_url = match.group(1)
        self.assertEqual(canonical_url, f"{settings.SITE_URL.rstrip('/')}/tech100/scores/")

    @mock.patch("core.views.fetch_tech100")
    @mock.patch("core.views.fetch_news_list")
    @mock.patch("core.views.fetch_filter_options")
    @mock.patch("core.views.get_latest_trade_date")
    def test_json_ld_present_on_home_and_news(
        self, get_latest_trade_date, fetch_filter_options, fetch_news_list, fetch_tech100
    ):
        get_latest_trade_date.return_value = None
        fetch_filter_options.return_value = {
            "source_options": [],
            "tag_options": [],
            "supports_source": False,
            "supports_tag": False,
            "supports_ticker": False,
        }
        fetch_tech100.return_value = {"items": [], "error": None, "meta": {}}
        fetch_news_list.return_value = {
            "items": [
                {
                    "title": "Sample headline",
                    "url": "https://example.com/article",
                    "summary": "Sample summary",
                    "published_at": "2025-01-01T00:00:00Z",
                    "source": "Example News",
                    "has_full_body": True,
                    "id": "NEWS_ITEMS:101",
                }
            ],
            "error": None,
            "meta": {"count": 1, "has_more": False},
        }

        home_response = self.client.get(reverse("home"))
        self.assertEqual(home_response.status_code, 200)
        self.assertIn('application/ld+json', home_response.content.decode("utf-8"))

        news_response = self.client.get(reverse("news"))
        self.assertEqual(news_response.status_code, 200)
        content = news_response.content.decode("utf-8")
        self.assertIn('application/ld+json', content)
        self.assertIn('"@type": "NewsArticle"', content)

    @mock.patch("core.views.fetch_tech100")
    @mock.patch("core.views.fetch_news_list")
    @mock.patch("core.views.get_latest_trade_date")
    @mock.patch("core.context_processors.settings")
    def test_preview_mode_includes_noindex_meta(
        self,
        settings_mock,
        get_latest_trade_date,
        fetch_news_list,
        fetch_tech100,
    ):
        settings_mock.PREVIEW_MODE = True
        settings_mock.PREVIEW_HOSTS = ["preview.sustainacore.org"]
        settings_mock.DEFAULT_META_DESCRIPTION = "Preview description"
        settings_mock.SITE_URL = "https://preview.sustainacore.org"
        get_latest_trade_date.return_value = None
        fetch_tech100.return_value = {"items": [], "error": None, "meta": {}}
        fetch_news_list.return_value = {"items": [], "error": None, "meta": {"count": 0}}

        response = self.client.get(reverse("home"), HTTP_HOST="preview.sustainacore.org")
        self.assertEqual(response.status_code, 200)
        content = response.content.decode("utf-8")
        self.assertIn('name="robots"', content)
        self.assertIn("noindex", content)
