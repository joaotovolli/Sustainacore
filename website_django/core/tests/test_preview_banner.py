import os
from unittest import mock

from django.test import TestCase, override_settings


class PreviewBannerTests(TestCase):
    @mock.patch.dict(os.environ, {"TECH100_UI_DATA_MODE": "fixture"})
    @mock.patch("core.views.fetch_news")
    @mock.patch("core.views.fetch_tech100")
    @override_settings(SUSTAINACORE_ENV="preview", PREVIEW_MODE=True)
    def test_banner_hidden_on_production_host(self, tech100_mock, news_mock):
        tech100_mock.return_value = {"items": [], "error": None, "meta": {}}
        news_mock.return_value = {"items": [], "error": None, "meta": {}}
        response = self.client.get("/", HTTP_HOST="sustainacore.org")
        self.assertEqual(response.status_code, 200)
        self.assertNotContains(response, "Preview environment")
        self.assertNotContains(response, "PREVIEW")

    @mock.patch.dict(os.environ, {"TECH100_UI_DATA_MODE": "fixture"})
    @mock.patch("core.views.fetch_news")
    @mock.patch("core.views.fetch_tech100")
    @override_settings(SUSTAINACORE_ENV="production", PREVIEW_MODE=False)
    def test_banner_shows_on_preview_host(self, tech100_mock, news_mock):
        tech100_mock.return_value = {"items": [], "error": None, "meta": {}}
        news_mock.return_value = {"items": [], "error": None, "meta": {}}
        response = self.client.get("/", HTTP_HOST="preview.sustainacore.org")
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Preview environment")
        self.assertContains(response, "PREVIEW")
