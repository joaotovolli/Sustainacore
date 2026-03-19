import datetime as dt
import os
from unittest import mock

from django.core.cache import cache
from django.test import SimpleTestCase, override_settings
from django.urls import reverse


@override_settings(TELEMETRY_WRITE_ENABLED=False)
class Tech100PortfolioViewTests(SimpleTestCase):
    def setUp(self):
        cache.clear()

    @mock.patch.dict(os.environ, {"TECH100_UI_DATA_MODE": "fixture"})
    @mock.patch("core.tech100_portfolio_views.get_index_latest_trade_date", return_value=dt.date(2026, 3, 16))
    def test_portfolio_view_renders_fixture_mode(self, _index_latest_mock):
        response = self.client.get(reverse("tech100_portfolio"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "TECH100 Portfolio Analytics")
        self.assertContains(response, "data-tech100-portfolio-has-data")
        self.assertContains(response, "Daily model tape")
        self.assertContains(response, "Analytics workspace")
        self.assertContains(response, "Compare against")
        self.assertContains(response, 'id="portfolio-effective-date"', count=1, html=False)
        self.assertContains(response, "As of")
        self.assertContains(response, "Comparison table and factor lens")
        self.assertContains(response, "Holdings, attribution, and sector tilt")
        self.assertNotContains(response, "Data source:")
        self.assertNotContains(response, "Model matrix")
        self.assertNotContains(response, "Portfolio analytics through")
        self.assertNotContains(response, "Official TECH100 index through")
        self.assertNotContains(response, "trail the official TECH100 index")
        self.assertEqual(response.context["freshness_gap_days"], 0)
        self.assertIsNone(response.context["freshness_warning"])

    @mock.patch.dict(os.environ, {"TECH100_UI_DATA_MODE": "fixture"})
    @mock.patch("core.tech100_portfolio_views.get_index_latest_trade_date", return_value=dt.date(2026, 3, 17))
    def test_portfolio_view_shows_compact_warning_when_index_is_ahead(self, _index_latest_mock):
        response = self.client.get(reverse("tech100_portfolio"))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["freshness_gap_days"], 1)
        self.assertContains(response, 'id="portfolio-freshness-warning"', count=1, html=False)
        self.assertContains(response, "Daily portfolio refresh is pending.")
        self.assertContains(response, 'id="portfolio-effective-date"', count=1, html=False)
        self.assertNotContains(response, "Official TECH100 index through")
        self.assertNotContains(response, "trail the official TECH100 index")

    @mock.patch.dict(os.environ, {"TECH100_UI_DATA_MODE": "fixture"})
    def test_portfolio_view_accepts_model_selector(self):
        response = self.client.get(f"{reverse('tech100_portfolio')}?model=TECH100_GOV")
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Governance Tilt")
        self.assertContains(response, 'data-selected-model="TECH100_GOV"', html=False)

    @mock.patch("core.tech100_portfolio_views.get_latest_trade_date", return_value=None)
    @mock.patch("core.tech100_portfolio_views.get_snapshot_rows", return_value=[])
    def test_portfolio_view_handles_empty_state(self, _snapshot_mock, _latest_mock):
        response = self.client.get(reverse("tech100_portfolio"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Portfolio analytics are not populated yet")
