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
    def test_portfolio_view_renders_fixture_mode(self):
        response = self.client.get(reverse("tech100_portfolio"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "TECH100 Portfolio Analytics")
        self.assertContains(response, "data-tech100-portfolio-has-data")
        self.assertContains(response, "Portfolio analytics workspace")
        self.assertContains(response, "Compare against")
        self.assertNotContains(response, "Data source:")
        self.assertNotContains(response, "Models covered")

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
