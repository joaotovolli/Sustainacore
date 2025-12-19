import datetime as dt
import os
from unittest import mock

from django.test import SimpleTestCase
from django.urls import reverse

from core.tech100_index_data import DrawdownResult


class Tech100IndexViewTests(SimpleTestCase):
    @mock.patch("core.tech100_index_views.get_latest_trade_date")
    @mock.patch("core.tech100_index_views.get_index_levels")
    @mock.patch("core.tech100_index_views.get_index_returns")
    @mock.patch("core.tech100_index_views.get_kpis")
    @mock.patch("core.tech100_index_views.get_stats")
    @mock.patch("core.tech100_index_views.get_rolling_vol")
    @mock.patch("core.tech100_index_views.get_max_drawdown")
    @mock.patch("core.tech100_index_views.get_constituents")
    @mock.patch("core.tech100_index_views.get_imputed_overview")
    def test_overview_renders(
        self,
        imputed_mock,
        constituents_mock,
        drawdown_mock,
        vol_mock,
        stats_mock,
        kpis_mock,
        returns_mock,
        levels_mock,
        latest_mock,
    ):
        latest = dt.date(2025, 3, 5)
        latest_mock.return_value = latest
        levels_mock.return_value = [(latest, 1234.5)]
        returns_mock.return_value = [(latest, 0.01)]
        kpis_mock.return_value = {"level": 1234.5, "ret_1d": 0.01}
        stats_mock.return_value = {"n_imputed": 2}
        vol_mock.return_value = 0.15
        drawdown_mock.return_value = DrawdownResult(-0.1, latest, latest)
        constituents_mock.return_value = []
        imputed_mock.return_value = []

        response = self.client.get(reverse("tech100_index"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Performance &amp; Risk")

    @mock.patch.dict(os.environ, {"TECH100_UI_DATA_MODE": "fixture"})
    def test_overview_fixture_mode_has_data_markers(self):
        response = self.client.get(reverse("tech100_index"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "data-level-count")
        self.assertContains(response, "data-constituent-count")

    @mock.patch("core.tech100_index_views.get_latest_trade_date")
    @mock.patch("core.tech100_index_views.get_constituents")
    @mock.patch("core.tech100_index_views.get_trade_date_bounds")
    def test_constituents_view_ok(self, bounds_mock, constituents_mock, latest_mock):
        latest_mock.return_value = dt.date(2025, 3, 5)
        bounds_mock.return_value = (dt.date(2025, 1, 1), dt.date(2025, 3, 5))
        constituents_mock.return_value = []
        response = self.client.get(reverse("tech100_constituents"))
        self.assertEqual(response.status_code, 200)

    @mock.patch("core.tech100_index_views.get_latest_trade_date")
    @mock.patch("core.tech100_index_views.get_index_levels")
    def test_api_index_levels(self, levels_mock, latest_mock):
        latest_mock.return_value = dt.date(2025, 3, 5)
        levels_mock.return_value = [(dt.date(2025, 3, 5), 1234.5)]
        response = self.client.get("/api/tech100/index-levels?range=1m")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("levels", payload)
        self.assertEqual(payload["levels"][0]["level"], 1234.5)
