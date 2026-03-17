import datetime as dt
import os
from unittest import mock

from django.core.cache import cache
from django.test import SimpleTestCase, override_settings

from core import tech100_portfolio_data as data


@override_settings(
    CACHES={
        "default": {
            "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
            "LOCATION": "tech100-portfolio-test",
        }
    }
)
class Tech100PortfolioDataTests(SimpleTestCase):
    def setUp(self):
        cache.clear()

    def test_get_snapshot_rows_uses_trade_date_param(self):
        trade_date = dt.date(2026, 3, 16)
        with mock.patch("core.tech100_portfolio_data._safe_execute_rows", return_value=[]) as exec_mock:
            data.get_snapshot_rows(trade_date)
        sql, params = exec_mock.call_args.args
        self.assertIn(":trade_date", sql)
        self.assertEqual(params["trade_date"], trade_date)

    def test_get_position_rows_uses_model_and_date_params(self):
        trade_date = dt.date(2026, 3, 16)
        with mock.patch("core.tech100_portfolio_data._safe_execute_rows", return_value=[]) as exec_mock:
            data.get_position_rows(trade_date=trade_date, model_code="TECH100_GOV")
        _, params = exec_mock.call_args.args
        self.assertEqual(params["trade_date"], trade_date)
        self.assertEqual(params["model_code"], "TECH100_GOV")

    @mock.patch.dict(os.environ, {"TECH100_UI_DATA_MODE": "fixture"})
    def test_fixture_mode_returns_populated_snapshot(self):
        latest = data.get_latest_trade_date()
        snapshot = data.get_snapshot_rows(latest)
        positions = data.get_position_rows(trade_date=latest, model_code="TECH100_GOV")
        sectors = data.get_sector_rows(trade_date=latest, model_code="TECH100_GOV")
        timeseries = data.build_timeseries_payload(data.get_timeseries_rows())
        self.assertEqual(latest, data.FIXTURE_LATEST_DATE)
        self.assertEqual(len(snapshot), len(data.MODEL_ORDER))
        self.assertGreaterEqual(len(positions), 10)
        self.assertGreaterEqual(len(sectors), 3)
        self.assertGreaterEqual(len(timeseries["TECH100"]), 60)
