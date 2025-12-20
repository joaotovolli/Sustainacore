import datetime as dt
import os
from unittest import mock

from django.core.cache import cache
from django.test import SimpleTestCase, override_settings

from core import tech100_index_data as data


@override_settings(
    CACHES={"default": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache", "LOCATION": "tech100-test"}}
)
class Tech100IndexDataTests(SimpleTestCase):
    def setUp(self):
        cache.clear()

    def test_get_index_levels_uses_parameterized_dates(self):
        start = dt.date(2025, 1, 1)
        end = dt.date(2025, 2, 1)
        with mock.patch("core.tech100_index_data._execute_rows", return_value=[]) as exec_mock:
            data.get_index_levels(start, end)
        sql, params = exec_mock.call_args.args
        self.assertIn(":start_date", sql)
        self.assertIn(":end_date", sql)
        self.assertEqual(params["start_date"], start)
        self.assertEqual(params["end_date"], end)
        self.assertEqual(params["index_code"], data.INDEX_CODE)

    def test_get_constituents_uses_trade_date_param(self):
        trade_date = dt.date(2025, 3, 15)
        with mock.patch("core.tech100_index_data._execute_rows", return_value=[]) as exec_mock:
            data.get_constituents(trade_date)
        _, params = exec_mock.call_args.args
        self.assertEqual(params["trade_date"], trade_date)

    def test_get_holdings_uses_trade_date_param(self):
        trade_date = dt.date(2025, 4, 1)
        with mock.patch("core.tech100_index_data._execute_rows", return_value=[]) as exec_mock:
            data.get_holdings_with_meta(trade_date)
        _, params = exec_mock.call_args.args
        self.assertEqual(params["trade_date"], trade_date)

    @mock.patch.dict(os.environ, {"TECH100_UI_DATA_MODE": "fixture"})
    def test_fixture_mode_returns_levels_and_constituents(self):
        levels = data.get_index_levels()
        constituents = data.get_constituents(data.FIXTURE_LATEST_DATE)
        self.assertGreaterEqual(len(levels), 10)
        self.assertGreaterEqual(len(constituents), 20)

    @mock.patch.dict(os.environ, {"TECH100_UI_DATA_MODE": "fixture"})
    def test_fixture_mode_supports_performance_helpers(self):
        counts = data.get_quality_counts(data.FIXTURE_LATEST_DATE)
        holdings = data.get_holdings_with_meta(data.FIXTURE_LATEST_DATE)
        attribution = data.get_attribution_table(
            data.FIXTURE_LATEST_DATE,
            data.FIXTURE_LATEST_DATE - dt.timedelta(days=30),
            data.FIXTURE_LATEST_DATE - dt.timedelta(days=365),
        )
        self.assertGreaterEqual(counts.get("REAL", 0), 1)
        self.assertGreaterEqual(len(holdings), 10)
        self.assertGreaterEqual(len(attribution), 10)
