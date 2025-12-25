import datetime as _dt
import importlib.util
import pathlib
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "app" / "providers" / "market_data_provider.py"
spec = importlib.util.spec_from_file_location("market_data_provider_test", MODULE_PATH)
if spec is None or spec.loader is None:
    raise RuntimeError("unable to load market_data_provider module for tests")
market_data_provider = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = market_data_provider
spec.loader.exec_module(market_data_provider)  # type: ignore[arg-type]


def test_fetch_latest_eod_date_parses_values(monkeypatch):
    monkeypatch.setattr(market_data_provider, "_get_api_key", lambda explicit=None: "demo")
    monkeypatch.setattr(
        market_data_provider,
        "_request_json",
        lambda path, params: {"values": [{"datetime": "2025-01-03"}]},
    )
    assert market_data_provider.fetch_latest_eod_date("SPY") == _dt.date(2025, 1, 3)


def test_fetch_latest_eod_date_raises_on_empty(monkeypatch):
    monkeypatch.setattr(market_data_provider, "_get_api_key", lambda explicit=None: "demo")
    monkeypatch.setattr(market_data_provider, "_request_json", lambda path, params: {"values": []})
    with pytest.raises(market_data_provider.MarketDataProviderError):
        market_data_provider.fetch_latest_eod_date("SPY")
