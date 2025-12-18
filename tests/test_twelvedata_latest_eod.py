import datetime as _dt
import importlib.util
import pathlib
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "app" / "providers" / "twelvedata.py"
spec = importlib.util.spec_from_file_location("twelvedata_test", MODULE_PATH)
if spec is None or spec.loader is None:
    raise RuntimeError("unable to load twelvedata module for tests")
twelvedata = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = twelvedata
spec.loader.exec_module(twelvedata)  # type: ignore[arg-type]


def test_fetch_latest_eod_date_parses_values(monkeypatch):
    monkeypatch.setattr(twelvedata, "_get_api_key", lambda explicit=None: "demo")
    monkeypatch.setattr(
        twelvedata,
        "_request_json",
        lambda path, params: {"values": [{"datetime": "2025-01-03"}]},
    )
    assert twelvedata.fetch_latest_eod_date("SPY") == _dt.date(2025, 1, 3)


def test_fetch_latest_eod_date_raises_on_empty(monkeypatch):
    monkeypatch.setattr(twelvedata, "_get_api_key", lambda explicit=None: "demo")
    monkeypatch.setattr(twelvedata, "_request_json", lambda path, params: {"values": []})
    with pytest.raises(twelvedata.TwelveDataError):
        twelvedata.fetch_latest_eod_date("SPY")
