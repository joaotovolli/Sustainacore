import datetime as dt

import app.providers.twelvedata as twelvedata
from tools.index_engine.run_daily import _probe_with_fallback


def test_probe_with_fallback_skips_after_no_data(monkeypatch):
    trading_days = [dt.date(2025, 1, 1), dt.date(2025, 1, 2), dt.date(2025, 1, 3)]
    order = {trading_days[2]: "NO_DATA", trading_days[1]: "NO_DATA", trading_days[0]: "NO_DATA"}
    called = []

    def fake_probe(d):
        called.append(d)
        return order[d]

    chosen, tried = _probe_with_fallback(trading_days[2], trading_days, probe_fn=fake_probe)
    assert chosen is None
    assert tried == [trading_days[2], trading_days[1], trading_days[0]]
    assert called == tried


def test_fetch_single_day_bar_selects_descending(monkeypatch):
    rows = [
        {"datetime": "2025-01-03", "close": "3"},
        {"datetime": "2025-01-02", "close": "2"},
        {"datetime": "2025-01-01", "close": "1"},
    ]
    monkeypatch.setattr(twelvedata, "fetch_daily_window_desc", lambda *a, **k: rows)
    result = twelvedata.fetch_single_day_bar("SPY", dt.date(2025, 1, 2))
    assert len(result) == 1
    assert result[0]["trade_date"] == "2025-01-02"


def test_fetch_eod_prices_single_day_uses_desc(monkeypatch):
    rows = [
        {"datetime": "2025-01-02", "close": "2"},
    ]
    called_desc = {"count": 0}

    def fake_desc(*args, **kwargs):
        called_desc["count"] += 1
        return rows

    def boom(*args, **kwargs):
        raise AssertionError("should not call _fetch_ticker for single-day")

    monkeypatch.setattr(twelvedata, "fetch_daily_window_desc", fake_desc)
    monkeypatch.setattr(twelvedata, "_fetch_ticker", boom)

    result = twelvedata.fetch_eod_prices(["SPY"], "2025-01-02", "2025-01-02")
    assert called_desc["count"] == 1
    assert len(result) == 1
    assert result[0]["trade_date"] == "2025-01-02"
