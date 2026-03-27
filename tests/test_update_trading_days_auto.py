import datetime as _dt
import importlib.util
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "tools" / "index_engine" / "update_trading_days.py"
spec = importlib.util.spec_from_file_location("update_trading_days_test", MODULE_PATH)
if spec is None or spec.loader is None:
    raise RuntimeError("unable to load update_trading_days module for tests")
update_module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = update_module
spec.loader.exec_module(update_module)  # type: ignore[arg-type]


class _Provider:
    def __init__(self, latest):
        self.latest = latest
        self.seen_start = None
        self.seen_end = None

    def fetch_latest_eod_date(self, ticker):
        return self.latest

    def fetch_time_series(self, ticker, start, end):
        self.seen_start = start
        self.seen_end = end
        return [{"datetime": start.isoformat()}, {"datetime": end.isoformat()}]


def test_auto_extend_starts_after_max(monkeypatch):
    latest = _dt.date(2025, 12, 17)
    provider = _Provider(latest)

    monkeypatch.setattr(update_module, "_load_provider_module", lambda: provider)
    monkeypatch.setattr(update_module, "fetch_latest_trading_day", lambda: _dt.date(2025, 12, 16))
    monkeypatch.setattr(update_module, "upsert_trading_days", lambda dates, source: len(dates))
    monkeypatch.setattr(update_module, "_fetch_total_count", lambda: 10)

    inserted, total, latest_eod, max_before, max_after = update_module.update_trading_days(
        None, auto_extend=True
    )
    assert inserted == 1
    assert total == 10
    assert latest_eod == latest
    assert max_before == _dt.date(2025, 12, 16)
    assert provider.seen_start == _dt.date(2025, 12, 17)
    assert provider.seen_end == _dt.date(2025, 12, 17)


def test_auto_extend_falls_back_to_window(monkeypatch):
    latest = _dt.date(2026, 1, 2)

    class _ProviderEmpty:
        def __init__(self):
            self.window = None

        def fetch_latest_eod_date(self, ticker):
            return latest

        def fetch_time_series(self, ticker, start, end):
            return []

        def fetch_daily_window_desc(self, ticker, window):
            self.window = window
            return [{"datetime": latest.isoformat()}]

    provider = _ProviderEmpty()
    calls = {"count": 0}

    def fake_fetch_latest_trading_day():
        calls["count"] += 1
        return _dt.date(2025, 12, 31) if calls["count"] == 1 else latest

    monkeypatch.setattr(update_module, "_load_provider_module", lambda: provider)
    monkeypatch.setattr(update_module, "fetch_latest_trading_day", fake_fetch_latest_trading_day)
    monkeypatch.setattr(update_module, "upsert_trading_days", lambda dates, source: len(dates))
    monkeypatch.setattr(update_module, "_fetch_total_count", lambda: 10)

    inserted, total, latest_eod, max_before, max_after = update_module.update_trading_days(
        None, auto_extend=True
    )
    assert inserted == 1
    assert total == 10
    assert latest_eod == latest
    assert max_before == _dt.date(2025, 12, 31)
    assert max_after == latest
    assert provider.window is not None


def test_update_trading_days_retry_falls_back_on_timeout(monkeypatch):
    monkeypatch.setattr(
        update_module,
        "update_trading_days",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("The read operation timed out")),
    )
    monkeypatch.setattr(update_module, "_sleep_backoff", lambda *_args, **_kwargs: None)

    updated, reason = update_module.update_trading_days_with_retry(
        auto_extend=True,
        max_attempts=2,
        backoff_base_sec=0.0,
    )

    assert updated is False
    assert reason == "market_data_timeout"
