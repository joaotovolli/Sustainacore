import datetime as dt
import io
import urllib.error
import urllib.request

import app.providers.market_data_provider as market_data_provider
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
    monkeypatch.setattr(market_data_provider, "_get_api_key", lambda explicit=None: "demo")
    monkeypatch.setattr(market_data_provider, "fetch_daily_window_desc", lambda *a, **k: rows)
    result = market_data_provider.fetch_single_day_bar("SPY", dt.date(2025, 1, 2))
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

    monkeypatch.setattr(market_data_provider, "_get_api_key", lambda explicit=None: "demo")
    monkeypatch.setattr(market_data_provider, "fetch_daily_window_desc", fake_desc)
    monkeypatch.setattr(market_data_provider, "_fetch_ticker", boom)

    result = market_data_provider.fetch_eod_prices(["SPY"], "2025-01-02", "2025-01-02")
    assert called_desc["count"] == 1
    assert len(result) == 1
    assert result[0]["trade_date"] == "2025-01-02"


def test_fetch_api_usage_separates_daily_credits_from_minute_capacity(monkeypatch):
    monkeypatch.setattr(market_data_provider, "_get_api_key", lambda explicit=None: "demo")
    monkeypatch.setattr(
        market_data_provider,
        "_request_json",
        lambda *_args, **_kwargs: {
            "daily_current_usage": 2,
            "daily_plan_limit": 800,
            "current_usage": 1,
            "plan_limit": 8,
        },
    )

    usage = market_data_provider.fetch_api_usage()

    assert usage["current_usage"] == 2
    assert usage["plan_limit"] == 800
    assert usage["daily_current_usage"] == 2
    assert usage["daily_plan_limit"] == 800
    assert usage["minute_current_usage"] == 1
    assert usage["minute_plan_limit"] == 8


def test_fetch_api_usage_does_not_map_minute_limit_to_daily_credits(monkeypatch):
    monkeypatch.setattr(market_data_provider, "_get_api_key", lambda explicit=None: "demo")
    monkeypatch.setattr(
        market_data_provider,
        "_request_json",
        lambda *_args, **_kwargs: {"current_usage": 1, "plan_limit": 8},
    )

    usage = market_data_provider.fetch_api_usage()

    assert usage["current_usage"] is None
    assert usage["plan_limit"] is None
    assert usage["daily_current_usage"] is None
    assert usage["daily_plan_limit"] is None
    assert usage["minute_current_usage"] == 1
    assert usage["minute_plan_limit"] == 8


def test_provider_http_429_fails_without_sleep_retry(monkeypatch):
    calls = {"urlopen": 0, "sleep": 0}

    def fake_urlopen(*_args, **_kwargs):
        calls["urlopen"] += 1
        raise urllib.error.HTTPError(
            url="https://example.invalid",
            code=429,
            msg="rate limited",
            hdrs={},
            fp=io.BytesIO(b"{}"),
        )

    monkeypatch.setattr(market_data_provider, "_urlopen", fake_urlopen)
    monkeypatch.setattr(market_data_provider, "_acquire_token_blocking", lambda: None)
    monkeypatch.setattr(market_data_provider, "_sleep_until_window_reset", lambda: calls.__setitem__("sleep", calls["sleep"] + 1))
    monkeypatch.setattr(market_data_provider, "_sleep_backoff", lambda *_args, **_kwargs: calls.__setitem__("sleep", calls["sleep"] + 1))

    request = urllib.request.Request("https://example.invalid")
    try:
        market_data_provider._throttled_json_request(request, max_retries=3)
    except RuntimeError as exc:
        assert str(exc) == "market_data_http_error:429"
    else:
        raise AssertionError("expected provider 429 error")

    assert calls == {"urlopen": 1, "sleep": 0}


def test_provider_payload_rate_limit_fails_without_sleep_retry(monkeypatch):
    calls = {"urlopen": 0, "sleep": 0}

    class _Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self):
            return b'{"status":"error","message":"credit limit reached"}'

    def fake_urlopen(*_args, **_kwargs):
        calls["urlopen"] += 1
        return _Response()

    monkeypatch.setattr(market_data_provider, "_urlopen", fake_urlopen)
    monkeypatch.setattr(market_data_provider, "_acquire_token_blocking", lambda: None)
    monkeypatch.setattr(market_data_provider, "_sleep_until_window_reset", lambda: calls.__setitem__("sleep", calls["sleep"] + 1))
    monkeypatch.setattr(market_data_provider, "_sleep_backoff", lambda *_args, **_kwargs: calls.__setitem__("sleep", calls["sleep"] + 1))

    request = urllib.request.Request("https://example.invalid")
    try:
        market_data_provider._throttled_json_request(request, max_retries=3)
    except RuntimeError as exc:
        assert str(exc) == "market_data_rate_limited"
    else:
        raise AssertionError("expected provider rate-limit error")

    assert calls == {"urlopen": 1, "sleep": 0}
