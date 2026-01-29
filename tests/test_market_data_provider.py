import datetime as _dt
import io
import json
import urllib.error
from contextlib import contextmanager

import app.providers.market_data_provider as tw
from tools.index_engine.ingest_prices import compute_canonical_rows


def test_parse_rows_adjust_all_sets_adj_close_equal_close():
    payload = {
        "meta": {"symbol": "ABC", "currency": "USD"},
        "values": [
            {
                "datetime": "2025-01-02 00:00:00",
                "close": "12.34",
                "volume": "100",
            }
        ],
    }

    rows = tw._parse_rows(payload, "ABC")

    assert rows == [
        {
            "ticker": "ABC",
            "trade_date": "2025-01-02",
            "close": 12.34,
            "adj_close": 12.34,
            "volume": 100,
            "currency": "USD",
        }
    ]


def test_fetch_eod_prices_retries_on_rate_limit(monkeypatch):
    success_payload = {
        "meta": {"symbol": "XYZ", "currency": "USD"},
        "values": [
            {"datetime": "2025-01-02", "close": "10", "volume": "5"},
        ],
    }

    calls = {"count": 0}

    def fake_urlopen(request, timeout=30):
        calls["count"] += 1
        if calls["count"] == 1:
            raise urllib.error.HTTPError(request.full_url, 429, "Too Many Requests", None, io.BytesIO(b"{}"))

        class Dummy:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self):
                return json.dumps(success_payload).encode("utf-8")

        return Dummy()

    monkeypatch.setenv("MARKET_DATA_API_KEY", "test-key")
    monkeypatch.setenv("MARKET_DATA_API_BASE_URL", "https://example.test")
    monkeypatch.setattr(tw, "_urlopen", fake_urlopen)
    monkeypatch.setattr(tw, "_acquire_token_blocking", lambda: None)
    monkeypatch.setattr(tw, "_sleep_until_window_reset", lambda: None)

    @contextmanager
    def _no_lock():
        yield

    monkeypatch.setattr(tw, "_provider_lock", _no_lock)

    rows = tw.fetch_eod_prices(["XYZ"], "2025-01-02", "2025-01-02")

    assert calls["count"] == 2
    assert len(rows) == 1
    assert rows[0]["ticker"] == "XYZ"
    assert rows[0]["adj_close"] == 10.0


def test_compute_canonical_skips_non_ok_rows():
    raw_rows = [
        {
            "ticker": "ABC",
            "trade_date": _dt.date.fromisoformat("2025-01-02"),
            "provider": "MARKET_DATA",
            "close_px": 10.0,
            "adj_close_px": 10.0,
            "status": "ERROR",
        }
    ]

    canon_rows = compute_canonical_rows(raw_rows)

    assert canon_rows == []


def test_throttle_env_overrides(monkeypatch):
    monkeypatch.setenv("SC_IDX_MARKET_DATA_CALLS_PER_WINDOW", "5")
    monkeypatch.setenv("SC_IDX_MARKET_DATA_WINDOW_SECONDS", "90")
    cfg = tw.get_throttle_config(refresh=True)
    assert cfg["calls_per_window"] == 5
    assert cfg["window_seconds"] == 90


def test_fetch_api_usage_and_latest_bar_use_throttle(monkeypatch):
    monkeypatch.setenv("MARKET_DATA_API_KEY", "test-key")
    monkeypatch.setenv("MARKET_DATA_API_BASE_URL", "https://example.test")
    calls = {"count": 0}

    def fake_throttled(request, **kwargs):
        calls["count"] += 1
        url = request.full_url if hasattr(request, "full_url") else ""
        if "api_usage" in url:
            return {
                "timestamp": "now",
                "current_usage": 1,
                "plan_limit": 8,
                "plan_category": "basic",
            }
        return {"values": [{"datetime": "2025-12-12 00:00:00", "close": "1"}]}

    monkeypatch.setattr(tw, "_throttled_json_request", fake_throttled)

    usage = tw.fetch_api_usage()
    latest = tw.fetch_latest_bar("AAPL")

    assert calls["count"] == 2
    assert usage["current_usage"] == 1
    assert latest[0]["datetime"].startswith("2025-12-12")


def test_request_json_adds_user_agent(monkeypatch):
    monkeypatch.setenv("MARKET_DATA_API_KEY", "test-key")
    monkeypatch.setenv("MARKET_DATA_API_BASE_URL", "https://example.test")
    captured = {}

    def fake_throttled(request, **kwargs):
        captured["ua"] = (
            request.get_header("User-agent")
            or request.headers.get("User-Agent")
            or request.headers.get("User-agent")
        )
        return {
            "timestamp": "now",
            "current_usage": 1,
            "plan_limit": 8,
            "plan_category": "basic",
        }

    monkeypatch.setattr(tw, "_throttled_json_request", fake_throttled)

    tw.fetch_api_usage()

    assert captured["ua"] == "sustainacore-index-engine"


def test_redact_url_removes_api_key():
    url = "https://example.test/api_usage?symbol=SPY&apikey=secret-token"
    redacted = tw._redact_url(url)

    assert "secret-token" not in redacted
    assert "apikey=REDACTED" in redacted
