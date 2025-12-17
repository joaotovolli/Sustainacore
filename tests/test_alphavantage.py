import importlib.util
import json
import pathlib
from contextlib import contextmanager

import pytest

def _load_module():
    path = pathlib.Path(__file__).resolve().parents[1] / "app" / "providers" / "alphavantage.py"
    spec = importlib.util.spec_from_file_location("app.providers.alphavantage", path)
    module = importlib.util.module_from_spec(spec)
    if spec.loader is None:
        raise ImportError("unable to load alphavantage provider module")
    spec.loader.exec_module(module)
    return module


av = _load_module()


def test_parse_rows_daily_adjusted_payload():
    payload = {
        "Time Series (Daily)": {
            "2025-01-02": {
                "1. open": "10",
                "2. high": "12",
                "3. low": "9",
                "4. close": "11.5",
                "5. adjusted close": "11.2",
                "6. volume": "1000",
            }
        }
    }

    rows = av._parse_rows(payload, "ACME")

    assert rows == [
        {
            "ticker": "ACME",
            "trade_date": "2025-01-02",
            "close": 11.5,
            "adj_close": 11.2,
            "volume": 1000,
            "currency": None,
        }
    ]


def _noop_lock():
    @contextmanager
    def _ctx():
        yield

    return _ctx()


def test_fetch_daily_adjusted_retries_on_note(monkeypatch):
    responses = [
        {"Note": "limit reached"},
        {
            "Time Series (Daily)": {
                "2025-01-02": {
                    "4. close": "100",
                    "5. adjusted close": "99.5",
                    "6. volume": "500",
                }
            }
        },
    ]
    calls = {"count": 0}

    def fake_urlopen(request, timeout=30):
        payload = responses[min(calls["count"], len(responses) - 1)]
        calls["count"] += 1

        class Dummy:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self):
                return json.dumps(payload).encode("utf-8")

        return Dummy()

    monkeypatch.setenv("ALPHAVANTAGE_API_KEY", "test-key")
    monkeypatch.setattr(av, "_urlopen", fake_urlopen)
    monkeypatch.setattr(av, "_provider_lock", lambda: _noop_lock())
    monkeypatch.setattr(av, "_acquire_token_blocking", lambda: None)
    monkeypatch.setattr(av, "_sleep_until_window_reset", lambda: None)

    rows = av.fetch_daily_adjusted("acme", outputsize="compact")

    assert calls["count"] == 2
    assert rows[0]["ticker"] == "ACME"
    assert rows[0]["adj_close"] == 99.5


def test_fetch_daily_adjusted_note_fails_after_retries(monkeypatch):
    calls = {"count": 0}

    def fake_urlopen(request, timeout=30):
        calls["count"] += 1

        class Dummy:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self):
                return json.dumps({"Note": "limit reached"}).encode("utf-8")

        return Dummy()

    monkeypatch.setenv("ALPHAVANTAGE_API_KEY", "test-key")
    monkeypatch.setattr(av, "_urlopen", fake_urlopen)
    monkeypatch.setattr(av, "_provider_lock", lambda: _noop_lock())
    monkeypatch.setattr(av, "_acquire_token_blocking", lambda: None)
    monkeypatch.setattr(av, "_sleep_until_window_reset", lambda: None)

    with pytest.raises(RuntimeError, match="limit reached"):
        av.fetch_daily_adjusted("acme", outputsize="compact")

    assert calls["count"] >= av.MAX_RETRIES


def test_full_history_unavailable_detects_compact_payload(monkeypatch):
    payload = {"Time Series (Daily)": {}}
    for i in range(100):
        date = (i + 1)
        trade_date = f"2025-01-{date:02d}"
        payload["Time Series (Daily)"][trade_date] = {
            "4. close": "10",
            "5. adjusted close": "10",
            "6. volume": "1",
        }

    monkeypatch.setenv("ALPHAVANTAGE_API_KEY", "test-key")
    monkeypatch.setattr(av, "_throttled_json_request", lambda request, **kwargs: payload)

    with pytest.raises(RuntimeError, match="alphavantage_full_history_unavailable_free_tier"):
        av.fetch_daily_adjusted("AAPL", outputsize="full")
