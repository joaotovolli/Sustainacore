import datetime as _dt
import json
import urllib.error

import app.providers.twelvedata as tw
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
            raise urllib.error.HTTPError(request.full_url, 429, "Too Many Requests", None, None)

        class Dummy:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self):
                return json.dumps(success_payload).encode("utf-8")

        return Dummy()

    monkeypatch.setenv("TWELVEDATA_API_KEY", "test-key")
    monkeypatch.setattr(tw.urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(tw, "_acquire_token", lambda: None)
    monkeypatch.setattr(tw, "_sleep_until_reset", lambda: None)

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
            "provider": "TWELVEDATA",
            "close_px": 10.0,
            "adj_close_px": 10.0,
            "status": "ERROR",
        }
    ]

    canon_rows = compute_canonical_rows(raw_rows)

    assert canon_rows == []
