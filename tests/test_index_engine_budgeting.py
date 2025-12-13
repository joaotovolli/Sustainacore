import datetime as dt
import importlib.util
import json
from pathlib import Path


def _load_provider():
    module_path = Path(__file__).resolve().parent.parent / "app" / "providers" / "twelvedata.py"
    spec = importlib.util.spec_from_file_location("twelvedata_test_provider", module_path)
    if spec is None or spec.loader is None:
        raise ImportError("Unable to import Twelve Data provider for tests")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[arg-type]
    return module


twelvedata = _load_provider()
def _load_ingest():
    module_path = Path(__file__).resolve().parent.parent / "tools" / "index_engine" / "ingest_prices.py"
    spec = importlib.util.spec_from_file_location("ingest_prices_test", module_path)
    if spec is None or spec.loader is None:
        raise ImportError("Unable to import ingest_prices for tests")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[arg-type]
    return module


ingest_prices = _load_ingest()


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def read(self):
        return json.dumps(self._payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def test_fetch_api_usage_parsing_and_remaining(monkeypatch):
    monkeypatch.setenv("TWELVEDATA_API_KEY", "test-key")
    payload = {
        "status": "ok",
        "plan_limit": 800,
        "current_usage": 12,
        "plan_category": "individual",
        "plan_id": "358001bd-cab0-429a-a8ff-524fbfd0001",
        "timestamp": "2025-09-26T23:30:00+00:00",
    }

    monkeypatch.setattr(twelvedata, "_urlopen", lambda req, timeout=30: _FakeResponse(payload))

    usage = twelvedata.fetch_api_usage()
    assert usage["timestamp"] == payload["timestamp"]
    assert usage["current_usage"] == payload["current_usage"]
    assert usage["plan_limit"] == payload["plan_limit"]
    assert usage["plan_category"] == payload["plan_category"]
    assert twelvedata.remaining_credits(usage) == payload["plan_limit"] - payload["current_usage"]


def test_backfill_halts_when_budget_reached(tmp_path: Path, capsys):
    start = dt.date(2025, 1, 1)
    end = dt.date(2025, 1, 5)
    calls = []

    def _fake_fetch(ticker: str, start_date: dt.date, end_date: dt.date):
        calls.append(ticker)
        return [{"datetime": start_date.isoformat(), "close": "1"}]

    summary = ingest_prices.backfill_prices(
        ["AAA", "BBB", "CCC"],
        start,
        end,
        max_provider_calls=2,
        fetcher=_fake_fetch,
        data_dir=tmp_path,
        sleep_seconds=0,
    )
    captured = capsys.readouterr()

    assert summary["provider_calls_used"] == 2
    assert calls == ["AAA", "BBB"]
    assert "budget_stop: provider_calls_used=2 max_provider_calls=2" in captured.out
    assert (tmp_path / "AAA.jsonl").exists()
    assert (tmp_path / "BBB.jsonl").exists()
    assert not (tmp_path / "CCC.jsonl").exists()


def test_has_eod_for_date_true(monkeypatch):
    target = dt.date(2025, 9, 26)
    payload = {"values": [{"datetime": target.isoformat(), "close": "100"}]}
    monkeypatch.setenv("TWELVEDATA_API_KEY", "test-key")
    monkeypatch.setattr(twelvedata, "_urlopen", lambda req, timeout=30: _FakeResponse(payload))

    assert twelvedata.has_eod_for_date("AAPL", target)


def test_has_eod_for_date_no_data(monkeypatch):
    target = dt.date(2025, 9, 26)
    payload = {"status": "error", "message": "No data is available for the selected interval"}
    monkeypatch.setenv("TWELVEDATA_API_KEY", "test-key")
    monkeypatch.setattr(twelvedata, "_urlopen", lambda req, timeout=30: _FakeResponse(payload))

    assert not twelvedata.has_eod_for_date("AAPL", target)
