import argparse
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


def test_backfill_halts_when_budget_reached(monkeypatch, capsys):
    args = argparse.Namespace(
        start="2025-01-01",
        end="2025-01-05",
        tickers="AAA,BBB,CCC",
        max_provider_calls=2,
        backfill=True,
        debug=False,
        date=None,
    )

    calls: list[str] = []

    def _fake_fetch_eod_prices(tickers, start, end):
        calls.append(tickers[0])
        return [{"ticker": tickers[0], "trade_date": start, "close": 1.0, "adj_close": 1.0}]

    monkeypatch.setattr(ingest_prices, "fetch_eod_prices", _fake_fetch_eod_prices)
    monkeypatch.setattr(ingest_prices, "fetch_max_ok_trade_date", lambda ticker, provider: None)
    monkeypatch.setattr(ingest_prices, "upsert_prices_raw", lambda rows: len(rows))
    monkeypatch.setattr(ingest_prices, "upsert_prices_canon", lambda rows: len(rows))
    monkeypatch.setattr(ingest_prices, "compute_canonical_rows", lambda rows: rows)

    exit_code = ingest_prices._run_backfill(args)
    captured = capsys.readouterr().out

    assert exit_code == 0
    assert calls == ["AAA", "BBB"]
    assert "budget_stop: provider_calls_used=2 max_provider_calls=2" in captured


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


def test_daily_budget_math():
    from tools.index_engine import run_daily

    remaining, max_calls = run_daily._compute_daily_budget(800, 25, 100)
    assert remaining == 700
    assert max_calls == 675

    remaining_low, max_calls_low = run_daily._compute_daily_budget(800, 25, 790)
    assert remaining_low == 10
    assert max_calls_low == 0


def test_run_daily_stops_on_daily_cap(monkeypatch, capsys):
    from tools.index_engine import run_daily

    class FakeProvider:
        def fetch_api_usage(self):
            return {"current_usage": 1, "plan_limit": 8}

        def has_eod_for_date(self, symbol, today):
            return True

    class FakeIngest:
        def __init__(self):
            self.called = False

        def main(self, args):
            self.called = True
            return 0

    class FakeRunLog:
        def __init__(self, calls):
            self.calls = calls

        def fetch_calls_used_today(self, provider):
            return self.calls

    fake_ingest = FakeIngest()
    fake_run_log = FakeRunLog(790)

    monkeypatch.setenv("SC_IDX_TWELVEDATA_DAILY_LIMIT", "800")
    monkeypatch.setenv("SC_IDX_TWELVEDATA_DAILY_BUFFER", "25")
    monkeypatch.setattr(run_daily, "_load_provider_module", lambda: FakeProvider())
    monkeypatch.setattr(run_daily, "_load_ingest_module", lambda: fake_ingest)
    monkeypatch.setattr(run_daily, "_load_run_log_module", lambda: fake_run_log)

    exit_code = run_daily.main()
    captured = capsys.readouterr().out

    assert exit_code == 0
    assert "daily_budget_stop" in captured
    assert fake_ingest.called is False


def test_run_daily_runs_when_budget_available(monkeypatch):
    from tools.index_engine import run_daily

    class FakeProvider:
        def fetch_api_usage(self):
            return {"current_usage": 1, "plan_limit": 8}

        def has_eod_for_date(self, symbol, today):
            return True

    class FakeIngest:
        def __init__(self):
            self.called_with = None

        def main(self, args):
            self.called_with = list(args)
            return 0

    class FakeRunLog:
        def __init__(self, calls):
            self.calls = calls

        def fetch_calls_used_today(self, provider):
            return self.calls

    fake_ingest = FakeIngest()
    fake_run_log = FakeRunLog(100)

    monkeypatch.setenv("SC_IDX_TWELVEDATA_DAILY_LIMIT", "800")
    monkeypatch.setenv("SC_IDX_TWELVEDATA_DAILY_BUFFER", "25")
    monkeypatch.setattr(run_daily, "_load_provider_module", lambda: FakeProvider())
    monkeypatch.setattr(run_daily, "_load_ingest_module", lambda: fake_ingest)
    monkeypatch.setattr(run_daily, "_load_run_log_module", lambda: fake_run_log)

    exit_code = run_daily.main()

    assert exit_code == 0
    assert fake_ingest.called_with is not None
    assert "--max-provider-calls" in fake_ingest.called_with
    max_calls_value = fake_ingest.called_with[fake_ingest.called_with.index("--max-provider-calls") + 1]
    # daily_limit=800, buffer=25, calls_used=100 => max=675
    assert max_calls_value == "675"
