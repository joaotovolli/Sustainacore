import datetime as _dt
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from tools.index_engine import ingest_prices
from tools.index_engine.ingest_prices import compute_canonical_rows, _build_raw_rows_from_provider


def test_compute_canonical_uses_provider_close_when_adj_missing():
    trade_date = _dt.date(2025, 1, 2)
    raw_rows = [
        {
            "ticker": "ABC",
            "trade_date": trade_date,
            "provider": "MARKET_DATA",
            "close_px": 12.34,
            "adj_close_px": None,
            "status": "OK",
        }
    ]

    canon_rows = compute_canonical_rows(raw_rows)

    assert len(canon_rows) == 1
    row = canon_rows[0]
    assert row["canon_adj_close_px"] == 12.34
    assert row["canon_close_px"] == 12.34
    assert row["chosen_provider"] == "MARKET_DATA"
    assert row["quality"] == "LOW"
    assert row["providers_ok"] == 1


def test_build_raw_rows_skips_null_close():
    rows = [
        {"ticker": "AAA", "trade_date": "2025-01-02", "close": None},
        {"ticker": "BBB", "trade_date": "2025-01-02", "close": 12.34},
    ]
    raw_rows = _build_raw_rows_from_provider(rows)
    assert len(raw_rows) == 1
    assert raw_rows[0]["ticker"] == "BBB"


def test_backfill_extends_short_cached_calendar_to_ready_end(monkeypatch):
    calls = []
    raw_written = []
    canon_written = []

    monkeypatch.setattr(
        ingest_prices,
        "fetch_trading_days",
        lambda start, end: [_dt.date(2026, 5, 28)],
    )
    monkeypatch.setattr(ingest_prices, "_provider_has_calendar_bar", lambda day: True)
    monkeypatch.setattr(ingest_prices, "fetch_impacted_tickers_for_trade_date", lambda day: ["AAPL"])
    monkeypatch.setattr(ingest_prices, "fetch_max_ok_trade_date", lambda ticker, provider: None)
    monkeypatch.setattr(ingest_prices, "_fetch_existing_ok", lambda **kwargs: set())

    def fake_fetch_provider_rows(ticker, start_date, end_date):
        calls.append((ticker, start_date, end_date))
        return [
            {"ticker": ticker, "trade_date": "2026-05-28", "close": 101.0, "adj_close": 101.0},
            {"ticker": ticker, "trade_date": "2026-05-29", "close": 102.0, "adj_close": 102.0},
        ]

    def fake_upsert_raw(rows):
        raw_written.extend(rows)
        return len(rows)

    def fake_upsert_canon(rows):
        canon_written.extend(rows)
        return len(rows)

    monkeypatch.setattr(ingest_prices, "_fetch_provider_rows", fake_fetch_provider_rows)
    monkeypatch.setattr(ingest_prices, "upsert_prices_raw", fake_upsert_raw)
    monkeypatch.setattr(ingest_prices, "upsert_prices_canon", fake_upsert_canon)

    args = type(
        "Args",
        (),
        {
            "start": "2026-05-28",
            "end": "2026-05-29",
            "tickers": None,
            "debug": False,
            "max_provider_calls": None,
            "provider_calls_per_minute": 100000,
        },
    )()

    code, summary = ingest_prices._run_backfill(args)

    assert code == 0
    assert calls == [("AAPL", _dt.date(2026, 5, 28), _dt.date(2026, 5, 29))]
    assert {row["trade_date"] for row in raw_written} == {
        _dt.date(2026, 5, 28),
        _dt.date(2026, 5, 29),
    }
    assert {row["trade_date"] for row in canon_written} == {
        _dt.date(2026, 5, 28),
        _dt.date(2026, 5, 29),
    }
    assert summary["max_ok_trade_date"] == _dt.date(2026, 5, 29)


def test_backfill_uses_bounded_weekday_when_cached_calendar_missing_target(monkeypatch):
    calls = []
    raw_written = []
    canon_written = []

    monkeypatch.setattr(ingest_prices, "fetch_trading_days", lambda start, end: [])
    monkeypatch.setattr(ingest_prices, "_provider_has_calendar_bar", lambda day: True)
    monkeypatch.setattr(ingest_prices, "fetch_impacted_tickers_for_trade_date", lambda day: ["MSFT"])
    monkeypatch.setattr(ingest_prices, "fetch_max_ok_trade_date", lambda ticker, provider: None)
    monkeypatch.setattr(ingest_prices, "_fetch_existing_ok", lambda **kwargs: set())

    def fake_fetch_provider_rows(ticker, start_date, end_date):
        calls.append((ticker, start_date, end_date))
        return [{"ticker": ticker, "trade_date": "2026-05-29", "close": 203.0, "adj_close": 203.0}]

    monkeypatch.setattr(ingest_prices, "_fetch_provider_rows", fake_fetch_provider_rows)
    monkeypatch.setattr(ingest_prices, "upsert_prices_raw", lambda rows: raw_written.extend(rows) or len(rows))
    monkeypatch.setattr(ingest_prices, "upsert_prices_canon", lambda rows: canon_written.extend(rows) or len(rows))

    args = type(
        "Args",
        (),
        {
            "start": "2026-05-29",
            "end": "2026-05-29",
            "tickers": None,
            "debug": False,
            "max_provider_calls": None,
            "provider_calls_per_minute": 100000,
        },
    )()

    code, summary = ingest_prices._run_backfill(args)

    assert code == 0
    assert calls == [("MSFT", _dt.date(2026, 5, 29), _dt.date(2026, 5, 29))]
    assert [row["trade_date"] for row in raw_written] == [_dt.date(2026, 5, 29)]
    assert [row["trade_date"] for row in canon_written] == [_dt.date(2026, 5, 29)]
    assert summary["max_ok_trade_date"] == _dt.date(2026, 5, 29)


def test_backfill_skips_synthetic_holiday_before_ready_day(monkeypatch):
    calls = []
    raw_written = []
    canon_written = []

    monkeypatch.setattr(ingest_prices, "fetch_trading_days", lambda start, end: [])
    monkeypatch.setattr(
        ingest_prices,
        "_provider_has_calendar_bar",
        lambda day: day == _dt.date(2026, 6, 22),
    )
    monkeypatch.setattr(ingest_prices, "fetch_impacted_tickers_for_trade_date", lambda day: ["NVDA"])
    monkeypatch.setattr(ingest_prices, "fetch_max_ok_trade_date", lambda ticker, provider: None)
    monkeypatch.setattr(ingest_prices, "_fetch_existing_ok", lambda **kwargs: set())

    def fake_fetch_provider_rows(ticker, start_date, end_date):
        calls.append((ticker, start_date, end_date))
        return [{"ticker": ticker, "trade_date": "2026-06-22", "close": 300.0, "adj_close": 300.0}]

    monkeypatch.setattr(ingest_prices, "_fetch_provider_rows", fake_fetch_provider_rows)
    monkeypatch.setattr(ingest_prices, "upsert_prices_raw", lambda rows: raw_written.extend(rows) or len(rows))
    monkeypatch.setattr(ingest_prices, "upsert_prices_canon", lambda rows: canon_written.extend(rows) or len(rows))

    args = type(
        "Args",
        (),
        {
            "start": "2026-06-19",
            "end": "2026-06-22",
            "tickers": None,
            "debug": False,
            "max_provider_calls": None,
            "provider_calls_per_minute": 100000,
        },
    )()

    code, summary = ingest_prices._run_backfill(args)

    assert code == 0
    assert calls == [("NVDA", _dt.date(2026, 6, 22), _dt.date(2026, 6, 22))]
    assert [row["trade_date"] for row in raw_written] == [_dt.date(2026, 6, 22)]
    assert [row["trade_date"] for row in canon_written] == [_dt.date(2026, 6, 22)]
    assert summary["max_ok_trade_date"] == _dt.date(2026, 6, 22)


def test_backfill_stops_on_provider_rate_limit_without_more_tickers(monkeypatch):
    calls = []

    monkeypatch.setattr(ingest_prices, "fetch_trading_days", lambda start, end: [_dt.date(2026, 6, 22)])
    monkeypatch.setattr(ingest_prices, "fetch_impacted_tickers_for_trade_date", lambda day: ["AAPL", "MSFT"])
    monkeypatch.setattr(ingest_prices, "fetch_max_ok_trade_date", lambda ticker, provider: None)
    monkeypatch.setattr(ingest_prices, "_fetch_existing_ok", lambda **kwargs: set())
    monkeypatch.setattr(ingest_prices, "upsert_prices_raw", lambda rows: (_ for _ in ()).throw(AssertionError("no missing rows on 429")))
    monkeypatch.setattr(ingest_prices, "upsert_prices_canon", lambda rows: (_ for _ in ()).throw(AssertionError("no canon rows on 429")))

    def fake_fetch_provider_rows(ticker, start_date, end_date):
        calls.append((ticker, start_date, end_date))
        raise RuntimeError("market_data_http_error:429")

    monkeypatch.setattr(ingest_prices, "_fetch_provider_rows", fake_fetch_provider_rows)

    args = type(
        "Args",
        (),
        {
            "start": "2026-06-22",
            "end": "2026-06-22",
            "tickers": None,
            "debug": False,
            "max_provider_calls": 10,
            "provider_minute_limit": 8,
            "provider_calls_per_minute": None,
        },
    )()

    code, summary = ingest_prices._run_backfill(args)

    assert code == 2
    assert summary["provider_rate_limited"] is True
    assert summary["provider_calls_used"] == 1
    assert summary["effective_calls_per_minute"] == 4
    assert calls == [("AAPL", _dt.date(2026, 6, 22), _dt.date(2026, 6, 22))]


def test_backfill_paces_calls_under_minute_limit(monkeypatch):
    calls = []
    sleeps = []
    clock = {"now": 0.0}

    monkeypatch.setattr(ingest_prices, "fetch_trading_days", lambda start, end: [_dt.date(2026, 6, 22)])
    monkeypatch.setattr(ingest_prices, "fetch_impacted_tickers_for_trade_date", lambda day: ["AAPL", "MSFT"])
    monkeypatch.setattr(ingest_prices, "_fetch_existing_ok", lambda **kwargs: set())
    monkeypatch.setattr(ingest_prices.time, "monotonic", lambda: clock["now"])

    def fake_sleep(seconds):
        sleeps.append(seconds)
        clock["now"] += seconds

    monkeypatch.setattr(ingest_prices.time, "sleep", fake_sleep)

    def fake_fetch_provider_rows(ticker, start_date, end_date):
        calls.append((ticker, start_date, end_date))
        return [{"ticker": ticker, "trade_date": "2026-06-22", "close": 100.0, "adj_close": 100.0}]

    monkeypatch.setattr(ingest_prices, "_fetch_provider_rows", fake_fetch_provider_rows)
    monkeypatch.setattr(ingest_prices, "upsert_prices_raw", lambda rows: len(rows))
    monkeypatch.setattr(ingest_prices, "upsert_prices_canon", lambda rows: len(rows))

    args = type(
        "Args",
        (),
        {
            "start": "2026-06-22",
            "end": "2026-06-22",
            "tickers": None,
            "debug": False,
            "max_provider_calls": None,
            "provider_minute_limit": 8,
            "provider_calls_per_minute": None,
        },
    )()

    code, summary = ingest_prices._run_backfill(args)

    assert code == 0
    assert summary["effective_calls_per_minute"] == 4
    assert len(calls) == 2
    assert sleeps == [15.0]


def test_backfill_skips_existing_canonical_rows(monkeypatch):
    calls = []

    monkeypatch.setattr(ingest_prices, "fetch_trading_days", lambda start, end: [_dt.date(2026, 6, 22)])
    monkeypatch.setattr(ingest_prices, "fetch_impacted_tickers_for_trade_date", lambda day: ["AAPL", "MSFT"])
    monkeypatch.setattr(
        ingest_prices,
        "_fetch_existing_ok",
        lambda **kwargs: {("AAPL", _dt.date(2026, 6, 22))},
    )

    def fake_fetch_provider_rows(ticker, start_date, end_date):
        calls.append((ticker, start_date, end_date))
        return [{"ticker": ticker, "trade_date": "2026-06-22", "close": 250.0, "adj_close": 250.0}]

    monkeypatch.setattr(ingest_prices, "_fetch_provider_rows", fake_fetch_provider_rows)
    monkeypatch.setattr(ingest_prices, "upsert_prices_raw", lambda rows: len(rows))
    monkeypatch.setattr(ingest_prices, "upsert_prices_canon", lambda rows: len(rows))

    args = type(
        "Args",
        (),
        {
            "start": "2026-06-22",
            "end": "2026-06-22",
            "tickers": None,
            "debug": False,
            "max_provider_calls": None,
            "provider_minute_limit": 8,
            "provider_calls_per_minute": None,
        },
    )()

    code, summary = ingest_prices._run_backfill(args)

    assert code == 0
    assert calls == [("MSFT", _dt.date(2026, 6, 22), _dt.date(2026, 6, 22))]
    assert summary["provider_calls_used"] == 1


def test_backfill_missing_uses_pacing_and_reports_rate(monkeypatch):
    calls = []
    sleeps = []
    clock = {"now": 0.0}

    monkeypatch.setattr(ingest_prices, "fetch_trading_days", lambda start, end: [_dt.date(2026, 6, 22)])
    monkeypatch.setattr(ingest_prices, "_fetch_existing_ok", lambda **kwargs: set())
    monkeypatch.setattr(ingest_prices.time, "monotonic", lambda: clock["now"])

    def fake_sleep(seconds):
        sleeps.append(seconds)
        clock["now"] += seconds

    monkeypatch.setattr(ingest_prices.time, "sleep", fake_sleep)

    def fake_fetch_provider_rows(ticker, start_date, end_date):
        calls.append((ticker, start_date, end_date))
        return [{"ticker": ticker, "trade_date": "2026-06-22", "close": 300.0, "adj_close": 300.0}]

    monkeypatch.setattr(ingest_prices, "_fetch_provider_rows", fake_fetch_provider_rows)
    monkeypatch.setattr(ingest_prices, "upsert_prices_raw", lambda rows: len(rows))
    monkeypatch.setattr(ingest_prices, "upsert_prices_canon", lambda rows: len(rows))

    args = type(
        "Args",
        (),
        {
            "start": "2026-06-22",
            "end": "2026-06-22",
            "tickers": "AAPL,MSFT",
            "debug": False,
            "max_provider_calls": None,
            "provider_minute_limit": 8,
            "provider_calls_per_minute": None,
        },
    )()

    code, summary = ingest_prices._run_backfill_missing(args)

    assert code == 0
    assert summary["effective_calls_per_minute"] == 4
    assert len(calls) == 2
    assert sleeps == [15.0]
