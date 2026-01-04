from tools.index_engine import run_daily


def test_compute_daily_budget():
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

    class FakeIngest:
        def __init__(self):
            self.called = False

        def _run_backfill(self, args):
            self.called = True
            return 0, {}

    fake_ingest = FakeIngest()

    monkeypatch.setenv("SC_IDX_MARKET_DATA_DAILY_LIMIT", "800")
    monkeypatch.setenv("SC_IDX_MARKET_DATA_DAILY_BUFFER", "25")
    monkeypatch.setattr(run_daily, "_load_provider_module", lambda: FakeProvider())
    monkeypatch.setattr(run_daily, "_load_ingest_module", lambda: fake_ingest)
    monkeypatch.setattr(run_daily, "_load_trading_days_module", lambda: type("X", (), {"update_trading_days": staticmethod(lambda auto_extend=True: (0, 0, None, None, None))})())
    monkeypatch.setattr(
        run_daily,
        "_resolve_end_date",
        lambda *args, **kwargs: (run_daily.DEFAULT_START, [run_daily.DEFAULT_START], run_daily.DEFAULT_START),
    )
    monkeypatch.setattr(run_daily, "fetch_calls_used_today", lambda provider: 790)
    monkeypatch.setattr(run_daily, "start_run", lambda *args, **kwargs: None)
    monkeypatch.setattr(run_daily, "finish_run", lambda *args, **kwargs: None)
    monkeypatch.setattr(run_daily, "_oracle_preflight_or_exit", lambda **kwargs: "TEST")
    monkeypatch.setattr(run_daily._db_module, "fetch_max_canon_trade_date", lambda: None)
    monkeypatch.setattr(run_daily._db_module, "fetch_missing_real_for_trade_date", lambda *_args, **_kwargs: [])

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

    class FakeIngest:
        def __init__(self):
            self.called_with = None

        def _run_backfill(self, args):
            self.called_with = args
            return 0, {}

    fake_ingest = FakeIngest()

    monkeypatch.setenv("SC_IDX_MARKET_DATA_DAILY_LIMIT", "800")
    monkeypatch.setenv("SC_IDX_MARKET_DATA_DAILY_BUFFER", "25")
    monkeypatch.setattr(run_daily, "_load_provider_module", lambda: FakeProvider())
    monkeypatch.setattr(run_daily, "_load_ingest_module", lambda: fake_ingest)
    monkeypatch.setattr(run_daily, "_load_trading_days_module", lambda: type("X", (), {"update_trading_days": staticmethod(lambda auto_extend=True: (0, 0, None, None, None))})())
    monkeypatch.setattr(
        run_daily,
        "_resolve_end_date",
        lambda *args, **kwargs: (run_daily.DEFAULT_START, [run_daily.DEFAULT_START], run_daily.DEFAULT_START),
    )
    monkeypatch.setattr(run_daily, "fetch_calls_used_today", lambda provider: 100)
    monkeypatch.setattr(run_daily, "start_run", lambda *args, **kwargs: None)
    monkeypatch.setattr(run_daily, "finish_run", lambda *args, **kwargs: None)
    monkeypatch.setattr(run_daily, "_oracle_preflight_or_exit", lambda **kwargs: "TEST")
    monkeypatch.setattr(run_daily._db_module, "fetch_max_canon_trade_date", lambda: None)
    monkeypatch.setattr(run_daily._db_module, "fetch_missing_real_for_trade_date", lambda *_args, **_kwargs: [])

    exit_code = run_daily.main()

    assert exit_code == 0
    assert fake_ingest.called_with is not None
    assert fake_ingest.called_with.max_provider_calls == 675
