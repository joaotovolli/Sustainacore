import datetime as dt


def test_run_daily_degrades_on_trading_days_403(monkeypatch, capsys):
    from tools.index_engine import run_daily

    class FakeProvider:
        def fetch_api_usage(self):
            return {"current_usage": 0, "plan_limit": 1000}

    fake_trading_days_module = type(
        "FakeTradingDays",
        (),
        {"update_trading_days_with_retry": staticmethod(lambda **_kwargs: (False, "market_data_http_error:403"))},
    )

    fake_ingest = type(
        "FakeIngest",
        (),
        {"_run_backfill": staticmethod(lambda _args: (0, {}))},
    )

    def fake_resolve_end_date(_provider, _symbol, _today):
        trading_days = [run_daily.DEFAULT_START]
        provider_latest = run_daily.DEFAULT_START + dt.timedelta(days=1)
        return provider_latest, trading_days, run_daily.DEFAULT_START

    monkeypatch.setattr(run_daily, "_load_provider_module", lambda: FakeProvider())
    monkeypatch.setattr(run_daily, "_load_ingest_module", lambda: fake_ingest)
    monkeypatch.setattr(run_daily, "_load_trading_days_module", lambda: fake_trading_days_module())
    monkeypatch.setattr(run_daily, "_resolve_end_date", fake_resolve_end_date)
    monkeypatch.setattr(run_daily, "fetch_calls_used_today", lambda _provider: 0)
    monkeypatch.setattr(run_daily, "start_run", lambda *args, **kwargs: None)
    monkeypatch.setattr(run_daily, "finish_run", lambda *args, **kwargs: None)
    monkeypatch.setattr(run_daily, "_oracle_preflight_or_exit", lambda **kwargs: "TEST")
    monkeypatch.setattr(run_daily._db_module, "fetch_max_canon_trade_date", lambda: None)
    monkeypatch.setattr(run_daily._db_module, "fetch_missing_real_for_trade_date", lambda *_args, **_kwargs: [])

    exit_code = run_daily.main([])
    stderr = capsys.readouterr().err
    assert exit_code == 0
    assert "cached calendar" in stderr
