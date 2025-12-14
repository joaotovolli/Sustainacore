import datetime as dt

from tools.index_engine import verify_pipeline


def test_verify_pipeline_latest_bar_parsing(monkeypatch, capsys):
    monkeypatch.setattr(verify_pipeline, "_load_env_files", lambda: None)

    usage_calls = {"count": 0}

    def _fake_usage():
        usage_calls["count"] += 1
        return {"current_usage": usage_calls["count"], "plan_limit": 8, "plan_category": "basic", "timestamp": "now"}

    monkeypatch.setattr(verify_pipeline, "fetch_api_usage", _fake_usage)
    monkeypatch.setattr(
        verify_pipeline,
        "fetch_latest_bar",
        lambda symbol: [{"datetime": "2025-12-10 00:00:00", "close": "1.0"}],
    )

    oracle_payload = {
        "oracle_user": "TEST",
        "max_ingested_at": dt.datetime.now(dt.timezone.utc),
        "max_ok_trade_date": dt.date.today(),
        "daily_counts": [],
        "raw_count": 1,
        "canon_count": 1,
    }
    monkeypatch.setattr(verify_pipeline, "_query_oracle", lambda: oracle_payload)

    exit_code = verify_pipeline.main()
    output = capsys.readouterr().out

    assert exit_code == 0
    assert "provider_rows=1" in output
    assert "latest_datetime=2025-12-10" in output
    assert "OVERALL: PASS" in output
