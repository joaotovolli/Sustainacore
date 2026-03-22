import datetime as dt

from tools.index_engine import build_portfolio_analytics as builder


def test_main_filters_rows_and_persists_requested_window(monkeypatch, capsys):
    persisted = {}
    apply_calls = []

    monkeypatch.setattr(builder, "load_env_files", lambda: None)
    monkeypatch.setattr(builder, "load_default_env", lambda: None)
    monkeypatch.setattr(builder.db, "apply_ddl", lambda: apply_calls.append("ddl"))
    monkeypatch.setattr(
        builder.db,
        "fetch_trade_date_bounds",
        lambda: (dt.date(2026, 1, 2), dt.date(2026, 1, 3)),
    )
    monkeypatch.setattr(builder.db, "fetch_official_daily_rows", lambda: ["daily"])
    monkeypatch.setattr(builder.db, "fetch_official_position_rows", lambda: ["position"])
    monkeypatch.setattr(builder.db, "fetch_metadata_rows", lambda: ["meta"])
    monkeypatch.setattr(builder.db, "fetch_price_rows", lambda start_date, end_date: ["price"])
    monkeypatch.setattr(
        builder,
        "build_portfolio_outputs",
        lambda **kwargs: {
            "analytics": [
                {"trade_date": dt.date(2026, 1, 2), "model_code": "TECH100"},
                {"trade_date": dt.date(2026, 1, 3), "model_code": "TECH100_EQ"},
            ],
            "positions": [
                {"trade_date": dt.date(2026, 1, 2)},
                {"trade_date": dt.date(2026, 1, 3)},
            ],
            "optimizer_inputs": [
                {"trade_date": dt.date(2026, 1, 2)},
                {"trade_date": dt.date(2026, 1, 3)},
            ],
            "constraints": [{"model_code": "TECH100", "constraint_key": "LONG_ONLY"}],
        },
    )

    def _persist(**kwargs):
        persisted.update(kwargs)

    monkeypatch.setattr(builder.db, "persist_outputs", _persist)

    exit_code = builder.main(
        ["--apply-ddl", "--skip-preflight", "--start", "2026-01-03", "--end", "2026-01-03"]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert apply_calls == ["ddl"]
    assert "analytics_rows=1" in captured.out
    assert len(persisted["analytics_rows"]) == 1
    assert len(persisted["position_rows"]) == 1
    assert len(persisted["optimizer_rows"]) == 1
    assert persisted["start_date"] == dt.date(2026, 1, 3)
    assert persisted["end_date"] == dt.date(2026, 1, 3)
