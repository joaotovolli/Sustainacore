import datetime as dt
from types import SimpleNamespace

import pytest

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


def _patch_low_resource_inputs(monkeypatch):
    monkeypatch.setattr(builder, "load_env_files", lambda: None)
    monkeypatch.setattr(builder, "load_default_env", lambda: None)
    monkeypatch.setattr(
        builder.db,
        "fetch_trade_date_bounds",
        lambda: (dt.date(2026, 1, 2), dt.date(2026, 1, 3)),
    )
    monkeypatch.setattr(builder.db, "fetch_official_daily_rows", lambda: ["daily"])
    monkeypatch.setattr(builder.db, "fetch_official_position_rows", lambda: ["position"])
    monkeypatch.setattr(builder.db, "fetch_metadata_rows", lambda: ["meta"])
    monkeypatch.setattr(builder.db, "fetch_price_rows", lambda **_kwargs: ["price"])


def test_constraint_mismatch_fails_before_low_resource_reset(monkeypatch):
    _patch_low_resource_inputs(monkeypatch)
    events = []
    monkeypatch.setattr(
        builder.db,
        "validate_static_constraints",
        lambda _rows: (_ for _ in ()).throw(RuntimeError("model_portfolio_constraints_mismatch")),
    )
    monkeypatch.setattr(builder.db, "reset_output_window", lambda **_kwargs: events.append("reset"))

    with pytest.raises(RuntimeError, match="model_portfolio_constraints_mismatch"):
        builder.main(
            [
                "--skip-preflight",
                "--low-resource",
                "--dry-run",
                "--start",
                "2026-01-02",
                "--end",
                "2026-01-03",
            ]
        )
    assert events == []


def test_status_file_failure_is_secondary_to_portfolio_persistence(monkeypatch, capsys):
    _patch_low_resource_inputs(monkeypatch)
    events = []
    monkeypatch.setattr(builder.db, "validate_static_constraints", lambda _rows: None)

    class Connection:
        call_timeout = None

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    monkeypatch.setattr(builder.db, "get_connection", Connection)
    monkeypatch.setattr(
        "tools.db_migrations.repair_sc_idx_corporate_actions.validate_backup_set",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(builder.db, "reset_output_window", lambda **_kwargs: events.append("reset"))
    monkeypatch.setattr(
        builder.db,
        "persist_model_output_batch",
        lambda **_kwargs: (1, 1),
    )
    monkeypatch.setattr(
        builder.db,
        "persist_optimizer_with_static_constraints",
        lambda **_kwargs: 1,
    )

    def build(**kwargs):
        kwargs["model_output_callback"](
            SimpleNamespace(code="TECH100"),
            [{"trade_date": dt.date(2026, 1, 3), "model_code": "TECH100"}],
            [{"trade_date": dt.date(2026, 1, 3), "model_code": "TECH100"}],
        )
        return {
            "analytics": [],
            "positions": [],
            "optimizer_inputs": [{"trade_date": dt.date(2026, 1, 3)}],
            "constraints": [],
        }

    monkeypatch.setattr(builder, "build_portfolio_outputs", build)
    monkeypatch.setattr(
        builder,
        "write_reconstruction_status",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("status disk unavailable")),
    )

    assert builder.main(
        [
            "--skip-preflight",
            "--low-resource",
            "--backup-tag",
            "20260712120000ABCD",
            "--run-id",
            "ca-1",
            "--status-file",
            "/tmp/test-reconstruction-status.json",
            "--start",
            "2026-01-02",
            "--end",
            "2026-01-03",
        ]
    ) == 0
    assert events == ["reset"]
    assert "portfolio_status_update_error=OSError:status disk unavailable" in capsys.readouterr().out
