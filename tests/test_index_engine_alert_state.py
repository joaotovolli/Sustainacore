import datetime as _dt
import importlib.util
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "app" / "index_engine" / "alert_state.py"
spec = importlib.util.spec_from_file_location("alert_state_test", MODULE_PATH)
if spec is None or spec.loader is None:
    raise RuntimeError("unable to load alert_state module for tests")
alert_state = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = alert_state
spec.loader.exec_module(alert_state)  # type: ignore[arg-type]


def test_should_send_today_when_no_last_sent():
    today = _dt.date(2025, 1, 2)
    assert alert_state.should_send_today(None, today) is True


def test_should_send_today_same_day_false():
    today = _dt.date(2025, 1, 2)
    assert alert_state.should_send_today(today, today) is False


def test_evaluate_alert_gate_first_send(monkeypatch):
    monkeypatch.setattr(alert_state, "get_alert_state", lambda name: {"alert_name": name, "exists": False})

    result = alert_state.evaluate_alert_gate("sc_idx_pipeline_failed")

    assert result["should_send"] is True
    assert result["reason"] == "first_send"


def test_evaluate_alert_gate_suppresses_same_day(monkeypatch):
    monkeypatch.setattr(
        alert_state,
        "get_alert_state",
        lambda name: {
            "alert_name": name,
            "exists": True,
            "last_sent_utc_date": "2025-01-02",
            "last_sent_at": "2025-01-02T00:10:00+00:00",
            "last_status": "ERROR",
            "last_detail_hash": "abc",
        },
    )

    result = alert_state.evaluate_alert_gate(
        "sc_idx_pipeline_failed",
        now=_dt.datetime(2025, 1, 2, 12, 0, tzinfo=_dt.timezone.utc),
    )

    assert result["should_send"] is False
    assert result["reason"] == "already_sent_today"
