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
