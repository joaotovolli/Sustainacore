import datetime as _dt
import importlib.util
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "tools" / "index_engine" / "run_daily.py"
spec = importlib.util.spec_from_file_location("run_daily_test", MODULE_PATH)
if spec is None or spec.loader is None:
    raise RuntimeError("unable to load run_daily module for tests")
run_daily = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = run_daily
spec.loader.exec_module(run_daily)  # type: ignore[arg-type]


def test_select_effective_end_date_prefers_latest_trading_day():
    provider_latest = _dt.date(2025, 12, 17)
    trading_days = [_dt.date(2025, 12, 16), _dt.date(2025, 12, 17)]
    assert run_daily.select_effective_end_date(provider_latest, trading_days) == _dt.date(2025, 12, 17)
