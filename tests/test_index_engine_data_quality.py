import datetime as _dt
import importlib.util
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "app" / "index_engine" / "data_quality.py"
spec = importlib.util.spec_from_file_location("data_quality_test", MODULE_PATH)
if spec is None or spec.loader is None:
    raise RuntimeError("unable to load data_quality module for tests")
data_quality = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = data_quality
spec.loader.exec_module(data_quality)  # type: ignore[arg-type]

evaluate_completeness = data_quality.evaluate_completeness
find_bad_days = data_quality.find_bad_days
find_previous_available = data_quality.find_previous_available
format_imputation_alert = data_quality.format_imputation_alert
generate_weekdays = data_quality.generate_weekdays
infer_holidays = data_quality.infer_holidays
select_trading_days = data_quality.select_trading_days


def test_generate_weekdays_ignores_weekends():
    start = _dt.date(2025, 1, 3)  # Friday
    end = _dt.date(2025, 1, 6)  # Monday
    weekdays = generate_weekdays(start, end)
    assert weekdays == [_dt.date(2025, 1, 3), _dt.date(2025, 1, 6)]


def test_infer_holidays_from_low_coverage():
    coverage = {
        _dt.date(2025, 1, 2): 0.05,
        _dt.date(2025, 1, 3): 0.60,
    }
    holidays = infer_holidays(coverage, threshold=0.10)
    assert _dt.date(2025, 1, 2) in holidays
    assert _dt.date(2025, 1, 3) not in holidays


def test_find_bad_days_excludes_holidays():
    coverage = {
        _dt.date(2025, 1, 2): 0.05,
        _dt.date(2025, 1, 3): 0.60,
        _dt.date(2025, 1, 6): 0.95,
    }
    holidays = {_dt.date(2025, 1, 2)}
    bad_days = find_bad_days(coverage, holidays=holidays, min_daily_coverage=0.90)
    assert bad_days == [_dt.date(2025, 1, 3)]


def test_evaluate_completeness_pass_fail():
    coverage = {
        _dt.date(2025, 1, 2): 0.95,
        _dt.date(2025, 1, 3): 0.85,
    }
    result = evaluate_completeness(
        coverage,
        holidays=[],
        min_daily_coverage=0.90,
        max_bad_days=0,
    )
    assert result["status"] == "FAIL"
    assert result["bad_days"] == [_dt.date(2025, 1, 3)]


def test_select_trading_days_range():
    trading_days = [
        _dt.date(2025, 1, 2),
        _dt.date(2025, 1, 3),
        _dt.date(2025, 1, 6),
    ]
    result = select_trading_days(trading_days, _dt.date(2025, 1, 3), _dt.date(2025, 1, 6))
    assert result == [_dt.date(2025, 1, 3), _dt.date(2025, 1, 6)]


def test_find_previous_available_trading_day():
    trading_days = [
        _dt.date(2025, 1, 2),
        _dt.date(2025, 1, 3),
        _dt.date(2025, 1, 6),
    ]
    available = {_dt.date(2025, 1, 2), _dt.date(2025, 1, 6)}
    assert find_previous_available(trading_days, _dt.date(2025, 1, 6), available) == _dt.date(2025, 1, 2)


def test_format_imputation_alert_body():
    body = format_imputation_alert(
        date_range=(_dt.date(2025, 1, 2), _dt.date(2025, 1, 6)),
        total_imputed=3,
        total_missing_without_prior=1,
        per_date_counts=[(_dt.date(2025, 1, 3), 2)],
        top_tickers=[("AAPL", 2)],
    )
    assert "date_range=2025-01-02..2025-01-06" in body
    assert "total_imputed=3" in body
