import datetime as _dt
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
APP = ROOT / "app"
for path in (ROOT, APP):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from index_engine.data_quality import (
    evaluate_completeness,
    find_bad_days,
    generate_weekdays,
    infer_holidays,
)


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
