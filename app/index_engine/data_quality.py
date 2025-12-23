"""Data quality helpers for TECH100 price completeness checks."""
from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass
from typing import Iterable, Mapping, Sequence


@dataclass(frozen=True)
class CoverageRecord:
    trade_date: _dt.date
    expected: int
    ok: int

    @property
    def ratio(self) -> float:
        if self.expected <= 0:
            return 0.0
        return self.ok / self.expected


def generate_weekdays(start: _dt.date, end: _dt.date) -> list[_dt.date]:
    """Return all weekdays (Mon-Fri) between start and end, inclusive."""
    if end < start:
        raise ValueError("end must be on or after start")
    days = (end - start).days + 1
    weekdays: list[_dt.date] = []
    for offset in range(days):
        current = start + _dt.timedelta(days=offset)
        if current.weekday() < 5:
            weekdays.append(current)
    return weekdays


def infer_holidays(
    coverage_by_date: Mapping[_dt.date, float],
    *,
    threshold: float,
) -> set[_dt.date]:
    """Infer market holidays based on low overall coverage."""
    holidays: set[_dt.date] = set()
    for trade_date, ratio in coverage_by_date.items():
        if ratio <= threshold:
            holidays.add(trade_date)
    return holidays


def find_bad_days(
    coverage_by_date: Mapping[_dt.date, float],
    *,
    holidays: Iterable[_dt.date],
    min_daily_coverage: float,
) -> list[_dt.date]:
    """Return weekdays that fail coverage and are not holidays."""
    holiday_set = set(holidays)
    bad_days: list[_dt.date] = []
    for trade_date, ratio in coverage_by_date.items():
        if trade_date in holiday_set:
            continue
        if ratio < min_daily_coverage:
            bad_days.append(trade_date)
    return bad_days


def evaluate_completeness(
    coverage_by_date: Mapping[_dt.date, float],
    *,
    holidays: Sequence[_dt.date],
    min_daily_coverage: float,
    max_bad_days: int,
) -> dict[str, object]:
    """Return completeness status and computed bad days."""
    bad_days = find_bad_days(
        coverage_by_date,
        holidays=holidays,
        min_daily_coverage=min_daily_coverage,
    )
    status = "PASS" if len(bad_days) <= max_bad_days else "FAIL"
    return {"status": status, "bad_days": bad_days}


__all__ = [
    "CoverageRecord",
    "evaluate_completeness",
    "find_bad_days",
    "generate_weekdays",
    "infer_holidays",
]
