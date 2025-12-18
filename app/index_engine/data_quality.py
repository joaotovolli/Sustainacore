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


def select_trading_days(
    trading_days: Sequence[_dt.date],
    start: _dt.date,
    end: _dt.date,
) -> list[_dt.date]:
    """Filter trading days to the requested date range (inclusive)."""
    if end < start:
        raise ValueError("end must be on or after start")
    return [day for day in trading_days if start <= day <= end]


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


def find_previous_available(
    trading_days: Sequence[_dt.date],
    target_date: _dt.date,
    available_dates: set[_dt.date],
) -> _dt.date | None:
    """Return the most recent trading day before target_date with available data."""
    for day in reversed(trading_days):
        if day >= target_date:
            continue
        if day in available_dates:
            return day
    return None


def format_imputation_alert(
    *,
    date_range: tuple[_dt.date, _dt.date],
    total_imputed: int,
    total_missing_without_prior: int,
    per_date_counts: Sequence[tuple[_dt.date, int]],
    top_tickers: Sequence[tuple[str, int]],
) -> str:
    """Build an imputation alert body."""
    start, end = date_range
    lines = [
        f"date_range={start.isoformat()}..{end.isoformat()}",
        f"total_imputed={total_imputed}",
        f"missing_without_prior={total_missing_without_prior}",
        "",
        "imputations_by_date:",
    ]
    for trade_date, count in per_date_counts:
        lines.append(f"- {trade_date.isoformat()} count={count}")
    lines.append("")
    lines.append("top_tickers:")
    for ticker, count in top_tickers:
        lines.append(f"- {ticker} count={count}")
    return "\n".join(lines)


__all__ = [
    "CoverageRecord",
    "evaluate_completeness",
    "find_bad_days",
    "find_previous_available",
    "format_imputation_alert",
    "generate_weekdays",
    "infer_holidays",
    "select_trading_days",
]
