"""Trigger detectors for scheduled reports."""
from __future__ import annotations

import datetime as dt
from typing import Optional, Tuple

from .oracle import fetch_latest_port_dates, fetch_stats_latest, get_report_value


def detect_rebalance(conn) -> Tuple[bool, Optional[dt.date], Optional[dt.date]]:
    dates = fetch_latest_port_dates(conn, limit=2)
    if not dates:
        return False, None, None
    latest = dates[0]
    previous = dates[1] if len(dates) > 1 else None
    last_reported = get_report_value(conn, "rebalance_port_date")
    if last_reported and latest.strftime("%Y-%m-%d") <= str(last_reported):
        return False, latest, previous
    return True, latest, previous


def detect_weekly(conn, now: dt.datetime) -> bool:
    if now.weekday() != 0:
        return False
    last_weekly = get_report_value(conn, "weekly_last_date")
    today_str = now.strftime("%Y-%m-%d")
    if last_weekly == today_str:
        return False
    return True


def detect_period_close(conn) -> Tuple[bool, Optional[str]]:
    stats = fetch_stats_latest(conn)
    latest_trade = stats.get("trade_date") if stats else None
    if not latest_trade:
        return False, None
    # Use previous month/quarter/year label based on latest trade date.
    year = latest_trade.year
    month = latest_trade.month
    prev_month = month - 1 or 12
    prev_year = year - 1 if month == 1 else year
    month_label = f"{prev_year:04d}-{prev_month:02d}"
    quarter = (prev_month - 1) // 3 + 1
    quarter_label = f"{prev_year:04d}-Q{quarter}"
    year_label = f"{prev_year:04d}"

    last_month = get_report_value(conn, "period_close_month")
    last_quarter = get_report_value(conn, "period_close_quarter")
    last_year = get_report_value(conn, "period_close_year")

    if last_year != year_label and month == 1:
        return True, f"Year close {year_label}"
    if last_quarter != quarter_label and month in (1, 4, 7, 10):
        return True, f"Quarter close {quarter_label}"
    if last_month != month_label:
        return True, f"Month close {month_label}"
    return False, None


def detect_anomaly(conn) -> Tuple[bool, Optional[str]]:
    stats = fetch_stats_latest(conn)
    if not stats:
        return False, None
    trade_date = stats.get("trade_date")
    if not trade_date:
        return False, None
    vol = stats.get("vol_20d") or 0
    ret_1d = stats.get("ret_1d") or 0
    drawdown = stats.get("max_drawdown_252d") or 0
    z_score = (float(ret_1d) / float(vol)) if vol else 0
    trigger = abs(z_score) >= 2 or float(drawdown) <= -0.1
    last_anomaly = get_report_value(conn, "anomaly_last_date")
    if last_anomaly == trade_date.strftime("%Y-%m-%d"):
        return False, None
    return (trigger, trade_date.strftime("%Y-%m-%d") if trigger else None)
