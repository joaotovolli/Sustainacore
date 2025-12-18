"""Oracle-backed alert suppression for SC_IDX."""
from __future__ import annotations

import datetime as _dt
import hashlib
from typing import Optional

from db_helper import get_connection


def _hash_detail(detail: Optional[str]) -> Optional[str]:
    if detail is None:
        return None
    digest = hashlib.sha256(detail.encode("utf-8")).hexdigest()
    return digest


def _utc_today(now: Optional[_dt.datetime] = None) -> _dt.date:
    current = now or _dt.datetime.now(_dt.timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=_dt.timezone.utc)
    return current.date()


def should_send_today(last_sent_date: Optional[_dt.date], today: _dt.date) -> bool:
    """Return True if last_sent_date is not today."""
    if last_sent_date is None:
        return True
    return last_sent_date != today


def should_send_alert(
    alert_name: str,
    *,
    detail: Optional[str] = None,
    now: Optional[_dt.datetime] = None,
) -> bool:
    """Return True if alert should be sent (once per UTC day)."""

    today = _utc_today(now)
    detail_hash = _hash_detail(detail)

    sql = (
        "SELECT last_sent_utc_date, last_detail_hash "
        "FROM SC_IDX_ALERT_STATE "
        "WHERE alert_name = :alert_name"
    )
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, {"alert_name": alert_name})
        row = cur.fetchone()

    if not row:
        return True

    last_sent_date, last_hash = row
    if isinstance(last_sent_date, _dt.datetime):
        last_sent_date = last_sent_date.date()

    if not should_send_today(last_sent_date, today):
        return False

    return True


def mark_alert_sent(
    alert_name: str,
    *,
    status: str,
    detail: Optional[str] = None,
    now: Optional[_dt.datetime] = None,
) -> None:
    """Record that an alert was sent."""

    current = now or _dt.datetime.now(_dt.timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=_dt.timezone.utc)
    detail_hash = _hash_detail(detail)

    sql = (
        "MERGE INTO SC_IDX_ALERT_STATE dst "
        "USING (SELECT :alert_name AS alert_name FROM dual) src "
        "ON (dst.alert_name = src.alert_name) "
        "WHEN MATCHED THEN UPDATE SET "
        "  last_sent_utc_date = :last_sent_utc_date, "
        "  last_sent_at = :last_sent_at, "
        "  last_status = :last_status, "
        "  last_detail_hash = :last_detail_hash "
        "WHEN NOT MATCHED THEN INSERT "
        "  (alert_name, last_sent_utc_date, last_sent_at, last_status, last_detail_hash) "
        "VALUES (:alert_name, :last_sent_utc_date, :last_sent_at, :last_status, :last_detail_hash)"
    )
    binds = {
        "alert_name": alert_name,
        "last_sent_utc_date": current.date(),
        "last_sent_at": current,
        "last_status": status,
        "last_detail_hash": detail_hash,
    }
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, binds)
        conn.commit()


def should_send_alert_once_per_day(
    alert_name: str,
    *,
    detail: Optional[str] = None,
    status: str,
    now: Optional[_dt.datetime] = None,
) -> bool:
    """Return True if alert sent and state was recorded."""

    if not should_send_alert(alert_name, detail=detail, now=now):
        return False
    mark_alert_sent(alert_name, status=status, detail=detail, now=now)
    return True


__all__ = [
    "mark_alert_sent",
    "should_send_alert",
    "should_send_alert_once_per_day",
    "should_send_today",
]
