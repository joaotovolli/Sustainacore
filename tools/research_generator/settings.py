"""Oracle-backed settings for research generator."""
from __future__ import annotations

import datetime as dt
from typing import Any, Dict, Optional

from .oracle import get_connection, table_exists


DEFAULTS: Dict[str, Any] = {
    "settings_id": 1,
    "schedule_enabled": "Y",
    "schedule_tz": "UTC",
    "schedule_hour": 3,
    "schedule_minute": 0,
    "schedule_freq": "DAILY",
    "schedule_dow_mask": None,
    "max_context_pct": 10,
    "saver_mode": "MEDIUM",
    "dev_noop": "N",
}


def ensure_settings(conn) -> None:
    if not table_exists(conn, "PROC_RESEARCH_SETTINGS"):
        raise RuntimeError("PROC_RESEARCH_SETTINGS missing; run init_proc_research_settings.py")
    cur = conn.cursor()
    cur.execute(
        """
        MERGE INTO proc_research_settings target
        USING (SELECT 1 AS settings_id FROM dual) src
        ON (target.settings_id = src.settings_id)
        WHEN NOT MATCHED THEN
          INSERT (
            settings_id, schedule_enabled, schedule_tz, schedule_hour,
            schedule_minute, schedule_freq, schedule_dow_mask,
            max_context_pct, saver_mode, dev_noop, updated_at, updated_by
          ) VALUES (
            1, 'Y', 'UTC', 3, 0, 'DAILY', NULL,
            10, 'MEDIUM', 'N', SYSTIMESTAMP, 'system'
          )
        """
    )
    conn.commit()


def get_settings(conn) -> Dict[str, Any]:
    ensure_settings(conn)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT settings_id, schedule_enabled, schedule_tz, schedule_hour,
               schedule_minute, schedule_freq, schedule_dow_mask,
               max_context_pct, saver_mode, dev_noop, updated_at, updated_by
          FROM proc_research_settings
         WHERE settings_id = 1
        """
    )
    row = cur.fetchone()
    if not row:
        return DEFAULTS.copy()
    return {
        "settings_id": row[0],
        "schedule_enabled": row[1],
        "schedule_tz": row[2],
        "schedule_hour": row[3],
        "schedule_minute": row[4],
        "schedule_freq": row[5],
        "schedule_dow_mask": row[6],
        "max_context_pct": row[7],
        "saver_mode": row[8],
        "dev_noop": row[9],
        "updated_at": row[10],
        "updated_by": row[11],
    }


def update_settings(conn, patch: Dict[str, Any], updated_by: str) -> Dict[str, Any]:
    ensure_settings(conn)
    fields = []
    values: Dict[str, Any] = {"updated_by": updated_by}
    for key, value in patch.items():
        fields.append(f"{key} = :{key}")
        values[key] = value
    if not fields:
        return get_settings(conn)
    fields.append("updated_at = SYSTIMESTAMP")
    fields.append("updated_by = :updated_by")
    cur = conn.cursor()
    cur.execute(
        f"""
        UPDATE proc_research_settings
           SET {', '.join(fields)}
         WHERE settings_id = 1
        """,
        values,
    )
    conn.commit()
    return get_settings(conn)
