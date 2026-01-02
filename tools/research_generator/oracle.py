"""Oracle helpers for research generator."""
from __future__ import annotations

import datetime as dt
import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import db_helper
from tools.oracle.env_bootstrap import load_env_files

LOGGER = logging.getLogger("research_generator.oracle")


@dataclass
class ApprovalRecord:
    approval_id: int
    status: Optional[str]


@dataclass
class ReportState:
    report_key: str
    report_value: Optional[str]
    updated_at: Optional[dt.datetime]


def init_env() -> None:
    load_env_files()


def get_connection():
    return db_helper.get_connection()


def _read_lob(value: Any) -> Any:
    if value is None:
        return None
    if hasattr(value, "read"):
        try:
            return value.read()
        except Exception:
            return None
    return value


def current_schema(conn) -> Optional[str]:
    cur = conn.cursor()
    cur.execute("SELECT SYS_CONTEXT('USERENV','CURRENT_SCHEMA') FROM dual")
    row = cur.fetchone()
    return row[0] if row else None


def table_exists(conn, table_name: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COUNT(*)
          FROM user_tables
         WHERE table_name = :table_name
        """,
        {"table_name": table_name.upper()},
    )
    row = cur.fetchone()
    return bool(row and row[0])


def ensure_proc_reports(conn) -> None:
    if table_exists(conn, "PROC_REPORTS"):
        return
    raise RuntimeError("PROC_REPORTS missing; run init_proc_reports.sql")


def get_report_value(conn, report_key: str) -> Optional[str]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT report_value
          FROM proc_reports
         WHERE report_key = :report_key
        """,
        {"report_key": report_key},
    )
    row = cur.fetchone()
    return _read_lob(row[0]) if row else None


def set_report_value(conn, report_key: str, report_value: str) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        MERGE INTO proc_reports target
        USING (SELECT :report_key AS report_key, :report_value AS report_value FROM dual) src
        ON (target.report_key = src.report_key)
        WHEN MATCHED THEN
          UPDATE SET target.report_value = src.report_value,
                     target.updated_at = SYSTIMESTAMP
        WHEN NOT MATCHED THEN
          INSERT (report_key, report_value, updated_at)
          VALUES (src.report_key, src.report_value, SYSTIMESTAMP)
        """,
        {"report_key": report_key, "report_value": report_value},
    )
    conn.commit()


def insert_approval(conn, payload: Dict[str, Any]) -> int:
    cur = conn.cursor()
    approval_id = cur.var(int)
    cur.execute(
        """
        INSERT INTO proc_gemini_approvals (
            source_job_id,
            request_type,
            title,
            proposed_text,
            details,
            file_name,
            file_mime,
            file_blob,
            status,
            created_at
        ) VALUES (
            :source_job_id,
            :request_type,
            :title,
            :proposed_text,
            :details,
            :file_name,
            :file_mime,
            :file_blob,
            :status,
            SYSTIMESTAMP
        ) RETURNING approval_id INTO :approval_id
        """,
        {**payload, "approval_id": approval_id},
    )
    conn.commit()
    value = approval_id.getvalue()
    if isinstance(value, list) and value:
        value = value[0]
    return int(value)


def count_pending_approvals(conn) -> int:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COUNT(*)
          FROM proc_gemini_approvals
         WHERE UPPER(TRIM(status)) = 'PENDING'
        """
    )
    row = cur.fetchone()
    return int(row[0]) if row else 0


def fetch_latest_port_dates(conn, limit: int = 2) -> List[dt.date]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT DISTINCT port_date
          FROM tech11_ai_gov_eth_index
         ORDER BY port_date DESC
         FETCH FIRST :limit ROWS ONLY
        """,
        {"limit": limit},
    )
    rows = cur.fetchall()
    return [row[0] for row in rows if row and row[0]]


def fetch_rebalance_rows(conn, port_date: dt.date) -> List[Dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT company_name,
               ticker,
               port_weight,
               gics_sector,
               aiges_composite_average,
               summary,
               source_links
          FROM tech11_ai_gov_eth_index
         WHERE port_date = :port_date
         ORDER BY port_weight DESC
        """,
        {"port_date": port_date},
    )
    rows = cur.fetchall()
    payload = []
    for row in rows:
        payload.append(
            {
                "company": row[0],
                "ticker": row[1],
                "weight": row[2],
                "sector": row[3],
                "aiges": row[4],
                "summary": _read_lob(row[5]),
                "sources": _read_lob(row[6]),
            }
        )
    return payload


def fetch_stats_latest(conn) -> Optional[Dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT trade_date,
               ret_1d,
               ret_5d,
               ret_20d,
               vol_20d,
               max_drawdown_252d,
               n_constituents,
               top5_weight,
               herfindahl
          FROM sc_idx_stats_daily
         ORDER BY trade_date DESC
         FETCH FIRST 1 ROWS ONLY
        """
    )
    row = cur.fetchone()
    if not row:
        return None
    return {
        "trade_date": row[0],
        "ret_1d": row[1],
        "ret_5d": row[2],
        "ret_20d": row[3],
        "vol_20d": row[4],
        "max_drawdown_252d": row[5],
        "n_constituents": row[6],
        "top5_weight": row[7],
        "herfindahl": row[8],
    }


def fetch_stats_window(conn, days: int = 5) -> List[Dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT trade_date,
               ret_1d,
               ret_5d,
               ret_20d,
               vol_20d,
               max_drawdown_252d
          FROM sc_idx_stats_daily
         ORDER BY trade_date DESC
         FETCH FIRST :days ROWS ONLY
        """,
        {"days": days},
    )
    rows = cur.fetchall()
    payload = []
    for row in rows:
        payload.append(
            {
                "trade_date": row[0],
                "ret_1d": row[1],
                "ret_5d": row[2],
                "ret_20d": row[3],
                "vol_20d": row[4],
                "max_drawdown_252d": row[5],
            }
        )
    return payload


def fetch_levels_window(conn, days: int = 20) -> List[Dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT trade_date, level_tr
          FROM sc_idx_levels
         ORDER BY trade_date DESC
         FETCH FIRST :days ROWS ONLY
        """,
        {"days": days},
    )
    rows = cur.fetchall()
    payload = []
    for row in rows:
        payload.append({"trade_date": row[0], "level_tr": row[1]})
    return payload


def fetch_contributions(conn, trade_date: dt.date, limit: int = 8) -> List[Dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT ticker, contribution
          FROM sc_idx_contribution_daily
         WHERE trade_date = :trade_date
         ORDER BY contribution DESC
         FETCH FIRST :limit ROWS ONLY
        """,
        {"trade_date": trade_date, "limit": limit},
    )
    rows = cur.fetchall()
    return [{"ticker": row[0], "contribution": row[1]} for row in rows]


def fetch_latest_trade_date(conn) -> Optional[dt.date]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT trade_date
          FROM sc_idx_levels
         ORDER BY trade_date DESC
         FETCH FIRST 1 ROWS ONLY
        """
    )
    row = cur.fetchone()
    return row[0] if row else None


def serialize_details(payload: Dict[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True)
