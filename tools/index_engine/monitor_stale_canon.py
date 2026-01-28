from __future__ import annotations

import datetime as _dt
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
APP_ROOT = REPO_ROOT / "app"
for path in (REPO_ROOT, APP_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from tools.oracle.env_bootstrap import load_env_files
from db_helper import get_connection


def _fetch_one(sql: str) -> object | None:
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        return row[0] if row else None


def _fetch_latest_ingest_run() -> tuple[str | None, str | None, _dt.datetime | None]:
    sql = (
        "SELECT status, error_msg, started_at "
        "FROM SC_IDX_JOB_RUNS "
        "WHERE job_name = 'sc_idx_price_ingest' "
        "ORDER BY started_at DESC FETCH FIRST 1 ROWS ONLY"
    )
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        if not row:
            return None, None, None
        return row[0], row[1], row[2]


def _format_date(value: object | None) -> str:
    if value is None:
        return "none"
    if isinstance(value, _dt.datetime):
        return value.date().isoformat()
    if isinstance(value, _dt.date):
        return value.isoformat()
    return str(value)


def _next_ingest_schedule() -> str | None:
    try:
        proc = subprocess.run(
            ["systemctl", "list-timers", "--all", "--no-pager", "sc-idx-price-ingest.timer"],
            check=False,
            capture_output=True,
            text=True,
        )
    except Exception:
        return None
    if proc.returncode != 0:
        return None
    for line in proc.stdout.splitlines():
        if "sc-idx-price-ingest.timer" in line:
            return line.strip()
    return None


def main() -> int:
    load_env_files()

    trading_days_max = _fetch_one("SELECT MAX(trade_date) FROM SC_IDX_TRADING_DAYS")
    canon_max = _fetch_one("SELECT MAX(trade_date) FROM SC_IDX_PRICES_CANON")
    levels_max = _fetch_one("SELECT MAX(trade_date) FROM SC_IDX_LEVELS WHERE index_code='TECH100'")
    stats_max = _fetch_one("SELECT MAX(trade_date) FROM SC_IDX_STATS_DAILY")

    ingest_status, ingest_error, ingest_started = _fetch_latest_ingest_run()
    ingest_error_text = (ingest_error or "").strip() if ingest_error else None

    reason = "ok"
    if ingest_error_text and "trading_days_update_cached" in ingest_error_text:
        reason = "cached_calendar_403"
    if canon_max and trading_days_max:
        if isinstance(canon_max, _dt.datetime):
            canon_max = canon_max.date()
        if isinstance(trading_days_max, _dt.datetime):
            trading_days_max = trading_days_max.date()
        if canon_max < trading_days_max:
            reason = "canon_stale"
    if canon_max and levels_max:
        if isinstance(canon_max, _dt.datetime):
            canon_max = canon_max.date()
        if isinstance(levels_max, _dt.datetime):
            levels_max = levels_max.date()
        if levels_max < canon_max:
            reason = "index_behind_canon"

    schedule_line = _next_ingest_schedule()

    print(
        "sc_idx_canon_monitor: trading_days_max={trading_days_max} canon_max={canon_max} "
        "levels_max={levels_max} stats_max={stats_max} ingest_status={status} "
        "ingest_started_at={started} reason={reason}".format(
            trading_days_max=_format_date(trading_days_max),
            canon_max=_format_date(canon_max),
            levels_max=_format_date(levels_max),
            stats_max=_format_date(stats_max),
            status=ingest_status or "none",
            started=_format_date(ingest_started),
            reason=reason,
        )
    )
    if ingest_error_text:
        print(f"sc_idx_canon_monitor_detail: last_ingest_error={ingest_error_text}")
    if schedule_line:
        print(f"sc_idx_canon_monitor_schedule: {schedule_line}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
