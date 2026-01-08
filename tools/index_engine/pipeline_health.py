from __future__ import annotations

import datetime as _dt
from pathlib import Path
from typing import Any, Dict

from db_helper import get_connection

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_HEALTH_PATH = REPO_ROOT / "tools" / "audit" / "output" / "pipeline_health_latest.txt"


def _coerce_date(value: Any) -> _dt.date | None:
    if value is None:
        return None
    if isinstance(value, _dt.datetime):
        return value.date()
    if isinstance(value, _dt.date):
        return value
    return None


def _fetch_scalar(sql: str, binds: Dict[str, Any] | None = None) -> Any:
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, binds or {})
        row = cur.fetchone()
        return row[0] if row else None


def _fetch_one(sql: str, binds: Dict[str, Any] | None = None) -> tuple[Any, ...] | None:
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, binds or {})
        return cur.fetchone()


def collect_health_snapshot(
    *,
    stage_durations: Dict[str, float],
    last_error: str | None,
) -> Dict[str, Any]:
    calendar_max = _coerce_date(_fetch_scalar("SELECT MAX(trade_date) FROM SC_IDX_TRADING_DAYS"))

    canon_max = _coerce_date(_fetch_scalar("SELECT MAX(trade_date) FROM SC_IDX_PRICES_CANON"))
    canon_count_latest = None
    if canon_max:
        canon_count_latest = _fetch_scalar(
            "SELECT COUNT(*) FROM SC_IDX_PRICES_CANON WHERE trade_date = :trade_date",
            {"trade_date": canon_max},
        )

    levels_row = _fetch_one(
        "SELECT MAX(trade_date) FROM SC_IDX_LEVELS WHERE index_code = 'TECH100'"
    )
    levels_max = _coerce_date(levels_row[0]) if levels_row else None
    level_latest = None
    if levels_max:
        level_latest = _fetch_scalar(
            "SELECT level_tr FROM SC_IDX_LEVELS WHERE index_code = 'TECH100' AND trade_date = :trade_date",
            {"trade_date": levels_max},
        )

    stats_max = _coerce_date(_fetch_scalar("SELECT MAX(trade_date) FROM SC_IDX_STATS_DAILY"))
    ret_1d_latest = None
    if stats_max:
        ret_1d_latest = _fetch_scalar(
            "SELECT ret_1d FROM SC_IDX_STATS_DAILY WHERE trade_date = :trade_date",
            {"trade_date": stats_max},
        )

    contrib_max = _coerce_date(_fetch_scalar("SELECT MAX(trade_date) FROM SC_IDX_CONTRIBUTION_DAILY"))
    contrib_count_latest = None
    if contrib_max:
        contrib_count_latest = _fetch_scalar(
            "SELECT COUNT(*) FROM SC_IDX_CONTRIBUTION_DAILY WHERE trade_date = :trade_date",
            {"trade_date": contrib_max},
        )

    next_missing = None
    if levels_max:
        next_missing = _coerce_date(
            _fetch_scalar(
                "SELECT MIN(trade_date) FROM SC_IDX_TRADING_DAYS WHERE trade_date > :trade_date",
                {"trade_date": levels_max},
            )
        )
    else:
        next_missing = _coerce_date(_fetch_scalar("SELECT MIN(trade_date) FROM SC_IDX_TRADING_DAYS"))

    oracle_error_count = _fetch_scalar(
        "SELECT COUNT(*) FROM SC_IDX_JOB_RUNS "
        "WHERE started_at >= SYSTIMESTAMP - INTERVAL '1' DAY "
        "AND error_msg LIKE '%ORA-%'"
    )

    return {
        "calendar_max_date": calendar_max.isoformat() if calendar_max else None,
        "canon_max_date": canon_max.isoformat() if canon_max else None,
        "canon_count_latest_day": int(canon_count_latest) if canon_count_latest is not None else None,
        "levels_max_date": levels_max.isoformat() if levels_max else None,
        "level_latest": float(level_latest) if level_latest is not None else None,
        "stats_max_date": stats_max.isoformat() if stats_max else None,
        "ret_1d_latest": float(ret_1d_latest) if ret_1d_latest is not None else None,
        "contrib_max_date": contrib_max.isoformat() if contrib_max else None,
        "contrib_count_latest_day": int(contrib_count_latest) if contrib_count_latest is not None else None,
        "next_missing_trading_day": next_missing.isoformat() if next_missing else None,
        "oracle_error_counts_24h": int(oracle_error_count) if oracle_error_count is not None else None,
        "stage_durations_sec": {k: round(v, 2) for k, v in stage_durations.items()},
        "last_error": last_error,
    }


def format_health_summary(health: Dict[str, Any]) -> str:
    lines = []
    for key in [
        "calendar_max_date",
        "canon_max_date",
        "canon_count_latest_day",
        "levels_max_date",
        "level_latest",
        "stats_max_date",
        "ret_1d_latest",
        "contrib_max_date",
        "contrib_count_latest_day",
        "next_missing_trading_day",
        "oracle_error_counts_24h",
        "last_error",
    ]:
        lines.append(f"{key}={health.get(key)}")
    durations = health.get("stage_durations_sec") or {}
    for stage_name, duration in durations.items():
        lines.append(f"stage_duration_{stage_name}_sec={duration}")
    return "\n".join(lines)


def write_health_artifact(
    health: Dict[str, Any],
    *,
    path: Path | None = None,
) -> Path:
    output_path = path or DEFAULT_HEALTH_PATH
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(format_health_summary(health) + "\n", encoding="utf-8")
    return output_path
