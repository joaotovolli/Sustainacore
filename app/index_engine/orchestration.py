from __future__ import annotations

import argparse
import datetime as _dt
import fcntl
import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Protocol, TypedDict

from langgraph.graph import END, START, StateGraph

REPO_ROOT = Path(__file__).resolve().parents[2]
APP_ROOT = REPO_ROOT / "app"
for path in (REPO_ROOT, APP_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from db_helper import get_connection
from index_engine import db as engine_db
from index_engine import db_index_calc
from index_engine import db_portfolio_analytics
from index_engine.alert_state import evaluate_alert_gate, mark_alert_sent
from index_engine.alerts import send_email_result, smtp_configuration_status
from index_engine.run_log import fetch_calls_used_today, finish_run, start_run
from index_engine.run_report import (
    build_pipeline_run_summary,
    format_pipeline_terminal_report,
    write_pipeline_run_artifacts,
)
from tools.index_engine.env_loader import load_default_env
from tools.index_engine.oracle_preflight import (
    collect_wallet_diagnostics,
    format_wallet_diagnostics,
    probe_oracle_user,
)
from tools.index_engine.pipeline_health import collect_health_snapshot, write_health_artifact
from tools.index_engine.pipeline_state import PipelineStateStore

PIPELINE_NAME = "sc_idx_pipeline"
BASE_DATE = _dt.date(2025, 1, 2)
TRANSIENT_ORA_CODES = {"ORA-12545", "ORA-29002"}
DEFAULT_REPORT_DIR = REPO_ROOT / "tools" / "audit" / "output" / "pipeline_runs"
DEFAULT_TELEMETRY_DIR = REPO_ROOT / "tools" / "audit" / "output" / "pipeline_telemetry"
STAGE_SEQUENCE = [
    "preflight_oracle",
    "acquire_lock",
    "determine_target_dates",
    "readiness_probe",
    "ingest_prices",
    "completeness_check",
    "imputation_or_replacement",
    "calc_index",
    "portfolio_analytics",
    "generate_run_report",
    "decide_alerts",
    "emit_telemetry",
    "persist_terminal_status",
    "release_lock",
]
TERMINAL_SHORT_STATUS = {
    "success": "OK",
    "success_with_degradation": "DEGRADED",
    "clean_skip": "SKIP",
    "failed": "ERROR",
    "blocked": "BLOCKED",
}
SHORT_TO_TERMINAL_STATUS = {value: key for key, value in TERMINAL_SHORT_STATUS.items()}


class StagePolicy(TypedDict):
    max_attempts: int
    backoff_base_sec: float


class PipelineStageResult(TypedDict, total=False):
    stage: str
    status: Literal["OK", "DEGRADED", "SKIP", "FAILED", "BLOCKED"]
    detail: str | None
    detail_json: dict[str, Any]
    error: str | None
    error_token: str | None
    remediation: str | None
    retryable: bool
    attempts: int
    started_at: str
    ended_at: str
    duration_sec: float
    counts: dict[str, Any]
    warnings: list[str]


class PipelineGraphState(TypedDict, total=False):
    run_id: str
    run_date: str
    started_at: str
    resume: bool
    restart: bool
    smoke: bool
    skip_ingest: bool
    smoke_scenario: str
    terminal_status: Literal[
        "success", "success_with_degradation", "clean_skip", "failed", "blocked"
    ] | None
    status_reason: str | None
    root_cause: str | None
    remediation: str | None
    warnings: list[str]
    stage_results: dict[str, PipelineStageResult]
    context: dict[str, Any]
    report: dict[str, Any]
    alert: dict[str, Any]
    telemetry: dict[str, Any]


class PipelineRuntime(Protocol):
    state_store: PipelineStateStore
    report_dir: Path
    telemetry_dir: Path

    def policy_for_stage(self, stage_name: str) -> StagePolicy:
        ...

    def preflight_oracle(self, state: PipelineGraphState) -> dict[str, Any]:
        ...

    def acquire_lock(self, state: PipelineGraphState) -> dict[str, Any]:
        ...

    def determine_target_dates(self, state: PipelineGraphState) -> dict[str, Any]:
        ...

    def readiness_probe(self, state: PipelineGraphState) -> dict[str, Any]:
        ...

    def ingest_prices(self, state: PipelineGraphState) -> dict[str, Any]:
        ...

    def completeness_check(self, state: PipelineGraphState) -> dict[str, Any]:
        ...

    def imputation_or_replacement(self, state: PipelineGraphState) -> dict[str, Any]:
        ...

    def calc_index(self, state: PipelineGraphState) -> dict[str, Any]:
        ...

    def portfolio_analytics(self, state: PipelineGraphState) -> dict[str, Any]:
        ...

    def release_lock(self, state: PipelineGraphState) -> dict[str, Any]:
        ...


def _utc_now() -> _dt.datetime:
    return _dt.datetime.now(_dt.timezone.utc)


def _iso_now() -> str:
    return _utc_now().isoformat()


def _safe_json_default(value: Any) -> Any:
    if isinstance(value, (_dt.datetime, _dt.date)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    return str(value)


def _json_dumps(payload: Any) -> str:
    return json.dumps(payload, sort_keys=True, default=_safe_json_default)


def _json_loads(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        value = json.loads(raw)
    except Exception:
        return {"raw": raw}
    return value if isinstance(value, dict) else {"value": value}


def _git_head() -> str | None:
    try:
        proc = subprocess.run(
            ["git", "-C", str(REPO_ROOT), "rev-parse", "--short", "HEAD"],
            check=False,
            capture_output=True,
            text=True,
        )
    except Exception:
        return None
    if proc.returncode != 0:
        return None
    value = proc.stdout.strip()
    return value or None


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, str(default)).strip()
    try:
        return int(raw)
    except ValueError:
        return default


def _append_warning(state: PipelineGraphState, warning: str) -> None:
    warnings = list(state.get("warnings") or [])
    if warning not in warnings:
        warnings.append(warning)
    state["warnings"] = warnings


def _mark_terminal(
    state: PipelineGraphState,
    *,
    terminal_status: Literal[
        "success", "success_with_degradation", "clean_skip", "failed", "blocked"
    ],
    status_reason: str | None = None,
    root_cause: str | None = None,
    remediation: str | None = None,
) -> None:
    state["terminal_status"] = terminal_status
    if status_reason:
        state["status_reason"] = status_reason
    if root_cause:
        state["root_cause"] = root_cause
    if remediation:
        state["remediation"] = remediation


def _normalize_stage_status(raw: str | None) -> Literal["OK", "DEGRADED", "SKIP", "FAILED", "BLOCKED"]:
    value = (raw or "FAILED").strip().upper()
    if value not in {"OK", "DEGRADED", "SKIP", "FAILED", "BLOCKED"}:
        return "FAILED"
    return value  # type: ignore[return-value]


def _is_oracle_transient_error(exc: Exception | str) -> bool:
    message = str(exc)
    return any(code in message for code in TRANSIENT_ORA_CODES)


def _is_oracle_blocker(error_text: str) -> bool:
    blocker_tokens = (
        "ORA-28759",
        "ORA-01017",
        "DPY-4011",
        "DPY-4027",
        "missing oracle credentials",
        "wallet",
        "permission denied",
    )
    lower = error_text.lower()
    return any(token.lower() in lower for token in blocker_tokens)


def _coerce_date(value: Any) -> _dt.date | None:
    if value is None:
        return None
    if isinstance(value, _dt.datetime):
        return value.date()
    if isinstance(value, _dt.date):
        return value
    if isinstance(value, str):
        try:
            return _dt.date.fromisoformat(value)
        except ValueError:
            return None
    return None


def _fetch_scalar(sql: str, binds: dict[str, Any] | None = None) -> Any:
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, binds or {})
        row = cur.fetchone()
        return row[0] if row else None


def _fetch_recent_terminal_history(limit: int = 5) -> list[str]:
    sql = (
        "SELECT status FROM ("
        "  SELECT status "
        "  FROM SC_IDX_JOB_RUNS "
        "  WHERE job_name = :job_name "
        "    AND status IS NOT NULL "
        "  ORDER BY started_at DESC"
        ") WHERE ROWNUM <= :limit"
    )
    try:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute(sql, {"job_name": PIPELINE_NAME, "limit": max(1, limit)})
            rows = cur.fetchall() or []
    except Exception:
        return []

    history: list[str] = []
    for row in rows:
        raw = str(row[0] or "").strip().upper()
        mapped = SHORT_TO_TERMINAL_STATUS.get(raw)
        if mapped:
            history.append(mapped)
    return history


def _leading_status_count(history: list[str], status: str) -> int:
    count = 0
    for item in history:
        if item != status:
            break
        count += 1
    return count


def _query_calc_counts(start_date: _dt.date, end_date: _dt.date) -> dict[str, int | None]:
    binds = {"start_date": start_date, "end_date": end_date}
    return {
        "levels_rows": int(
            _fetch_scalar(
                "SELECT COUNT(*) FROM SC_IDX_LEVELS WHERE index_code = 'TECH100' "
                "AND trade_date BETWEEN :start_date AND :end_date",
                binds,
            )
            or 0
        ),
        "constituent_rows": int(
            _fetch_scalar(
                "SELECT COUNT(*) FROM SC_IDX_CONSTITUENT_DAILY "
                "WHERE trade_date BETWEEN :start_date AND :end_date",
                binds,
            )
            or 0
        ),
        "contribution_rows": int(
            _fetch_scalar(
                "SELECT COUNT(*) FROM SC_IDX_CONTRIBUTION_DAILY "
                "WHERE trade_date BETWEEN :start_date AND :end_date",
                binds,
            )
            or 0
        ),
        "stats_rows": int(
            _fetch_scalar(
                "SELECT COUNT(*) FROM SC_IDX_STATS_DAILY "
                "WHERE trade_date BETWEEN :start_date AND :end_date",
                binds,
            )
            or 0
        ),
    }


def _fetch_current_max_stats_date() -> _dt.date | None:
    return _coerce_date(_fetch_scalar("SELECT MAX(trade_date) FROM SC_IDX_STATS_DAILY"))


def _fetch_current_max_portfolio_date() -> _dt.date | None:
    return _coerce_date(_fetch_scalar("SELECT MAX(trade_date) FROM SC_IDX_PORTFOLIO_ANALYTICS_DAILY"))


def _query_portfolio_counts(start_date: _dt.date, end_date: _dt.date) -> dict[str, int | None]:
    binds = {"start_date": start_date, "end_date": end_date}
    return {
        "portfolio_analytics_rows": int(
            _fetch_scalar(
                "SELECT COUNT(*) FROM SC_IDX_PORTFOLIO_ANALYTICS_DAILY "
                "WHERE trade_date BETWEEN :start_date AND :end_date",
                binds,
            )
            or 0
        ),
        "portfolio_position_rows": int(
            _fetch_scalar(
                "SELECT COUNT(*) FROM SC_IDX_PORTFOLIO_POSITION_DAILY "
                "WHERE trade_date BETWEEN :start_date AND :end_date",
                binds,
            )
            or 0
        ),
        "portfolio_optimizer_rows": int(
            _fetch_scalar(
                "SELECT COUNT(*) FROM SC_IDX_PORTFOLIO_OPT_INPUTS "
                "WHERE trade_date BETWEEN :start_date AND :end_date",
                binds,
            )
            or 0
        ),
    }


def _select_next_missing_trading_day(
    trading_days: list[_dt.date],
    max_level_date: _dt.date | None,
) -> _dt.date | None:
    for day in trading_days:
        if max_level_date is None or day > max_level_date:
            return day
    return None


def _report_root_cause(state: PipelineGraphState) -> str | None:
    if state.get("root_cause"):
        return state["root_cause"]
    for stage_name in STAGE_SEQUENCE:
        result = (state.get("stage_results") or {}).get(stage_name)
        if not result:
            continue
        if result.get("status") in {"FAILED", "BLOCKED"}:
            return result.get("error_token") or stage_name
    return None


def _derive_terminal_status(
    state: PipelineGraphState,
) -> Literal["success", "success_with_degradation", "clean_skip", "failed", "blocked"]:
    terminal_status = state.get("terminal_status")
    if terminal_status:
        return terminal_status
    results = state.get("stage_results") or {}
    statuses = {result.get("status") for result in results.values()}
    if "BLOCKED" in statuses:
        return "blocked"
    if "FAILED" in statuses:
        return "failed"
    if "DEGRADED" in statuses or state.get("warnings"):
        return "success_with_degradation"
    if statuses and statuses.issubset({"SKIP"}):
        return "clean_skip"
    return "success"


def _exit_code_for_terminal(
    terminal_status: Literal["success", "success_with_degradation", "clean_skip", "failed", "blocked"],
) -> int:
    if terminal_status in {"success", "success_with_degradation", "clean_skip"}:
        return 0
    if terminal_status == "blocked":
        return 2
    return 1


@dataclass
class PipelineArgs:
    resume: bool = True
    restart: bool = False
    report_dir: Path | None = None
    smoke: bool = False
    smoke_scenario: str = "success"
    skip_ingest: bool = False


class SCIdxPipelineRuntime:
    def __init__(
        self,
        *,
        report_dir: Path | None = None,
        telemetry_dir: Path | None = None,
        lock_path: Path | None = None,
        state_store: PipelineStateStore | None = None,
    ) -> None:
        load_default_env()
        self.state_store = state_store or PipelineStateStore(pipeline_name=PIPELINE_NAME)
        self.report_dir = report_dir or DEFAULT_REPORT_DIR
        self.telemetry_dir = telemetry_dir or DEFAULT_TELEMETRY_DIR
        self.lock_path = lock_path or Path("/tmp/sc_idx_pipeline.lock")
        self._lock_handle: Any = None

    def _load_run_daily_module(self):
        from tools.index_engine import run_daily

        return run_daily

    def _load_ingest_module(self):
        from tools.index_engine import ingest_prices

        return ingest_prices

    def _load_calc_module(self):
        from tools.index_engine import calc_index

        return calc_index

    def _load_portfolio_analytics_module(self):
        from tools.index_engine import build_portfolio_analytics

        return build_portfolio_analytics

    def _load_completeness_module(self):
        from tools.index_engine import check_price_completeness

        return check_price_completeness

    def _load_impute_module(self):
        from tools.index_engine import impute_missing_prices

        return impute_missing_prices

    def policy_for_stage(self, stage_name: str) -> StagePolicy:
        oracle_attempts = max(1, int(os.getenv("SC_IDX_ORACLE_RETRY_ATTEMPTS", "3")))
        oracle_backoff = max(0.0, float(os.getenv("SC_IDX_ORACLE_RETRY_BASE_SEC", "1")))
        stage_specific = {
            "preflight_oracle": {"max_attempts": oracle_attempts, "backoff_base_sec": oracle_backoff},
            "determine_target_dates": {"max_attempts": min(3, oracle_attempts), "backoff_base_sec": oracle_backoff},
            "ingest_prices": {"max_attempts": 2, "backoff_base_sec": 1.0},
            "completeness_check": {"max_attempts": 2, "backoff_base_sec": oracle_backoff},
            "imputation_or_replacement": {"max_attempts": 2, "backoff_base_sec": oracle_backoff},
            "calc_index": {"max_attempts": 2, "backoff_base_sec": oracle_backoff},
            "portfolio_analytics": {"max_attempts": 2, "backoff_base_sec": oracle_backoff},
        }
        return stage_specific.get(stage_name, {"max_attempts": 1, "backoff_base_sec": 0.0})

    def preflight_oracle(self, state: PipelineGraphState) -> dict[str, Any]:
        try:
            oracle_user = probe_oracle_user()
            return {
                "status": "OK",
                "detail": f"oracle_user={oracle_user}",
                "counts": {"oracle_user": oracle_user},
                "context": {"oracle_user": oracle_user},
            }
        except Exception as exc:
            diagnostics = format_wallet_diagnostics(collect_wallet_diagnostics())
            artifact = self.report_dir.parent / f"oracle_preflight_{state['run_id']}.txt"
            artifact.parent.mkdir(parents=True, exist_ok=True)
            artifact.write_text(diagnostics + "\n", encoding="utf-8")
            error_text = str(exc)
            blocked = _is_oracle_blocker(error_text)
            return {
                "status": "BLOCKED" if blocked else "FAILED",
                "detail": "oracle_preflight_failed",
                "error": error_text,
                "error_token": "oracle_preflight_failed",
                "retryable": _is_oracle_transient_error(exc) and not blocked,
                "remediation": "Run `python3 tools/oracle/preflight_oracle.py` on VM1 and fix wallet or DB env access.",
                "counts": {"diagnostic_artifact": str(artifact)},
            }

    def acquire_lock(self, state: PipelineGraphState) -> dict[str, Any]:
        if self._lock_handle is not None:
            return {"status": "OK", "detail": "lock_already_held"}
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.lock_path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            handle.close()
            return {
                "status": "BLOCKED",
                "detail": f"lock_busy:{self.lock_path}",
                "error": "another sc_idx_pipeline run is active",
                "error_token": "lock_busy",
                "remediation": "Wait for the active run to conclude, then rerun the pipeline once.",
            }
        self._lock_handle = handle
        return {"status": "OK", "detail": f"lock_path={self.lock_path}"}

    def determine_target_dates(self, state: PipelineGraphState) -> dict[str, Any]:
        run_daily = self._load_run_daily_module()
        provider = run_daily._load_provider_module()
        trading_days_module = run_daily._load_trading_days_module()

        today_utc = _utc_now().date()
        probe_symbol = os.getenv(run_daily.PROBE_SYMBOL_ENV, "AAPL")
        usage_current = None
        usage_limit = None
        warnings: list[str] = []
        try:
            usage = provider.fetch_api_usage()
            usage_current = usage.get("current_usage")
            usage_limit = usage.get("plan_limit")
        except Exception as exc:
            warnings.append(f"provider_usage_unavailable:{exc}")

        calls_used_today = fetch_calls_used_today("MARKET_DATA")
        daily_limit = int(os.getenv(run_daily.DAILY_LIMIT_ENV, str(run_daily.DEFAULT_DAILY_LIMIT)))
        daily_buffer = int(
            os.getenv(
                run_daily.DAILY_BUFFER_ENV,
                os.getenv(run_daily.BUFFER_ENV, str(run_daily.DEFAULT_BUFFER)),
            )
        )
        remaining_daily, max_provider_calls = run_daily._compute_daily_budget(
            daily_limit=daily_limit,
            daily_buffer=daily_buffer,
            calls_used_today=calls_used_today,
        )
        provider_usage_remaining = None
        if usage_limit is not None and usage_current is not None:
            try:
                provider_usage_remaining = int(usage_limit) - int(usage_current)
            except Exception:
                provider_usage_remaining = None

        refresh_attempts = max(1, int(os.getenv(run_daily.TRADING_DAYS_RETRY_ENV, "3")))
        refresh_backoff = max(0.0, float(os.getenv(run_daily.TRADING_DAYS_RETRY_BASE_ENV, "1")))
        updated, update_error = trading_days_module.update_trading_days_with_retry(
            auto_extend=True,
            max_attempts=refresh_attempts,
            backoff_base_sec=refresh_backoff,
            allow_cached_on_403=True,
            allow_cached_on_timeout=True,
        )
        if not updated and update_error:
            warnings.append(f"trading_days_cached:{update_error}")

        provider_latest, trading_days, candidate_end = run_daily._resolve_end_date(provider, probe_symbol, today_utc)
        if not trading_days:
            return {
                "status": "BLOCKED",
                "detail": "no_trading_days_in_calendar",
                "error": "SC_IDX_TRADING_DAYS is empty",
                "error_token": "no_trading_days_in_calendar",
                "remediation": "Run `python3 tools/index_engine/update_trading_days.py --start 2025-01-02` on VM1.",
            }

        calendar_max = trading_days[-1]
        expected_target_date, expected_target_source, fallback_trading_days, synthetic_days = (
            run_daily.derive_expected_target_date(
                provider_latest=provider_latest,
                today_utc=today_utc,
                trading_days=trading_days,
                allow_weekday_fallback=not updated,
            )
        )
        if synthetic_days:
            trading_days = fallback_trading_days
            calendar_max = trading_days[-1]
            candidate_end = run_daily.compute_eligible_end_date(
                provider_latest=provider_latest,
                today_utc=today_utc,
                trading_days=trading_days,
            )
            warnings.append(
                "trading_days_weekday_fallback:"
                f"start={synthetic_days[0].isoformat()} end={synthetic_days[-1].isoformat()} count={len(synthetic_days)}"
            )
        if calendar_max and provider_latest and calendar_max < provider_latest:
            if not updated:
                warnings.append(
                    "trading_days_behind_provider:"
                    f"calendar_max={calendar_max.isoformat()} provider_latest={provider_latest.isoformat()}"
                )
                if expected_target_source == "provider_guard_estimate" and expected_target_date is not None:
                    warnings.append(
                        "expected_target_estimated:"
                        f"expected_target_date={expected_target_date.isoformat()}"
                    )
            else:
                return {
                    "status": "FAILED",
                    "detail": "trading_days_behind_provider",
                    "error": (
                        "trading_days_behind_provider:"
                        f"latest={provider_latest.isoformat()} max_trading_day={calendar_max.isoformat()}"
                    ),
                    "error_token": "trading_days_behind_provider",
                    "retryable": True,
                    "remediation": "Refresh the trading-day calendar on VM1, then rerun the pipeline.",
                }

        max_canon_date = engine_db.fetch_max_canon_trade_date()
        max_level_date = db_index_calc.fetch_max_level_date()
        max_portfolio_date = db_portfolio_analytics.fetch_portfolio_analytics_max_date()
        next_missing_canon = run_daily.select_next_missing_trading_day(trading_days, max_canon_trade_date=max_canon_date)
        next_missing_level = _select_next_missing_trading_day(trading_days, max_level_date)
        ingest_required = (
            not state.get("skip_ingest")
            and candidate_end is not None
            and next_missing_canon is not None
            and next_missing_canon <= candidate_end
        )
        if max_provider_calls <= 0:
            return {
                "status": "SKIP",
                "detail": "daily_budget_stop",
                "terminal_status": "clean_skip",
                "error_token": "daily_budget_stop",
                "counts": {
                    "calls_used_today": calls_used_today,
                    "daily_limit": daily_limit,
                    "daily_buffer": daily_buffer,
                    "remaining_daily": remaining_daily,
                },
                "context": {
                    "calendar_max_date": calendar_max,
                    "provider_latest_date": provider_latest,
                    "candidate_end_date": candidate_end,
                    "expected_target_date": expected_target_date,
                    "expected_target_source": expected_target_source,
                    "trading_days": trading_days,
                    "synthetic_trading_days": [day.isoformat() for day in synthetic_days],
                    "calls_used_today": calls_used_today,
                    "daily_limit": daily_limit,
                    "daily_buffer": daily_buffer,
                    "remaining_daily": remaining_daily,
                    "max_provider_calls": 0,
                    "provider_usage_current": usage_current,
                    "provider_usage_limit": usage_limit,
                    "provider_usage_remaining": provider_usage_remaining,
                },
                "remediation": "Wait for the UTC budget window to reset or raise the SC_IDX daily call limit.",
            }

        if candidate_end is None and not ingest_required and next_missing_level is None:
            return {
                "status": "SKIP",
                "detail": "up_to_date",
                "terminal_status": "clean_skip",
                "error_token": "up_to_date",
                "counts": {"calendar_max_date": calendar_max.isoformat()},
                "context": {
                    "calendar_max_date": calendar_max,
                    "provider_latest_date": provider_latest,
                    "candidate_end_date": candidate_end,
                    "expected_target_date": expected_target_date,
                    "expected_target_source": expected_target_source,
                    "trading_days": trading_days,
                    "synthetic_trading_days": [day.isoformat() for day in synthetic_days],
                    "max_canon_before": max_canon_date,
                    "max_level_before": max_level_date,
                    "max_portfolio_before": max_portfolio_date,
                },
            }

        status = "DEGRADED" if warnings else "OK"
        detail = (
            f"candidate_end={candidate_end.isoformat() if candidate_end else None} "
            f"next_missing_canon={next_missing_canon.isoformat() if next_missing_canon else None} "
            f"next_missing_level={next_missing_level.isoformat() if next_missing_level else None}"
        )
        context = {
            "today_utc": today_utc,
            "calendar_max_date": calendar_max,
            "provider_latest_date": provider_latest,
            "candidate_end_date": candidate_end,
            "expected_target_date": expected_target_date,
            "expected_target_source": expected_target_source,
            "trading_days": trading_days,
            "synthetic_trading_days": [day.isoformat() for day in synthetic_days],
            "next_missing_canon_date": next_missing_canon,
            "next_missing_level_date": next_missing_level,
            "ingest_start_date": next_missing_canon or candidate_end,
            "ingest_required": ingest_required,
            "max_provider_calls": max_provider_calls,
            "provider_calls_remaining": max_provider_calls,
            "calls_used_today": calls_used_today,
            "daily_limit": daily_limit,
            "daily_buffer": daily_buffer,
            "remaining_daily": remaining_daily,
            "provider_usage_current": usage_current,
            "provider_usage_limit": usage_limit,
            "provider_usage_remaining": provider_usage_remaining,
            "max_canon_before": max_canon_date,
            "max_level_before": max_level_date,
            "max_portfolio_before": max_portfolio_date,
        }
        return {
            "status": status,
            "detail": detail,
            "warnings": warnings,
            "counts": {
                "candidate_end_date": candidate_end,
                "max_provider_calls": max_provider_calls,
                "remaining_daily": remaining_daily,
            },
            "context": context,
        }

    def readiness_probe(self, state: PipelineGraphState) -> dict[str, Any]:
        if state.get("skip_ingest"):
            return {"status": "SKIP", "detail": "skip_ingest_requested"}
        context = state.get("context") or {}
        if not context.get("ingest_required"):
            return {"status": "SKIP", "detail": "ingest_not_required"}

        run_daily = self._load_run_daily_module()
        provider = run_daily._load_provider_module()
        probe_symbol = os.getenv(run_daily.PROBE_SYMBOL_ENV, "SPY")
        candidate_end = _coerce_date(context.get("candidate_end_date"))
        trading_days = context.get("trading_days") or []
        if candidate_end is None or not trading_days:
            return {
                "status": "FAILED",
                "detail": "readiness_missing_inputs",
                "error": "candidate_end_date or trading_days missing",
                "error_token": "readiness_missing_inputs",
            }

        def _probe(day: _dt.date) -> str:
            try:
                rows = provider.fetch_single_day_bar(probe_symbol, day)
            except Exception as exc:
                text = str(exc).lower()
                if "no data" in text or "not ready" in text or "404" in text:
                    return "NO_DATA"
                raise
            return "OK" if rows else "NO_DATA"

        try:
            ready_end, tried = run_daily._probe_with_fallback(candidate_end, trading_days, probe_fn=_probe)
        except Exception as exc:
            return {
                "status": "FAILED",
                "detail": "provider_probe_failed",
                "error": str(exc),
                "error_token": "provider_probe_failed",
                "retryable": True,
                "remediation": "Retry after the provider stabilizes or run `python3 tools/index_engine/debug_provider_availability.py --debug`.",
            }

        if ready_end is None:
            return {
                "status": "SKIP",
                "detail": "provider_not_ready",
                "terminal_status": "clean_skip",
                "error_token": "provider_not_ready",
                "counts": {"tried_dates": [day.isoformat() for day in tried]},
                "context": {
                    "expected_target_date": None,
                    "expected_target_source": "provider_not_ready",
                },
                "remediation": "Wait for the market-data provider to publish the latest EOD bars, then rerun.",
            }

        status = "DEGRADED" if ready_end < candidate_end else "OK"
        detail = f"ready_end={ready_end.isoformat()} candidate_end={candidate_end.isoformat()}"
        return {
            "status": status,
            "detail": detail,
            "counts": {"tried_dates": [day.isoformat() for day in tried]},
            "context": {
                "ready_end_date": ready_end,
                "expected_target_date": ready_end,
                "expected_target_source": "readiness_probe",
            },
            "warnings": [detail] if status == "DEGRADED" else [],
        }

    def ingest_prices(self, state: PipelineGraphState) -> dict[str, Any]:
        if state.get("skip_ingest"):
            return {"status": "SKIP", "detail": "skip_ingest_requested"}
        context = state.get("context") or {}
        if not context.get("ingest_required"):
            return {"status": "SKIP", "detail": "ingest_not_required"}
        start_date = _coerce_date(context.get("ingest_start_date"))
        end_date = _coerce_date(context.get("ready_end_date") or context.get("candidate_end_date"))
        if start_date is None or end_date is None:
            return {
                "status": "FAILED",
                "detail": "ingest_missing_window",
                "error": "ingest_start_date or ingest_end_date missing",
                "error_token": "ingest_missing_window",
            }
        if start_date > end_date:
            return {"status": "SKIP", "detail": "ingest_up_to_date"}

        ingest_module = self._load_ingest_module()
        tickers_env = os.getenv("SC_IDX_TICKERS")
        max_provider_calls = int(context.get("provider_calls_remaining") or context.get("max_provider_calls") or 0)
        args = argparse.Namespace(
            date=None,
            start=start_date.isoformat(),
            end=end_date.isoformat(),
            backfill=True,
            backfill_missing=False,
            tickers=tickers_env,
            debug=False,
            max_provider_calls=max_provider_calls,
        )

        try:
            exit_code, summary = ingest_module._run_backfill(args)
        except Exception as exc:
            return {
                "status": "FAILED",
                "detail": "ingest_exception",
                "error": str(exc),
                "error_token": "ingest_exception",
                "retryable": True,
                "remediation": "Retry the pipeline once; if it repeats, inspect provider and Oracle logs.",
            }

        summary = summary if isinstance(summary, dict) else {}
        provider_calls_used = int(summary.get("provider_calls_used") or 0)
        provider_calls_remaining = max(0, max_provider_calls - provider_calls_used)
        max_ok_trade_date = _coerce_date(summary.get("max_ok_trade_date"))
        max_canon_after = engine_db.fetch_max_canon_trade_date()
        missing_tickers = engine_db.fetch_missing_real_for_trade_date(end_date)

        partial_budget_stop = (
            max_provider_calls > 0
            and provider_calls_used >= max_provider_calls
            and max_ok_trade_date is not None
            and max_ok_trade_date < end_date
        )
        if exit_code != 0:
            return {
                "status": "FAILED",
                "detail": "ingest_exit_code",
                "error": f"ingest_exit_code={exit_code}",
                "error_token": "ingest_exit_code",
                "retryable": True,
                "counts": summary,
            }

        if max_ok_trade_date is None and (max_canon_after is None or max_canon_after < end_date):
            return {
                "status": "FAILED",
                "detail": "ingest_missing_ok_prices",
                "error": "ingest_missing_ok_prices",
                "error_token": "ingest_missing_ok_prices",
                "counts": summary,
            }

        if max_ok_trade_date is not None and max_ok_trade_date < end_date and not partial_budget_stop:
            return {
                "status": "FAILED",
                "detail": "ingest_incomplete",
                "error": f"max_ok_trade_date={max_ok_trade_date.isoformat()} target={end_date.isoformat()}",
                "error_token": "ingest_incomplete",
                "counts": summary,
            }

        warnings: list[str] = []
        status = "OK"
        if partial_budget_stop:
            status = "DEGRADED"
            warnings.append(
                f"daily_budget_partial:max_ok_trade_date={max_ok_trade_date.isoformat() if max_ok_trade_date else None}"
            )
        if missing_tickers:
            status = "DEGRADED"
            warnings.append(
                "missing_prices_for_date:"
                f"date={end_date.isoformat()} missing_count={len(missing_tickers)} sample={','.join(missing_tickers[:10])}"
            )

        return {
            "status": status,
            "detail": f"ingest_window={start_date.isoformat()}..{end_date.isoformat()}",
            "warnings": warnings,
            "counts": {
                **summary,
                "missing_real_count": len(missing_tickers),
                "max_canon_after": max_canon_after,
            },
            "context": {
                "provider_calls_remaining": provider_calls_remaining,
                "max_canon_after_ingest": max_canon_after,
                "latest_ingest_end_date": end_date,
                "missing_real_after_ingest": missing_tickers,
                "partial_budget_stop": partial_budget_stop,
            },
        }

    def completeness_check(self, state: PipelineGraphState) -> dict[str, Any]:
        check_module = self._load_completeness_module()
        ingest_module = self._load_ingest_module()
        context = state.get("context") or {}
        trading_days = context.get("trading_days") or []
        if not trading_days:
            return {
                "status": "FAILED",
                "detail": "no_trading_days",
                "error": "trading_days missing from state",
                "error_token": "no_trading_days",
            }
        max_level_date = db_index_calc.fetch_max_level_date()
        target_day = _select_next_missing_trading_day(trading_days, max_level_date)
        if target_day is None:
            return {"status": "SKIP", "detail": "no_missing_trading_day"}

        max_canon_date = engine_db.fetch_max_canon_trade_date()
        if max_canon_date is None or max_canon_date < target_day:
            if context.get("partial_budget_stop"):
                return {
                    "status": "DEGRADED",
                    "detail": "canon_lag_after_budget_stop",
                    "warnings": [
                        "canon_lag:"
                        f"max_canon={max_canon_date} target_day={target_day.isoformat()}"
                    ],
                    "context": {"calc_target_day": target_day, "calc_end_date": max_canon_date},
                }
            return {
                "status": "FAILED",
                "detail": "canon_lag",
                "error": f"max_canon={max_canon_date} target_day={target_day.isoformat()}",
                "error_token": "canon_lag",
            }

        end_day = max(day for day in trading_days if day <= max_canon_date)
        missing = engine_db.fetch_missing_real_for_trade_date(target_day)
        if missing and not state.get("skip_ingest"):
            retry_calls = min(
                int(context.get("provider_calls_remaining") or 0),
                min(10, max(1, len(missing))),
            )
            if retry_calls > 0:
                args = argparse.Namespace(
                    date=None,
                    start=target_day.isoformat(),
                    end=target_day.isoformat(),
                    backfill=False,
                    backfill_missing=True,
                    tickers=",".join(sorted(set(missing))),
                    debug=False,
                    max_provider_calls=retry_calls,
                )
                ingest_module._run_backfill_missing(args)
                missing = engine_db.fetch_missing_real_for_trade_date(target_day)
                context["provider_calls_remaining"] = max(
                    0,
                    int(context.get("provider_calls_remaining") or 0) - retry_calls,
                )

        result = check_module.run_check(
            start_date=target_day,
            end_date=end_day,
            min_daily_coverage=1.0,
            max_bad_days=0,
            provider="CANON",
            allow_canon_close=True,
            allow_imputation=True,
            email_on_fail=False,
        )
        status = str(result.get("status") or "FAIL").upper()
        summary_text = str(result.get("summary_text") or "")
        if status == "PASS" and not missing:
            return {
                "status": "OK",
                "detail": f"coverage_ok:{summary_text}",
                "counts": {"target_day": target_day, "end_day": end_day},
                "context": {"calc_target_day": target_day, "calc_end_date": end_day},
            }
        warnings = []
        if missing:
            warnings.append(
                "missing_prices_for_date:"
                f"date={target_day.isoformat()} missing_count={len(missing)} sample={','.join(missing[:10])}"
            )
        if status != "PASS":
            warnings.append(f"canon_incomplete:{summary_text}" if summary_text else "canon_incomplete")
        return {
            "status": "DEGRADED",
            "detail": f"completeness_gap:{summary_text or 'see warnings'}",
            "warnings": warnings,
            "counts": {"target_day": target_day, "end_day": end_day, "missing_real_count": len(missing)},
            "context": {
                "calc_target_day": target_day,
                "calc_end_date": end_day,
                "missing_real_after_completeness": missing,
                "completeness_summary": summary_text,
            },
        }

    def imputation_or_replacement(self, state: PipelineGraphState) -> dict[str, Any]:
        impute_module = self._load_impute_module()
        ingest_module = self._load_ingest_module()
        context = state.get("context") or {}
        target_day = _coerce_date(context.get("calc_target_day"))
        end_day = _coerce_date(context.get("calc_end_date"))
        if target_day is None or end_day is None or end_day < target_day:
            return {"status": "SKIP", "detail": "no_impute_window"}

        replacement_days = int(os.getenv("SC_IDX_IMPUTED_REPLACEMENT_DAYS", "30"))
        replacement_limit = max(0, int(os.getenv("SC_IDX_IMPUTED_REPLACEMENT_LIMIT", "10")))
        replacement_calls = 0
        replacement_tickers: list[str] = []
        if not state.get("skip_ingest") and replacement_limit > 0 and int(context.get("provider_calls_remaining") or 0) > 0:
            replacement_start = max(BASE_DATE, end_day - _dt.timedelta(days=replacement_days))
            impacted_by_date = {
                trade_date: engine_db.fetch_impacted_tickers_for_trade_date(trade_date)
                for trade_date in engine_db.fetch_trading_days(replacement_start, end_day)
            }
            imputed_rows = engine_db.fetch_imputed_rows(replacement_start, end_day)
            replacement_tickers = impute_module.select_replacement_tickers(
                imputed_rows,
                impacted_by_date,
                limit=replacement_limit,
            )
            if replacement_tickers:
                replacement_calls = min(len(replacement_tickers), int(context.get("provider_calls_remaining") or 0))
                if replacement_calls > 0:
                    args = argparse.Namespace(
                        date=None,
                        start=replacement_start.isoformat(),
                        end=end_day.isoformat(),
                        backfill=False,
                        backfill_missing=True,
                        tickers=",".join(replacement_tickers[:replacement_calls]),
                        debug=False,
                        max_provider_calls=replacement_calls,
                    )
                    ingest_module._run_backfill_missing(args)
                    context["provider_calls_remaining"] = max(
                        0,
                        int(context.get("provider_calls_remaining") or 0) - replacement_calls,
                    )

        max_runtime_sec = float(os.getenv("SC_IDX_IMPUTE_TIMEOUT_SEC", "300"))
        try:
            summary = impute_module.impute_missing_prices(
                start_date=target_day,
                end_date=end_day,
                allow_canon_close=True,
                email_on_impute=False,
                max_runtime_sec=max_runtime_sec,
            )
        except TimeoutError as exc:
            return {
                "status": "FAILED",
                "detail": "impute_timeout",
                "error": str(exc),
                "error_token": "impute_timeout",
                "retryable": False,
                "remediation": "Lower the impute window or inspect VM1 memory pressure before retrying.",
            }
        except Exception as exc:
            return {
                "status": "FAILED",
                "detail": "impute_failed",
                "error": str(exc),
                "error_token": "impute_failed",
                "retryable": True,
                "remediation": "Inspect SC_IDX_IMPUTATIONS and Oracle logs, then retry once.",
            }

        recheck = self._load_completeness_module().run_check(
            start_date=target_day,
            end_date=end_day,
            min_daily_coverage=1.0,
            max_bad_days=0,
            provider="CANON",
            allow_canon_close=True,
            allow_imputation=True,
            email_on_fail=False,
        )
        warnings: list[str] = []
        if replacement_tickers:
            warnings.append(f"replacement_attempted:{','.join(replacement_tickers[:replacement_calls])}")
        if summary.get("total_imputed"):
            warnings.append(f"total_imputed={summary.get('total_imputed')}")
        if summary.get("missing_without_prior"):
            return {
                "status": "BLOCKED",
                "detail": "missing_without_prior",
                "error": (
                    f"missing_without_prior={summary.get('missing_without_prior')} "
                    f"window={target_day.isoformat()}..{end_day.isoformat()}"
                ),
                "error_token": "missing_without_prior",
                "counts": summary,
                "remediation": "Backfill the earliest missing TECH100 prices before rerunning the pipeline.",
            }
        if str(recheck.get("status") or "").upper() != "PASS":
            return {
                "status": "FAILED",
                "detail": "canon_incomplete_after_impute",
                "error": str(recheck.get("summary_text") or "canon_incomplete_after_impute"),
                "error_token": "canon_incomplete_after_impute",
                "counts": summary,
            }
        status = "DEGRADED" if warnings else "SKIP"
        return {
            "status": status,
            "detail": f"impute_window={target_day.isoformat()}..{end_day.isoformat()}",
            "warnings": warnings,
            "counts": {
                **summary,
                "replacement_calls_used": replacement_calls,
                "replacement_tickers": replacement_tickers,
            },
            "context": {"imputation_summary": summary},
        }

    def calc_index(self, state: PipelineGraphState) -> dict[str, Any]:
        calc_module = self._load_calc_module()
        context = state.get("context") or {}
        target_day = _coerce_date(context.get("calc_target_day"))
        if target_day is None:
            return {"status": "SKIP", "detail": "no_missing_trading_day"}
        max_canon_date = engine_db.fetch_max_canon_trade_date()
        if max_canon_date is None or max_canon_date < target_day:
            if context.get("partial_budget_stop"):
                return {
                    "status": "DEGRADED",
                    "detail": "calc_waiting_for_future_canon",
                    "warnings": [
                        f"calc_waiting_for_canon:max_canon={max_canon_date} target_day={target_day.isoformat()}"
                    ],
                }
            return {
                "status": "FAILED",
                "detail": "calc_canon_lag",
                "error": f"max_canon={max_canon_date} target_day={target_day.isoformat()}",
                "error_token": "calc_canon_lag",
            }

        trading_days = context.get("trading_days") or engine_db.fetch_trading_days(BASE_DATE, max_canon_date)
        end_day = max(day for day in trading_days if day <= max_canon_date)
        try:
            code = calc_module.main(
                [
                    "--start",
                    target_day.isoformat(),
                    "--end",
                    end_day.isoformat(),
                    "--strict",
                    "--debug",
                    "--no-preflight-self-heal",
                ]
            )
        except Exception as exc:
            return {
                "status": "FAILED",
                "detail": "calc_exception",
                "error": str(exc),
                "error_token": "calc_exception",
                "retryable": True,
                "remediation": "Inspect calc diagnostics and rerun the pipeline once after fixing the failing date window.",
            }
        if code != 0:
            return {
                "status": "FAILED",
                "detail": "calc_exit_code",
                "error": f"calc_exit_code={code}",
                "error_token": "calc_exit_code",
                "retryable": True,
            }

        max_level_after = db_index_calc.fetch_max_level_date()
        max_stats_after = _fetch_current_max_stats_date()
        if max_level_after is None or max_level_after < end_day:
            return {
                "status": "FAILED",
                "detail": "levels_not_advanced",
                "error": f"target_day={target_day.isoformat()} end_day={end_day.isoformat()} max_level={max_level_after}",
                "error_token": "levels_not_advanced",
            }
        if max_stats_after is None or max_stats_after < end_day:
            return {
                "status": "FAILED",
                "detail": "stats_not_advanced",
                "error": f"target_day={target_day.isoformat()} end_day={end_day.isoformat()} max_stats={max_stats_after}",
                "error_token": "stats_not_advanced",
            }

        counts = _query_calc_counts(target_day, end_day)
        return {
            "status": "OK",
            "detail": f"levels_advanced_to={max_level_after.isoformat()}",
            "counts": counts,
            "context": {
                "calc_start_date": target_day,
                "calc_end_date": end_day,
                "levels_max_after": max_level_after,
                "stats_max_after": max_stats_after,
            },
        }

    def portfolio_analytics(self, state: PipelineGraphState) -> dict[str, Any]:
        build_module = self._load_portfolio_analytics_module()
        context = state.get("context") or {}
        trading_days = context.get("trading_days") or []
        max_level_date = _coerce_date(context.get("levels_max_after")) or db_index_calc.fetch_max_level_date()
        if max_level_date is None:
            return {"status": "SKIP", "detail": "no_levels_available"}

        portfolio_max_before = (
            _coerce_date(context.get("max_portfolio_before"))
            or db_portfolio_analytics.fetch_portfolio_analytics_max_date()
        )
        if portfolio_max_before is not None and portfolio_max_before >= max_level_date:
            return {
                "status": "SKIP",
                "detail": f"portfolio_up_to_date={portfolio_max_before.isoformat()}",
                "context": {"portfolio_max_after": portfolio_max_before},
            }

        start_day = _select_next_missing_trading_day(trading_days, portfolio_max_before) if trading_days else None
        if start_day is None:
            start_day = max(BASE_DATE, max_level_date)

        try:
            code = build_module.main(
                [
                    "--apply-ddl",
                    "--skip-preflight",
                    "--start",
                    start_day.isoformat(),
                    "--end",
                    max_level_date.isoformat(),
                ]
            )
        except Exception as exc:
            return {
                "status": "FAILED",
                "detail": "portfolio_analytics_exception",
                "error": str(exc),
                "error_token": "portfolio_analytics_exception",
                "retryable": True,
                "remediation": "Inspect portfolio analytics logs and Oracle state, then rerun once.",
            }
        if code != 0:
            return {
                "status": "FAILED",
                "detail": "portfolio_analytics_exit_code",
                "error": f"portfolio_analytics_exit_code={code}",
                "error_token": "portfolio_analytics_exit_code",
                "retryable": True,
                "remediation": "Run `python3 tools/index_engine/build_portfolio_analytics.py --apply-ddl --dry-run` on VM1.",
            }

        portfolio_max_after = db_portfolio_analytics.fetch_portfolio_analytics_max_date()
        portfolio_position_max_after = db_portfolio_analytics.fetch_portfolio_position_max_date()
        if portfolio_max_after is None or portfolio_max_after < max_level_date:
            return {
                "status": "FAILED",
                "detail": "portfolio_not_advanced",
                "error": (
                    f"start_day={start_day.isoformat()} max_level={max_level_date.isoformat()} "
                    f"max_portfolio={portfolio_max_after}"
                ),
                "error_token": "portfolio_not_advanced",
                "remediation": "Re-run the portfolio refresh for the missing window and verify the additive tables advanced.",
            }

        counts = _query_portfolio_counts(start_day, max_level_date)
        return {
            "status": "OK",
            "detail": f"portfolio_advanced_to={portfolio_max_after.isoformat()}",
            "counts": counts,
            "context": {
                "portfolio_start_date": start_day,
                "portfolio_end_date": max_level_date,
                "portfolio_max_after": portfolio_max_after,
                "portfolio_position_max_after": portfolio_position_max_after,
            },
        }

    def release_lock(self, state: PipelineGraphState) -> dict[str, Any]:
        if self._lock_handle is None:
            return {"status": "SKIP", "detail": "lock_not_held"}
        try:
            fcntl.flock(self._lock_handle.fileno(), fcntl.LOCK_UN)
        finally:
            self._lock_handle.close()
            self._lock_handle = None
        return {"status": "OK", "detail": f"lock_released:{self.lock_path}"}


class SmokePipelineRuntime(SCIdxPipelineRuntime):
    def __init__(self, *, smoke_scenario: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.smoke_scenario = smoke_scenario

    def preflight_oracle(self, state: PipelineGraphState) -> dict[str, Any]:
        if self.smoke_scenario == "blocked":
            return {
                "status": "BLOCKED",
                "detail": "oracle_preflight_failed",
                "error": "smoke_oracle_blocked",
                "error_token": "oracle_preflight_failed",
                "remediation": "Provide Oracle credentials in the VM1 service env.",
            }
        return {"status": "OK", "detail": "oracle_user=SMOKE", "context": {"oracle_user": "SMOKE"}}

    def acquire_lock(self, state: PipelineGraphState) -> dict[str, Any]:
        return {"status": "OK", "detail": "smoke_lock"}

    def determine_target_dates(self, state: PipelineGraphState) -> dict[str, Any]:
        trading_days = [
            _dt.date(2026, 1, 5),
            _dt.date(2026, 1, 6),
            _dt.date(2026, 1, 7),
        ]
        if self.smoke_scenario == "clean_skip":
            return {
                "status": "SKIP",
                "detail": "provider_not_ready",
                "terminal_status": "clean_skip",
                "error_token": "provider_not_ready",
                "context": {"trading_days": trading_days},
            }
        status = "DEGRADED" if self.smoke_scenario == "degraded" else "OK"
        return {
            "status": status,
            "detail": "smoke_targets",
            "warnings": ["smoke_degraded"] if status == "DEGRADED" else [],
            "context": {
                "trading_days": trading_days,
                "candidate_end_date": trading_days[-1],
                "ready_end_date": trading_days[-1],
                "ingest_start_date": trading_days[-1],
                "ingest_required": True,
                "next_missing_level_date": trading_days[-1],
                "calc_target_day": trading_days[-1],
                "calc_end_date": trading_days[-1],
                "max_provider_calls": 3,
                "provider_calls_remaining": 3,
                "provider_usage_current": 1,
                "provider_usage_limit": 8,
                "provider_usage_remaining": 7,
                "calls_used_today": 10,
                "daily_limit": 800,
                "daily_buffer": 25,
            },
        }

    def readiness_probe(self, state: PipelineGraphState) -> dict[str, Any]:
        return {"status": "OK", "detail": "smoke_ready"}

    def ingest_prices(self, state: PipelineGraphState) -> dict[str, Any]:
        if self.smoke_scenario == "failed":
            return {
                "status": "FAILED",
                "detail": "ingest_exit_code",
                "error": "smoke_ingest_failed",
                "error_token": "ingest_exit_code",
            }
        status = "DEGRADED" if self.smoke_scenario == "degraded" else "OK"
        return {
            "status": status,
            "detail": "smoke_ingest",
            "warnings": ["daily_budget_partial"] if status == "DEGRADED" else [],
            "counts": {
                "provider_calls_used": 1,
                "raw_upserts": 25,
                "canon_upserts": 25,
                "raw_ok": 25,
                "raw_missing": 0,
                "raw_error": 0,
            },
            "context": {"max_canon_after_ingest": _dt.date(2026, 1, 7)},
        }

    def completeness_check(self, state: PipelineGraphState) -> dict[str, Any]:
        if self.smoke_scenario == "degraded":
            return {
                "status": "DEGRADED",
                "detail": "smoke_completeness_gap",
                "warnings": ["total_imputed=2"],
            }
        return {"status": "OK", "detail": "smoke_coverage_ok"}

    def imputation_or_replacement(self, state: PipelineGraphState) -> dict[str, Any]:
        if self.smoke_scenario == "degraded":
            return {
                "status": "DEGRADED",
                "detail": "smoke_imputation",
                "counts": {"total_imputed": 2, "canon_imputed": 2, "missing_without_prior": 0},
                "warnings": ["total_imputed=2"],
                "context": {"imputation_summary": {"total_imputed": 2, "canon_imputed": 2}},
            }
        return {"status": "SKIP", "detail": "smoke_no_imputation_needed"}

    def calc_index(self, state: PipelineGraphState) -> dict[str, Any]:
        return {
            "status": "OK",
            "detail": "smoke_levels_advanced",
            "counts": {
                "levels_rows": 1,
                "constituent_rows": 25,
                "contribution_rows": 25,
                "stats_rows": 1,
            },
            "context": {
                "calc_start_date": _dt.date(2026, 1, 7),
                "calc_end_date": _dt.date(2026, 1, 7),
                "levels_max_after": _dt.date(2026, 1, 7),
                "stats_max_after": _dt.date(2026, 1, 7),
            },
        }

    def portfolio_analytics(self, state: PipelineGraphState) -> dict[str, Any]:
        return {
            "status": "OK",
            "detail": "smoke_portfolio_advanced",
            "counts": {
                "portfolio_analytics_rows": 6,
                "portfolio_position_rows": 18,
                "portfolio_optimizer_rows": 18,
            },
            "context": {
                "portfolio_start_date": _dt.date(2026, 1, 7),
                "portfolio_end_date": _dt.date(2026, 1, 7),
                "portfolio_max_after": _dt.date(2026, 1, 7),
                "portfolio_position_max_after": _dt.date(2026, 1, 7),
            },
        }

    def release_lock(self, state: PipelineGraphState) -> dict[str, Any]:
        return {"status": "OK", "detail": "smoke_lock_released"}


def _parse_stage_result_from_store(
    state_store: PipelineStateStore,
    run_id: str,
) -> dict[str, PipelineStageResult]:
    parsed: dict[str, PipelineStageResult] = {}
    for stage_name, record in state_store.fetch_stage_statuses(run_id).items():
        payload = _json_loads(record.details)
        payload["stage"] = stage_name
        payload["status"] = _normalize_stage_status(record.status)
        parsed[stage_name] = payload  # type: ignore[assignment]
    return parsed


def _record_stage_start(
    runtime: PipelineRuntime,
    run_id: str,
    stage_name: str,
    *,
    attempt: int,
    max_attempts: int,
) -> None:
    runtime.state_store.record_stage_start(
        run_id,
        stage_name,
        details=_json_dumps(
            {
                "stage": stage_name,
                "status": "STARTED",
                "attempt": attempt,
                "max_attempts": max_attempts,
                "started_at": _iso_now(),
            }
        ),
    )


def _record_stage_end(
    runtime: PipelineRuntime,
    run_id: str,
    stage_name: str,
    result: PipelineStageResult,
) -> None:
    runtime.state_store.record_stage_end(
        run_id,
        stage_name,
        result["status"],
        details=_json_dumps(result),
    )


def _merge_context(state: PipelineGraphState, updates: dict[str, Any] | None) -> None:
    if not updates:
        return
    context = dict(state.get("context") or {})
    context.update(updates)
    state["context"] = context


def _merge_result(
    state: PipelineGraphState,
    *,
    stage_name: str,
    result: PipelineStageResult,
) -> None:
    stage_results = dict(state.get("stage_results") or {})
    stage_results[stage_name] = result
    state["stage_results"] = stage_results
    for warning in result.get("warnings") or []:
        _append_warning(state, warning)
    _merge_context(state, result.get("detail_json", {}).get("context") if False else None)


def _apply_stage_outcome(
    state: PipelineGraphState,
    stage_name: str,
    result: PipelineStageResult,
) -> None:
    stage_results = dict(state.get("stage_results") or {})
    stage_results[stage_name] = result
    state["stage_results"] = stage_results
    for warning in result.get("warnings") or []:
        _append_warning(state, warning)
    detail_json = result.get("detail_json") or {}
    if detail_json.get("context"):
        _merge_context(state, detail_json["context"])
    current_terminal = state.get("terminal_status")
    if result["status"] == "DEGRADED":
        if current_terminal not in {"failed", "blocked", "clean_skip"}:
            _mark_terminal(
                state,
                terminal_status="success_with_degradation",
                status_reason=result.get("error_token") or stage_name,
            )
    elif result["status"] == "SKIP" and detail_json.get("terminal_status"):
        _mark_terminal(
            state,
            terminal_status=detail_json["terminal_status"],
            status_reason=result.get("error_token") or result.get("detail"),
            remediation=result.get("remediation"),
        )
    elif result["status"] == "FAILED":
        _mark_terminal(
            state,
            terminal_status="failed",
            status_reason=result.get("error_token") or stage_name,
            root_cause=result.get("error_token") or stage_name,
            remediation=result.get("remediation"),
        )
    elif result["status"] == "BLOCKED":
        _mark_terminal(
            state,
            terminal_status="blocked",
            status_reason=result.get("error_token") or stage_name,
            root_cause=result.get("error_token") or stage_name,
            remediation=result.get("remediation"),
        )


def _stage_executor(
    state: PipelineGraphState,
    stage_name: str,
    runtime: PipelineRuntime,
    handler,
) -> PipelineGraphState:
    next_state: PipelineGraphState = dict(state)
    parsed = _parse_stage_result_from_store(runtime.state_store, next_state["run_id"])
    if next_state.get("resume") and stage_name in parsed and parsed[stage_name]["status"] in {"OK", "DEGRADED", "SKIP"}:
        _apply_stage_outcome(next_state, stage_name, parsed[stage_name])
        return next_state

    policy = runtime.policy_for_stage(stage_name)
    last_error_token: str | None = None
    same_error_count = 0
    for attempt in range(1, policy["max_attempts"] + 1):
        _record_stage_start(
            runtime,
            next_state["run_id"],
            stage_name,
            attempt=attempt,
            max_attempts=policy["max_attempts"],
        )
        started = time.monotonic()
        raw_result: dict[str, Any]
        try:
            raw_result = handler(next_state) or {}
        except Exception as exc:
            raw_result = {
                "status": "FAILED",
                "detail": f"{stage_name}_exception",
                "error": str(exc),
                "error_token": stage_name,
                "retryable": _is_oracle_transient_error(exc),
            }

        status = _normalize_stage_status(raw_result.get("status"))
        result: PipelineStageResult = {
            "stage": stage_name,
            "status": status,
            "detail": raw_result.get("detail"),
            "detail_json": {
                "context": raw_result.get("context") or {},
                "counts": raw_result.get("counts") or {},
                "warnings": raw_result.get("warnings") or [],
                "terminal_status": raw_result.get("terminal_status"),
            },
            "error": raw_result.get("error"),
            "error_token": raw_result.get("error_token"),
            "remediation": raw_result.get("remediation"),
            "retryable": bool(raw_result.get("retryable")),
            "attempts": attempt,
            "started_at": _iso_now(),
            "ended_at": _iso_now(),
            "duration_sec": round(time.monotonic() - started, 3),
            "counts": raw_result.get("counts") or {},
            "warnings": raw_result.get("warnings") or [],
        }
        _record_stage_end(runtime, next_state["run_id"], stage_name, result)
        if result["detail_json"].get("context"):
            _merge_context(next_state, result["detail_json"]["context"])
        if status == "OK" and attempt > 1 and next_state.get("status_reason") in {
            stage_name,
            last_error_token,
            result.get("error_token"),
        }:
            next_state["terminal_status"] = None
            next_state["status_reason"] = None
            next_state["root_cause"] = None
            next_state["remediation"] = None
        _apply_stage_outcome(next_state, stage_name, result)

        if status in {"OK", "DEGRADED", "SKIP", "BLOCKED"}:
            return next_state

        error_token = result.get("error_token") or stage_name
        if error_token == last_error_token:
            same_error_count += 1
        else:
            same_error_count = 1
        last_error_token = error_token

        if not result.get("retryable"):
            return next_state
        if same_error_count >= 2:
            _append_warning(next_state, f"repeat_failure:{stage_name}:{error_token}")
            return next_state
        if attempt < policy["max_attempts"]:
            time.sleep(policy["backoff_base_sec"] * (2 ** (attempt - 1)))
    return next_state


def _make_stage_node(stage_name: str, runtime: PipelineRuntime):
    def _node(state: PipelineGraphState) -> PipelineGraphState:
        handler = getattr(runtime, stage_name)
        return _stage_executor(state, stage_name, runtime, handler)

    return _node


def _generate_run_report(state: PipelineGraphState, runtime: PipelineRuntime) -> PipelineGraphState:
    next_state = _refresh_pipeline_report(dict(state), runtime)
    report_paths = (next_state.get("context") or {}).get("report_paths") or {}
    result: PipelineStageResult = {
        "stage": "generate_run_report",
        "status": "OK",
        "detail": f"report_json={report_paths.get('json_path')}",
        "detail_json": {"context": {"report_paths": report_paths}},
        "attempts": 1,
        "started_at": _iso_now(),
        "ended_at": _iso_now(),
        "duration_sec": 0.0,
        "counts": report_paths,
        "warnings": [],
    }
    _record_stage_end(runtime, next_state["run_id"], "generate_run_report", result)
    _apply_stage_outcome(next_state, "generate_run_report", result)
    return next_state


def _refresh_pipeline_report(state: PipelineGraphState, runtime: PipelineRuntime) -> PipelineGraphState:
    next_state: PipelineGraphState = dict(state)
    terminal_status = _derive_terminal_status(next_state)
    next_state["terminal_status"] = terminal_status
    report_context = dict(next_state.get("context") or {})
    summary = build_pipeline_run_summary(
        run_id=next_state["run_id"],
        terminal_status=terminal_status,
        started_at=next_state["started_at"],
        stage_results=next_state.get("stage_results") or {},
        context=report_context,
        warnings=next_state.get("warnings") or [],
        status_reason=next_state.get("status_reason"),
        root_cause=_report_root_cause(next_state),
        remediation=next_state.get("remediation"),
    )
    report_paths = write_pipeline_run_artifacts(
        run_id=next_state["run_id"],
        summary=summary,
        report_text=format_pipeline_terminal_report(summary),
        report_dir=runtime.report_dir,
    )
    report_context.update({"report_paths": report_paths})
    next_state["context"] = report_context
    summary = build_pipeline_run_summary(
        run_id=next_state["run_id"],
        terminal_status=terminal_status,
        started_at=next_state["started_at"],
        stage_results=next_state.get("stage_results") or {},
        context=report_context,
        warnings=next_state.get("warnings") or [],
        status_reason=next_state.get("status_reason"),
        root_cause=_report_root_cause(next_state),
        remediation=next_state.get("remediation"),
    )
    report_paths = write_pipeline_run_artifacts(
        run_id=next_state["run_id"],
        summary=summary,
        report_text=format_pipeline_terminal_report(summary),
        report_dir=runtime.report_dir,
    )
    next_state["report"] = summary
    return next_state


def _decide_alerts(state: PipelineGraphState, runtime: PipelineRuntime) -> PipelineGraphState:
    next_state: PipelineGraphState = dict(state)
    summary = dict(next_state.get("report") or {})
    if not summary:
        next_state = _refresh_pipeline_report(next_state, runtime)
        summary = dict(next_state.get("report") or {})
    terminal_status = _derive_terminal_status(next_state)
    freshness_health = (
        ((summary.get("freshness") or {}).get("health") or {})
        if isinstance((summary.get("freshness") or {}).get("health"), dict)
        else {}
    )
    degraded_repeat_threshold = max(2, _env_int("SC_IDX_ALERT_DEGRADED_REPEAT_THRESHOLD", 2))
    recent_terminal_history = [terminal_status] + _fetch_recent_terminal_history(limit=max(5, degraded_repeat_threshold + 2))
    degraded_streak = _leading_status_count(recent_terminal_history, "success_with_degradation")
    clean_skip_streak = _leading_status_count(recent_terminal_history, "clean_skip")
    _merge_context(next_state, {"recent_terminal_history": recent_terminal_history})
    summary["recent_terminal_history"] = recent_terminal_history
    subject = None
    alert_name = None
    should_send = False
    trigger_reason = None
    if terminal_status in {"failed", "blocked"}:
        alert_name = f"sc_idx_pipeline_{terminal_status}"
        subject = f"SC_IDX pipeline {terminal_status.upper()} on VM1 (run_id={next_state['run_id']})"
        should_send = True
        trigger_reason = terminal_status
    elif freshness_health.get("verdict") == "stale":
        alert_name = "sc_idx_pipeline_stale"
        subject = f"SC_IDX pipeline STALE on VM1 (run_id={next_state['run_id']})"
        should_send = True
        trigger_reason = str(freshness_health.get("reason") or "stale_freshness")
    elif terminal_status == "success_with_degradation" and degraded_streak >= degraded_repeat_threshold:
        alert_name = "sc_idx_pipeline_repeated_degraded"
        subject = (
            "SC_IDX pipeline REPEATED DEGRADED on VM1 "
            f"(run_id={next_state['run_id']} streak={degraded_streak})"
        )
        should_send = True
        trigger_reason = f"repeated_degraded:{degraded_streak}"
    elif next_state.get("status_reason") == "daily_budget_stop" and os.getenv("SC_IDX_EMAIL_ON_BUDGET_STOP") == "1":
        alert_name = "sc_idx_budget_stop"
        subject = f"SC_IDX pipeline DAILY_BUDGET_STOP on VM1 (run_id={next_state['run_id']})"
        should_send = True
        trigger_reason = "daily_budget_stop"
    elif terminal_status == "success_with_degradation" and os.getenv("SC_IDX_EMAIL_ON_DEGRADED") == "1":
        alert_name = "sc_idx_pipeline_degraded"
        subject = f"SC_IDX pipeline DEGRADED on VM1 (run_id={next_state['run_id']})"
        should_send = True
        trigger_reason = "degraded_opt_in"

    alert_payload = {
        "decision": "skipped",
        "alert_name": alert_name,
        "email_sent": False,
        "deduplicated": False,
        "gate": None,
        "delivery": None,
        "trigger": terminal_status,
        "trigger_reason": trigger_reason,
        "freshness_verdict": freshness_health.get("verdict"),
        "stale_signals": list(freshness_health.get("stale_signals") or []),
        "recent_terminal_history": recent_terminal_history,
        "degraded_streak": degraded_streak,
        "clean_skip_streak": clean_skip_streak,
    }
    if next_state.get("smoke"):
        alert_payload["decision"] = "skipped"
    elif should_send and alert_name and subject:
        body = format_pipeline_terminal_report(summary)
        try:
            gate = evaluate_alert_gate(alert_name, detail=body)
            alert_payload["gate"] = gate
            alert_payload["delivery"] = smtp_configuration_status()
            if gate["should_send"]:
                delivery = send_email_result(subject, body)
                alert_payload["delivery"] = delivery
                alert_payload["email_sent"] = bool(delivery.get("ok"))
                if delivery.get("ok"):
                    mark_alert_sent(
                        alert_name,
                        status=TERMINAL_SHORT_STATUS[terminal_status],
                        detail=body,
                    )
                    alert_payload["decision"] = "sent"
                else:
                    alert_payload["decision"] = "send_failed"
                    _append_warning(
                        next_state,
                        "alert_send_failed:"
                        + str(delivery.get("delivery_state") or delivery.get("error_class") or "unknown"),
                    )
            else:
                alert_payload["decision"] = "suppressed"
                alert_payload["deduplicated"] = True
        except Exception as exc:
            alert_payload["decision"] = "error"
            alert_payload["error"] = str(exc)
            _append_warning(next_state, f"alert_decision_failed:{exc}")

    _merge_context(next_state, {"alert_payload": alert_payload})
    next_state["alert"] = alert_payload
    next_state = _refresh_pipeline_report(next_state, runtime)
    result: PipelineStageResult = {
        "stage": "decide_alerts",
        "status": "DEGRADED" if alert_payload.get("decision") in {"error", "send_failed"} else "OK",
        "detail": f"alert_decision={alert_payload['decision']}",
        "detail_json": {"context": {"alert_payload": alert_payload}},
        "attempts": 1,
        "started_at": _iso_now(),
        "ended_at": _iso_now(),
        "duration_sec": 0.0,
        "counts": alert_payload,
        "warnings": (
            [f"alert_decision_failed:{alert_payload['error']}"] if alert_payload.get("error") else []
        )
        + (
            [
                "alert_send_failed:"
                + str(
                    (alert_payload.get("delivery") or {}).get("delivery_state")
                    or (alert_payload.get("delivery") or {}).get("error_class")
                    or "unknown"
                )
            ]
            if alert_payload.get("decision") == "send_failed"
            else []
        ),
    }
    _record_stage_end(runtime, next_state["run_id"], "decide_alerts", result)
    _apply_stage_outcome(next_state, "decide_alerts", result)
    return next_state


def _emit_telemetry(state: PipelineGraphState, runtime: PipelineRuntime) -> PipelineGraphState:
    next_state: PipelineGraphState = dict(state)
    runtime.telemetry_dir.mkdir(parents=True, exist_ok=True)
    report = dict(next_state.get("report") or {})
    alert = dict(next_state.get("alert") or {})
    telemetry_payload = {
        "run_id": next_state["run_id"],
        "terminal_status": _derive_terminal_status(next_state),
        "status_reason": next_state.get("status_reason"),
        "root_cause": _report_root_cause(next_state),
        "remediation": next_state.get("remediation"),
        "started_at": next_state.get("started_at"),
        "ended_at": (next_state.get("context") or {}).get("ended_at"),
        "duration_sec": (report or {}).get("duration_sec"),
        "report_generated": bool(report),
        "email_sent": bool(alert.get("email_sent")),
        "alert": alert,
        "warnings": next_state.get("warnings") or [],
        "stage_results": next_state.get("stage_results") or {},
        "retry_counts": (report or {}).get("retry_counts"),
        "total_retry_count": (report or {}).get("total_retry_count"),
        "freshness": dict((report or {}).get("freshness") or {}),
        "alignment": (report or {}).get("alignment"),
        "provider_readiness": (report or {}).get("provider_readiness"),
        "artifact_paths": dict((report or {}).get("artifact_paths") or {}),
        "runtime_identity": dict((report or {}).get("runtime_identity") or {}),
        "recent_terminal_history": list((report or {}).get("recent_terminal_history") or []),
    }
    telemetry_path = runtime.telemetry_dir / f"sc_idx_pipeline_{next_state['run_id']}.json"
    latest_path = runtime.telemetry_dir / "sc_idx_pipeline_latest.json"
    telemetry_payload["artifact_paths"]["telemetry_path"] = str(telemetry_path)
    payload_text = _json_dumps(telemetry_payload) + "\n"
    telemetry_path.write_text(payload_text, encoding="utf-8")
    latest_path.write_text(payload_text, encoding="utf-8")
    print(f"sc_idx_pipeline_telemetry {payload_text.strip()}", flush=True)
    _merge_context(next_state, {"telemetry_path": str(telemetry_path)})
    next_state["telemetry"] = telemetry_payload
    next_state = _refresh_pipeline_report(next_state, runtime)

    result: PipelineStageResult = {
        "stage": "emit_telemetry",
        "status": "OK",
        "detail": f"telemetry_path={telemetry_path}",
        "detail_json": {"context": {"telemetry_path": str(telemetry_path)}},
        "attempts": 1,
        "started_at": _iso_now(),
        "ended_at": _iso_now(),
        "duration_sec": 0.0,
        "counts": {"telemetry_path": str(telemetry_path)},
        "warnings": [],
    }
    _record_stage_end(runtime, next_state["run_id"], "emit_telemetry", result)
    _apply_stage_outcome(next_state, "emit_telemetry", result)
    return next_state


def _persist_terminal_status(state: PipelineGraphState, runtime: PipelineRuntime) -> PipelineGraphState:
    next_state: PipelineGraphState = dict(state)
    terminal_status = _derive_terminal_status(next_state)
    short_status = TERMINAL_SHORT_STATUS[terminal_status]
    context = dict(next_state.get("context") or {})
    if not context.get("ended_at"):
        context["ended_at"] = _iso_now()
    next_state["context"] = context
    next_state = _refresh_pipeline_report(next_state, runtime)
    report = dict(next_state.get("report") or {})
    telemetry = dict(next_state.get("telemetry") or {})
    telemetry_path = context.get("telemetry_path")
    if telemetry and telemetry_path:
        telemetry["ended_at"] = context.get("ended_at")
        telemetry["duration_sec"] = report.get("duration_sec")
        telemetry["artifact_paths"] = dict((report or {}).get("artifact_paths") or {})
        telemetry["artifact_paths"]["telemetry_path"] = telemetry_path
        payload_text = _json_dumps(telemetry) + "\n"
        telemetry_file = Path(str(telemetry_path))
        telemetry_file.write_text(payload_text, encoding="utf-8")
        (runtime.telemetry_dir / "sc_idx_pipeline_latest.json").write_text(payload_text, encoding="utf-8")
        next_state["telemetry"] = telemetry
    try:
        if not next_state.get("smoke"):
            health = collect_health_snapshot(
                stage_durations={
                    stage_name: float(result.get("duration_sec") or 0.0)
                    for stage_name, result in (next_state.get("stage_results") or {}).items()
                },
                last_error=_report_root_cause(next_state),
            )
            write_health_artifact(health)
            finish_run(
                next_state["run_id"],
                status=short_status,
                provider_calls_used=(report.get("counts") or {}).get("provider_calls_used"),
                raw_upserts=(report.get("counts") or {}).get("raw_upserts"),
                canon_upserts=(report.get("counts") or {}).get("canon_upserts"),
                raw_ok=(report.get("counts") or {}).get("raw_ok"),
                raw_missing=(report.get("counts") or {}).get("raw_missing"),
                raw_error=(report.get("counts") or {}).get("raw_error"),
                max_provider_calls=context.get("max_provider_calls"),
                usage_current=context.get("provider_usage_current"),
                usage_limit=context.get("provider_usage_limit"),
                usage_remaining=context.get("provider_usage_remaining"),
                oracle_user=context.get("oracle_user"),
                error=format_pipeline_terminal_report(report)[:3900] if report else None,
            )
    except Exception as exc:
        _append_warning(next_state, f"persist_terminal_status_failed:{exc}")
    result: PipelineStageResult = {
        "stage": "persist_terminal_status",
        "status": "DEGRADED" if next_state.get("warnings") else "OK",
        "detail": f"terminal_status={terminal_status}",
        "detail_json": {"context": {"terminal_status": terminal_status}},
        "attempts": 1,
        "started_at": _iso_now(),
        "ended_at": _iso_now(),
        "duration_sec": 0.0,
        "counts": {"terminal_status": terminal_status, "short_status": short_status},
        "warnings": [w for w in (next_state.get("warnings") or []) if w.startswith("persist_terminal_status_failed:")],
    }
    _record_stage_end(runtime, next_state["run_id"], "persist_terminal_status", result)
    _apply_stage_outcome(next_state, "persist_terminal_status", result)
    return next_state


def _route_after_preflight(state: PipelineGraphState) -> str:
    terminal = _derive_terminal_status(state)
    if terminal in {"failed", "blocked"}:
        return "generate_run_report"
    return "acquire_lock"


def _route_after_determine(state: PipelineGraphState) -> str:
    terminal = _derive_terminal_status(state)
    if terminal in {"clean_skip", "failed", "blocked"}:
        return "generate_run_report"
    return "readiness_probe"


def _route_after_readiness(state: PipelineGraphState) -> str:
    terminal = _derive_terminal_status(state)
    if terminal in {"clean_skip", "failed", "blocked"}:
        return "generate_run_report"
    return "ingest_prices"


def _route_after_ingest(state: PipelineGraphState) -> str:
    terminal = _derive_terminal_status(state)
    if terminal in {"failed", "blocked"}:
        return "generate_run_report"
    return "completeness_check"


def _route_after_completeness(state: PipelineGraphState) -> str:
    terminal = _derive_terminal_status(state)
    if terminal in {"failed", "blocked"}:
        return "generate_run_report"
    return "imputation_or_replacement"


def _route_after_impute(state: PipelineGraphState) -> str:
    terminal = _derive_terminal_status(state)
    if terminal in {"failed", "blocked"}:
        return "generate_run_report"
    return "calc_index"


def _route_after_calc(state: PipelineGraphState) -> str:
    terminal = _derive_terminal_status(state)
    if terminal in {"failed", "blocked"}:
        return "generate_run_report"
    return "portfolio_analytics"


def _route_after_portfolio(state: PipelineGraphState) -> str:
    terminal = _derive_terminal_status(state)
    if terminal in {"failed", "blocked"}:
        return "generate_run_report"
    return "generate_run_report"


def build_pipeline_graph(runtime: PipelineRuntime):
    graph = StateGraph(PipelineGraphState)
    for stage_name in [
        "preflight_oracle",
        "acquire_lock",
        "determine_target_dates",
        "readiness_probe",
        "ingest_prices",
        "completeness_check",
        "imputation_or_replacement",
        "calc_index",
        "portfolio_analytics",
        "release_lock",
    ]:
        graph.add_node(stage_name, _make_stage_node(stage_name, runtime))
    graph.add_node("generate_run_report", lambda state: _generate_run_report(state, runtime))
    graph.add_node("decide_alerts", lambda state: _decide_alerts(state, runtime))
    graph.add_node("emit_telemetry", lambda state: _emit_telemetry(state, runtime))
    graph.add_node("persist_terminal_status", lambda state: _persist_terminal_status(state, runtime))

    graph.add_edge(START, "preflight_oracle")
    graph.add_conditional_edges("preflight_oracle", _route_after_preflight)
    graph.add_edge("acquire_lock", "determine_target_dates")
    graph.add_conditional_edges("determine_target_dates", _route_after_determine)
    graph.add_conditional_edges("readiness_probe", _route_after_readiness)
    graph.add_conditional_edges("ingest_prices", _route_after_ingest)
    graph.add_conditional_edges("completeness_check", _route_after_completeness)
    graph.add_conditional_edges("imputation_or_replacement", _route_after_impute)
    graph.add_conditional_edges("calc_index", _route_after_calc)
    graph.add_conditional_edges("portfolio_analytics", _route_after_portfolio)
    graph.add_edge("generate_run_report", "decide_alerts")
    graph.add_edge("decide_alerts", "emit_telemetry")
    graph.add_edge("emit_telemetry", "persist_terminal_status")
    graph.add_edge("persist_terminal_status", "release_lock")
    graph.add_edge("release_lock", END)
    return graph.compile()


def _initial_state(args: PipelineArgs, run_id: str) -> PipelineGraphState:
    return {
        "run_id": run_id,
        "run_date": _utc_now().date().isoformat(),
        "started_at": _iso_now(),
        "resume": args.resume and not args.restart,
        "restart": args.restart,
        "smoke": args.smoke,
        "skip_ingest": args.skip_ingest,
        "smoke_scenario": args.smoke_scenario,
        "warnings": [],
        "stage_results": {},
        "context": {
            "repo_root": str(REPO_ROOT.resolve()),
            "repo_head": _git_head(),
        },
        "report": {},
        "alert": {},
        "telemetry": {},
        "terminal_status": None,
    }


def _select_runtime(args: PipelineArgs) -> PipelineRuntime:
    runtime_cls: type[SCIdxPipelineRuntime] = SmokePipelineRuntime if args.smoke else SCIdxPipelineRuntime
    if args.smoke:
        return runtime_cls(smoke_scenario=args.smoke_scenario, report_dir=args.report_dir)  # type: ignore[arg-type]
    return runtime_cls(report_dir=args.report_dir)


def run_pipeline(args: PipelineArgs) -> tuple[int, PipelineGraphState]:
    runtime = _select_runtime(args)
    run_date = _utc_now().date()
    resume_run_id = runtime.state_store.fetch_resume_run_id(run_date=run_date) if args.resume and not args.restart else None
    run_id = resume_run_id or runtime.state_store.create_run_id()
    state = _initial_state(args, run_id)
    if not args.smoke:
        start_run(
            PIPELINE_NAME,
            end_date=run_date,
            provider="PIPELINE",
            max_provider_calls=0,
            meta={"start_date": BASE_DATE, "run_id": run_id},
        )
    graph = build_pipeline_graph(runtime)
    final_state = graph.invoke(state)
    terminal_status = _derive_terminal_status(final_state)
    final_state["terminal_status"] = terminal_status
    return _exit_code_for_terminal(terminal_status), final_state


__all__ = [
    "PipelineArgs",
    "PipelineGraphState",
    "SCIdxPipelineRuntime",
    "SmokePipelineRuntime",
    "TERMINAL_SHORT_STATUS",
    "build_pipeline_graph",
    "run_pipeline",
]
