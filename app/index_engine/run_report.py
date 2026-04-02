"""Formatting helpers for SC_IDX pipeline and daily telemetry reports."""
from __future__ import annotations

import datetime as _dt
import json
import os
from pathlib import Path
from typing import Any, Dict, Optional

PIPELINE_STAGE_ORDER = [
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

HEALTH_LABELS = {
    "success": "Healthy",
    "success_with_degradation": "Degraded",
    "clean_skip": "Skipped",
    "failed": "Failed",
    "blocked": "Blocked",
}

STALE_LABEL = "Stale"


def _val(summary: Dict[str, Any], key: str, default: str = "n/a") -> str:
    value = summary.get(key)
    if value is None:
        return default
    return str(value)


def _truncate(text: Optional[str], limit: int = 800) -> str:
    if not text:
        return ""
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def _parse_datetime(value: Any) -> _dt.datetime | None:
    if value is None:
        return None
    if isinstance(value, _dt.datetime):
        return value
    if isinstance(value, str):
        try:
            return _dt.datetime.fromisoformat(value)
        except ValueError:
            return None
    return None


def _parse_date(value: Any) -> _dt.date | None:
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
            dt_value = _parse_datetime(value)
            return dt_value.date() if dt_value else None
    return None


def _iso(value: Any) -> str | None:
    dt_value = _parse_datetime(value)
    if dt_value is not None:
        if dt_value.tzinfo is None:
            dt_value = dt_value.replace(tzinfo=_dt.timezone.utc)
        return dt_value.astimezone(_dt.timezone.utc).isoformat()
    date_value = _parse_date(value)
    if date_value is not None:
        return date_value.isoformat()
    return None


def _duration_seconds(started_at: Any, ended_at: Any) -> float | None:
    start_dt = _parse_datetime(started_at)
    end_dt = _parse_datetime(ended_at)
    if start_dt is None or end_dt is None:
        return None
    if start_dt.tzinfo is None:
        start_dt = start_dt.replace(tzinfo=_dt.timezone.utc)
    if end_dt.tzinfo is None:
        end_dt = end_dt.replace(tzinfo=_dt.timezone.utc)
    return round(max(0.0, (end_dt - start_dt).total_seconds()), 3)


def _ordered_stage_names(stage_results: Dict[str, Dict[str, Any]]) -> list[str]:
    known = [name for name in PIPELINE_STAGE_ORDER if name in stage_results]
    extra = sorted(name for name in stage_results if name not in PIPELINE_STAGE_ORDER)
    return known + extra


def _alignment_summary(freshness: Dict[str, Any]) -> dict[str, Any]:
    dates = {
        "canon": _parse_date(freshness.get("canon_max_date")),
        "levels": _parse_date(freshness.get("levels_max_date")),
        "stats": _parse_date(freshness.get("stats_max_date")),
        "portfolio_analytics": _parse_date(
            freshness.get("portfolio_analytics_max_date") or freshness.get("portfolio_max_date")
        ),
        "portfolio_positions": _parse_date(
            freshness.get("portfolio_position_max_date") or freshness.get("portfolio_positions_max_date")
        ),
    }
    present = {key: value for key, value in dates.items() if value is not None}
    if not present:
        return {"verdict": "unknown", "reason": "no_freshness_dates"}

    unique_dates = sorted({value for value in present.values()})
    if len(unique_dates) == 1:
        verdict = "aligned"
        reason = "all_dates_match"
    else:
        max_gap = (unique_dates[-1] - unique_dates[0]).days
        if max_gap <= 1:
            verdict = "partially_aligned"
            reason = "minor_date_gap"
        else:
            verdict = "out_of_sync"
            reason = "multi_day_gap"
    return {
        "verdict": verdict,
        "reason": reason,
        "dates": {key: value.isoformat() if value else None for key, value in dates.items()},
    }


def _lag_days(reference_date: Any, subject_date: Any) -> int | None:
    ref = _parse_date(reference_date)
    subject = _parse_date(subject_date)
    if ref is None or subject is None:
        return None
    return max(0, (ref - subject).days)


def _stale_allowed_lag_days() -> int:
    raw = os.getenv("SC_IDX_STALE_ALLOWED_LAG_DAYS", "0").strip()
    try:
        return max(0, int(raw))
    except ValueError:
        return 0


def _freshness_health(
    freshness: Dict[str, Any],
    *,
    expected_target_date: Any = None,
    allowed_lag_days: int | None = None,
) -> dict[str, Any]:
    dates = {
        "canon": _parse_date(freshness.get("canon_max_date")),
        "levels": _parse_date(freshness.get("levels_max_date")),
        "stats": _parse_date(freshness.get("stats_max_date")),
        "portfolio_analytics": _parse_date(
            freshness.get("portfolio_analytics_max_date") or freshness.get("portfolio_max_date")
        ),
        "portfolio_positions": _parse_date(
            freshness.get("portfolio_position_max_date") or freshness.get("portfolio_positions_max_date")
        ),
    }
    expected = _parse_date(expected_target_date)
    allowed = _stale_allowed_lag_days() if allowed_lag_days is None else max(0, int(allowed_lag_days))
    latest_complete_candidates = [
        dates["levels"],
        dates["stats"],
        dates["portfolio_analytics"],
        dates["portfolio_positions"],
    ]
    latest_complete_present = [value for value in latest_complete_candidates if value is not None]
    latest_complete = min(latest_complete_present) if latest_complete_present else None
    lag_days = {key: _lag_days(expected, value) for key, value in dates.items()}
    latest_complete_lag = _lag_days(expected, latest_complete)

    signals: list[str] = []
    if dates["canon"] and dates["levels"] and dates["canon"] > dates["levels"]:
        signals.append("levels_behind_prices")
    if dates["levels"] and dates["stats"] and dates["levels"] > dates["stats"]:
        signals.append("stats_behind_levels")
    if dates["levels"] and dates["portfolio_analytics"] and dates["levels"] > dates["portfolio_analytics"]:
        signals.append("portfolio_analytics_behind_levels")
    if dates["levels"] and dates["portfolio_positions"] and dates["levels"] > dates["portfolio_positions"]:
        signals.append("portfolio_positions_behind_levels")
    if latest_complete_lag is not None and latest_complete_lag > allowed:
        signals.append("latest_complete_lagging_expected")
    for key, lag in lag_days.items():
        if lag is not None and lag > allowed:
            signals.append(f"{key}_lagging_expected")

    deduped_signals: list[str] = []
    for signal in signals:
        if signal not in deduped_signals:
            deduped_signals.append(signal)

    if deduped_signals:
        verdict = "stale"
        reason = deduped_signals[0]
    elif any(value is not None for value in dates.values()):
        verdict = "fresh"
        reason = "aligned_with_expected"
    else:
        verdict = "unknown"
        reason = "no_freshness_dates"

    return {
        "verdict": verdict,
        "reason": reason,
        "expected_target_date": expected.isoformat() if expected else None,
        "latest_complete_date": latest_complete.isoformat() if latest_complete else None,
        "allowed_lag_days": allowed,
        "lag_days": {key: lag for key, lag in lag_days.items()},
        "lag_days_latest_complete": latest_complete_lag,
        "stale_signals": deduped_signals,
    }


def _overall_health_label(terminal_status: str | None, freshness_health: Dict[str, Any] | None = None) -> str:
    if terminal_status in {"failed", "blocked"}:
        return HEALTH_LABELS.get(terminal_status, terminal_status or "Unknown")
    if (freshness_health or {}).get("verdict") == "stale":
        return STALE_LABEL
    return HEALTH_LABELS.get(terminal_status, terminal_status or "Unknown")


def _primary_stage(stage_results: Dict[str, Dict[str, Any]], terminal_status: str, status_reason: str | None) -> str | None:
    if status_reason and status_reason in stage_results:
        return status_reason
    if terminal_status == "success":
        return None
    target_statuses = {
        "failed": {"FAILED", "BLOCKED"},
        "blocked": {"BLOCKED"},
        "success_with_degradation": {"DEGRADED"},
        "clean_skip": {"SKIP"},
    }.get(terminal_status, {"OK"})
    for stage_name in _ordered_stage_names(stage_results):
        if str((stage_results.get(stage_name) or {}).get("status") or "") in target_statuses:
            return stage_name
    return None


def _stage_outcomes(stage_results: Dict[str, Dict[str, Any]]) -> list[dict[str, Any]]:
    outcomes: list[dict[str, Any]] = []
    for stage_name in _ordered_stage_names(stage_results):
        result = dict(stage_results.get(stage_name) or {})
        counts = result.get("counts") or {}
        outcomes.append(
            {
                "stage": stage_name,
                "status": result.get("status"),
                "duration_sec": result.get("duration_sec"),
                "attempts": int(result.get("attempts") or 0),
                "warnings": list(result.get("warnings") or []),
                "counts": counts,
            }
        )
    return outcomes


def _artifact_paths(context: Dict[str, Any]) -> Dict[str, Any]:
    report_paths = dict(context.get("report_paths") or {})
    telemetry_path = context.get("telemetry_path")
    artifacts = {
        "report_json_path": report_paths.get("json_path"),
        "report_text_path": report_paths.get("text_path"),
        "latest_report_json_path": report_paths.get("latest_json_path"),
        "latest_report_text_path": report_paths.get("latest_text_path"),
        "telemetry_path": telemetry_path,
    }
    return {key: value for key, value in artifacts.items() if value}


def format_run_report(run_id: str, summary: Dict[str, Any], tail_log: Optional[str] = None) -> str:
    """
    Return a compact, human-readable report body.
    Expected keys in summary:
      end_date, provider, max_provider_calls, provider_calls_used,
      raw_upserts, canon_upserts, raw_ok, raw_missing, raw_error,
      max_ok_trade_date, oracle_user, status, error_msg
    """

    lines = [
        f"run_id: {run_id}",
        f"status: {_val(summary, 'status')}",
        f"end_date: {_val(summary, 'end_date')}",
        f"provider: {_val(summary, 'provider')}",
        f"max_provider_calls: {_val(summary, 'max_provider_calls')}",
        f"provider_calls_used: {_val(summary, 'provider_calls_used')}",
        f"raw_upserts: {_val(summary, 'raw_upserts')}",
        f"canon_upserts: {_val(summary, 'canon_upserts')}",
        f"raw_ok: {_val(summary, 'raw_ok')}",
        f"raw_missing: {_val(summary, 'raw_missing')}",
        f"raw_error: {_val(summary, 'raw_error')}",
        f"max_ok_trade_date: {_val(summary, 'max_ok_trade_date')}",
        f"oracle_user: {_val(summary, 'oracle_user')}",
    ]

    err = _truncate(summary.get("error_msg"))
    if err:
        lines.append(f"error: {err}")

    if tail_log:
        lines.append("\nlog tail:\n" + tail_log.strip())

    return "\n".join(lines)


def build_pipeline_run_summary(
    *,
    run_id: str,
    terminal_status: str,
    started_at: str,
    stage_results: Dict[str, Dict[str, Any]],
    context: Dict[str, Any],
    warnings: list[str],
    status_reason: Optional[str],
    root_cause: Optional[str],
    remediation: Optional[str],
) -> Dict[str, Any]:
    counts: Dict[str, Any] = {}
    retry_counts: Dict[str, int] = {}
    stage_statuses: Dict[str, str] = {}
    for stage_name, result in stage_results.items():
        if not isinstance(result, dict):
            continue
        stage_statuses[stage_name] = str(result.get("status") or "UNKNOWN")
        retry_counts[stage_name] = max(0, int(result.get("attempts") or 1) - 1)
        for key, value in (result.get("counts") or {}).items():
            if isinstance(value, (int, float)):
                counts[key] = counts.get(key, 0) + value
            elif key not in counts:
                counts[key] = value

    freshness = {
        "canon_max_date": context.get("max_canon_after_ingest") or context.get("max_canon_before"),
        "levels_max_date": context.get("levels_max_after") or context.get("max_level_before"),
        "stats_max_date": context.get("stats_max_after") or context.get("max_stats_before"),
        "portfolio_analytics_max_date": context.get("portfolio_max_after") or context.get("max_portfolio_before"),
        "portfolio_position_max_date": context.get("portfolio_position_max_after")
        or context.get("max_portfolio_position_before"),
    }
    expected_target_date = (
        context.get("expected_target_date")
        or context.get("ready_end_date")
        or context.get("candidate_end_date")
        or context.get("calendar_max_date")
    )
    freshness_health = _freshness_health(freshness, expected_target_date=expected_target_date)
    freshness.update(
        {
            "expected_target_date": freshness_health.get("expected_target_date"),
            "expected_target_source": context.get("expected_target_source"),
            "latest_complete_date": freshness_health.get("latest_complete_date"),
            "allowed_lag_days": freshness_health.get("allowed_lag_days"),
            "lag_days": freshness_health.get("lag_days"),
            "lag_days_latest_complete": freshness_health.get("lag_days_latest_complete"),
            "health": freshness_health,
        }
    )
    alignment = _alignment_summary(freshness)
    ended_at = context.get("ended_at")
    duration_sec = _duration_seconds(started_at, ended_at)
    stage_outcomes = _stage_outcomes(stage_results)
    primary_stage = _primary_stage(stage_results, terminal_status, status_reason)
    alert_decision = context.get("alert_payload")
    runtime_identity = {
        "repo_root": context.get("repo_root"),
        "repo_head": context.get("repo_head"),
    }

    return {
        "environment": "VM1",
        "job_name": "sc_idx_pipeline",
        "run_id": run_id,
        "terminal_status": terminal_status,
        "overall_health": _overall_health_label(terminal_status, freshness_health),
        "started_at": _iso(started_at),
        "ended_at": _iso(ended_at),
        "duration_sec": duration_sec,
        "status_reason": status_reason,
        "failed_stage": primary_stage,
        "root_cause": root_cause,
        "remediation": remediation,
        "warnings": warnings,
        "counts": counts,
        "latest_data_date": (
            context.get("levels_max_after")
            or context.get("max_level_before")
            or context.get("max_canon_after_ingest")
            or context.get("candidate_end_date")
            or context.get("max_canon_before")
        ),
        "latest_successful_trade_date": context.get("levels_max_after") or context.get("max_level_before"),
        "impacted_date_range": {
            "start": context.get("calc_start_date") or context.get("ingest_start_date"),
            "end": context.get("calc_end_date") or context.get("ready_end_date") or context.get("candidate_end_date"),
        },
        "freshness": freshness,
        "alignment": alignment,
        "stage_statuses": stage_statuses,
        "stage_results": stage_results,
        "stage_outcomes": stage_outcomes,
        "recent_terminal_history": list(context.get("recent_terminal_history") or []),
        "retry_counts": retry_counts,
        "total_retry_count": sum(retry_counts.values()),
        "provider_readiness": {
            "candidate_end_date": context.get("candidate_end_date"),
            "ready_end_date": context.get("ready_end_date"),
            "expected_target_date": freshness_health.get("expected_target_date"),
            "expected_target_source": context.get("expected_target_source"),
            "provider_usage_current": context.get("provider_usage_current"),
            "provider_usage_limit": context.get("provider_usage_limit"),
            "provider_usage_remaining": context.get("provider_usage_remaining"),
            "calls_used_today": context.get("calls_used_today"),
            "daily_limit": context.get("daily_limit"),
            "daily_buffer": context.get("daily_buffer"),
            "max_provider_calls": context.get("max_provider_calls"),
        },
        "operational_state": {
            "provider_ready": bool(context.get("ready_end_date")),
            "ingest_ran": stage_statuses.get("ingest_prices") not in {None, "SKIP"},
            "completeness_status": stage_statuses.get("completeness_check"),
            "imputation_used": bool(counts.get("total_imputed") or counts.get("canon_imputed")),
            "index_completed": stage_statuses.get("calc_index") == "OK",
            "statistics_completed": bool(counts.get("stats_rows")) or stage_statuses.get("calc_index") == "OK",
            "portfolio_completed": stage_statuses.get("portfolio_analytics") == "OK",
            "lock_contention": terminal_status == "blocked" and (primary_stage == "acquire_lock"),
            "freshness_verdict": freshness_health.get("verdict"),
            "freshness_reason": freshness_health.get("reason"),
            "stale_signals": list(freshness_health.get("stale_signals") or []),
        },
        "alert_decision": alert_decision,
        "artifact_paths": _artifact_paths(context),
        "runtime_identity": runtime_identity,
    }


def format_pipeline_terminal_report(summary: Dict[str, Any]) -> str:
    counts = summary.get("counts") or {}
    impacted = summary.get("impacted_date_range") or {}
    readiness = summary.get("provider_readiness") or {}
    freshness = summary.get("freshness") or {}
    alignment = summary.get("alignment") or {}
    alert = summary.get("alert_decision") or {}
    artifacts = summary.get("artifact_paths") or {}
    operational = summary.get("operational_state") or {}
    freshness_health = (freshness.get("health") or {}) if isinstance(freshness.get("health"), dict) else {}
    runtime_identity = summary.get("runtime_identity") or {}

    lines = [
        "SC_IDX / TECH100 Pipeline Terminal Report",
        "",
        "Headline summary:",
        f"- environment: {summary.get('environment', 'VM1')}",
        f"- job_name: {summary.get('job_name', 'sc_idx_pipeline')}",
        f"- run_id: {summary.get('run_id')}",
        f"- overall_health: {summary.get('overall_health')}",
        f"- terminal_status: {summary.get('terminal_status')}",
        f"- failed_stage: {summary.get('failed_stage') or 'n/a'}",
        f"- root_cause: {summary.get('root_cause') or 'n/a'}",
        f"- started_at: {summary.get('started_at') or 'n/a'}",
        f"- ended_at: {summary.get('ended_at') or 'n/a'}",
        f"- duration_sec: {summary.get('duration_sec') if summary.get('duration_sec') is not None else 'n/a'}",
        f"- repo_root: {runtime_identity.get('repo_root') or 'n/a'}",
        f"- repo_head: {runtime_identity.get('repo_head') or 'n/a'}",
        "",
        "Data window and freshness:",
        f"- expected_target_date: {freshness.get('expected_target_date') or readiness.get('expected_target_date') or 'n/a'}",
        f"- expected_target_source: {freshness.get('expected_target_source') or readiness.get('expected_target_source') or 'n/a'}",
        f"- latest_successful_trade_date: {summary.get('latest_successful_trade_date') or 'n/a'}",
        f"- latest_data_date: {summary.get('latest_data_date') or 'n/a'}",
        f"- latest_complete_date: {freshness.get('latest_complete_date') or 'n/a'}",
        f"- lag_days_latest_complete: {freshness.get('lag_days_latest_complete')}",
        f"- impacted_start: {impacted.get('start') or 'n/a'}",
        f"- impacted_end: {impacted.get('end') or 'n/a'}",
        f"- provider_ready_date: {readiness.get('ready_end_date') or 'n/a'}",
        f"- canon_max_date: {freshness.get('canon_max_date') or 'n/a'}",
        f"- levels_max_date: {freshness.get('levels_max_date') or 'n/a'}",
        f"- stats_max_date: {freshness.get('stats_max_date') or 'n/a'}",
        f"- portfolio_analytics_max_date: {freshness.get('portfolio_analytics_max_date') or freshness.get('portfolio_max_date') or 'n/a'}",
        f"- portfolio_position_max_date: {freshness.get('portfolio_position_max_date') or 'n/a'}",
        f"- lag_days_canon: {(freshness.get('lag_days') or {}).get('canon')}",
        f"- lag_days_levels: {(freshness.get('lag_days') or {}).get('levels')}",
        f"- lag_days_stats: {(freshness.get('lag_days') or {}).get('stats')}",
        f"- lag_days_portfolio_analytics: {(freshness.get('lag_days') or {}).get('portfolio_analytics')}",
        f"- lag_days_portfolio_positions: {(freshness.get('lag_days') or {}).get('portfolio_positions')}",
        f"- alignment_verdict: {alignment.get('verdict') or 'n/a'}",
        f"- freshness_verdict: {freshness_health.get('verdict') or 'n/a'}",
        f"- freshness_reason: {freshness_health.get('reason') or 'n/a'}",
        f"- stale_signals: {','.join(freshness_health.get('stale_signals') or []) or 'none'}",
        "",
        "Operational state:",
        f"- provider_ready: {operational.get('provider_ready')}",
        f"- ingest_ran: {operational.get('ingest_ran')}",
        f"- completeness_status: {operational.get('completeness_status') or 'n/a'}",
        f"- imputation_used: {operational.get('imputation_used')}",
        f"- index_completed: {operational.get('index_completed')}",
        f"- statistics_completed: {operational.get('statistics_completed')}",
        f"- portfolio_completed: {operational.get('portfolio_completed')}",
        f"- replacement_calls_used: {counts.get('replacement_calls_used', 'n/a')}",
        f"- provider_calls_used: {counts.get('provider_calls_used', 'n/a')}",
        f"- raw_upserts: {counts.get('raw_upserts', 'n/a')}",
        f"- canon_upserts: {counts.get('canon_upserts', 'n/a')}",
        f"- total_imputed: {counts.get('total_imputed', 'n/a')}",
        f"- levels_rows: {counts.get('levels_rows', 'n/a')}",
        f"- stats_rows: {counts.get('stats_rows', 'n/a')}",
        f"- portfolio_analytics_rows: {counts.get('portfolio_analytics_rows', 'n/a')}",
        f"- portfolio_position_rows: {counts.get('portfolio_position_rows', 'n/a')}",
        "",
        "Alert delivery:",
        f"- alert_name: {alert.get('alert_name') or 'n/a'}",
        f"- decision: {alert.get('decision') or 'n/a'}",
        f"- trigger_reason: {alert.get('trigger_reason') or 'n/a'}",
        f"- email_sent: {alert.get('email_sent')}",
        f"- gate_reason: {((alert.get('gate') or {}).get('reason')) or 'n/a'}",
        f"- deduplicated: {alert.get('deduplicated')}",
        f"- smtp_ready: {((alert.get('delivery') or {}).get('ready')) if alert.get('delivery') is not None else 'n/a'}",
        f"- smtp_delivery_state: {((alert.get('delivery') or {}).get('delivery_state')) or 'n/a'}",
        f"- smtp_message_id: {((alert.get('delivery') or {}).get('message_id')) or 'n/a'}",
        f"- smtp_mail_to_count: {((alert.get('delivery') or {}).get('mail_to_count')) or 'n/a'}",
        f"- smtp_missing_env: {','.join((alert.get('delivery') or {}).get('missing_env') or []) or 'none'}",
        "",
        "Artifacts:",
        f"- report_json_path: {artifacts.get('report_json_path') or 'n/a'}",
        f"- report_text_path: {artifacts.get('report_text_path') or 'n/a'}",
        f"- latest_report_json_path: {artifacts.get('latest_report_json_path') or 'n/a'}",
        f"- latest_report_text_path: {artifacts.get('latest_report_text_path') or 'n/a'}",
        f"- telemetry_path: {artifacts.get('telemetry_path') or 'n/a'}",
        "",
        "Stage outcomes:",
    ]

    for outcome in summary.get("stage_outcomes") or []:
        warnings = ",".join(outcome.get("warnings") or []) or "none"
        counts_json = json.dumps(outcome.get("counts") or {}, sort_keys=True, default=str)
        lines.append(
            "- "
            f"{outcome['stage']}: status={outcome.get('status')} "
            f"duration_sec={outcome.get('duration_sec')} "
            f"attempts={outcome.get('attempts')} "
            f"warnings={warnings} "
            f"counts={counts_json}"
        )

    lines.append("")
    lines.append("Recent terminal history:")
    history = summary.get("recent_terminal_history") or []
    if history:
        for item in history:
            lines.append(f"- {item}")
    else:
        lines.append("- none")

    lines.append("")
    lines.append("Warnings:")
    warnings = summary.get("warnings") or []
    if warnings:
        for warning in warnings:
            lines.append(f"- {warning}")
    else:
        lines.append("- none")

    lines.append("")
    lines.append(f"Recommended next action: {summary.get('remediation') or 'No operator action required.'}")
    return "\n".join(lines)


def build_pipeline_daily_summary(
    *,
    latest_summary: Dict[str, Any] | None,
    latest_telemetry: Dict[str, Any] | None,
    health_snapshot: Dict[str, Any] | None,
    generated_at: str | None = None,
) -> Dict[str, Any]:
    summary = dict(latest_summary or {})
    telemetry = dict(latest_telemetry or {})
    health = dict(health_snapshot or {})

    freshness = {
        "calendar_max_date": health.get("calendar_max_date"),
        "canon_max_date": health.get("canon_max_date") or (summary.get("freshness") or {}).get("canon_max_date"),
        "levels_max_date": health.get("levels_max_date") or (summary.get("freshness") or {}).get("levels_max_date"),
        "stats_max_date": health.get("stats_max_date") or (summary.get("freshness") or {}).get("stats_max_date"),
        "portfolio_analytics_max_date": health.get("portfolio_analytics_max_date")
        or (summary.get("freshness") or {}).get("portfolio_analytics_max_date")
        or (telemetry.get("freshness") or {}).get("portfolio_max_date"),
        "portfolio_position_max_date": health.get("portfolio_position_max_date")
        or (summary.get("freshness") or {}).get("portfolio_position_max_date"),
    }
    expected_target_date = (
        (summary.get("freshness") or {}).get("expected_target_date")
        or (summary.get("provider_readiness") or {}).get("expected_target_date")
        or (summary.get("provider_readiness") or {}).get("ready_end_date")
        or (summary.get("provider_readiness") or {}).get("candidate_end_date")
        or freshness.get("calendar_max_date")
    )
    freshness_health = _freshness_health(freshness, expected_target_date=expected_target_date)
    freshness.update(
        {
            "expected_target_date": freshness_health.get("expected_target_date"),
            "expected_target_source": (summary.get("freshness") or {}).get("expected_target_source")
            or (summary.get("provider_readiness") or {}).get("expected_target_source"),
            "latest_complete_date": freshness_health.get("latest_complete_date"),
            "allowed_lag_days": freshness_health.get("allowed_lag_days"),
            "lag_days": freshness_health.get("lag_days"),
            "lag_days_latest_complete": freshness_health.get("lag_days_latest_complete"),
            "health": freshness_health,
        }
    )
    alignment = _alignment_summary(freshness)
    levels_date = freshness.get("levels_max_date")
    portfolio_positions_date = freshness.get("portfolio_position_max_date")
    portfolio_analytics_date = freshness.get("portfolio_analytics_max_date")
    portfolio_in_sync = bool(
        levels_date
        and levels_date == portfolio_analytics_date
        and levels_date == portfolio_positions_date
    )
    impacted = summary.get("impacted_date_range") or {}
    today_target_processed = bool(
        summary.get("terminal_status") in {"success", "success_with_degradation"}
        and impacted.get("end")
        and impacted.get("end") == freshness.get("levels_max_date")
    )
    stage_results = dict(summary.get("stage_results") or telemetry.get("stage_results") or {})
    alert = dict(summary.get("alert_decision") or telemetry.get("alert") or {})
    provider = dict(summary.get("provider_readiness") or {})
    counts = dict(summary.get("counts") or {})

    report = {
        "generated_at": generated_at or _dt.datetime.now(_dt.timezone.utc).isoformat(),
        "headline": {
            "overall_health": _overall_health_label(
                summary.get("terminal_status") or telemetry.get("terminal_status"),
                freshness_health,
            ),
            "latest_run_id": summary.get("run_id") or telemetry.get("run_id"),
            "terminal_status": summary.get("terminal_status") or telemetry.get("terminal_status"),
            "started_at": summary.get("started_at"),
            "ended_at": summary.get("ended_at"),
            "total_duration_sec": summary.get("duration_sec"),
            "latest_successful_trade_date": summary.get("latest_successful_trade_date") or freshness.get("levels_max_date"),
            "expected_target_date": freshness_health.get("expected_target_date"),
            "latest_complete_date": freshness_health.get("latest_complete_date"),
            "lag_days_latest_complete": freshness_health.get("lag_days_latest_complete"),
            "today_target_processed": today_target_processed,
            "portfolio_in_sync": portfolio_in_sync,
            "repo_root": health.get("repo_root") or ((summary.get("runtime_identity") or {}).get("repo_root")),
            "repo_head": health.get("repo_head") or ((summary.get("runtime_identity") or {}).get("repo_head")),
        },
        "freshness": {
            **freshness,
            "alignment": alignment,
        },
        "stage_outcomes": _stage_outcomes(stage_results),
        "operations": {
            "raw_upserts": counts.get("raw_upserts"),
            "canon_upserts": counts.get("canon_upserts"),
            "imputed_rows": counts.get("total_imputed") or counts.get("canon_imputed"),
            "replacement_calls_used": counts.get("replacement_calls_used"),
            "levels_rows": counts.get("levels_rows"),
            "stats_rows": counts.get("stats_rows"),
            "portfolio_analytics_rows": counts.get("portfolio_analytics_rows"),
            "portfolio_position_rows": counts.get("portfolio_position_rows"),
            "portfolio_optimizer_rows": counts.get("portfolio_optimizer_rows"),
            "provider_usage_current": provider.get("provider_usage_current"),
            "provider_usage_limit": provider.get("provider_usage_limit"),
            "provider_usage_remaining": provider.get("provider_usage_remaining"),
            "calls_used_today": provider.get("calls_used_today"),
            "oracle_preflight_clean": (stage_results.get("preflight_oracle") or {}).get("status") == "OK",
            "lock_contention": (stage_results.get("acquire_lock") or {}).get("status") == "BLOCKED",
            "retry_count": summary.get("total_retry_count"),
        },
        "alerts": {
            "email_sent": bool(alert.get("email_sent")),
            "decision": alert.get("decision"),
            "deduplicated": alert.get("deduplicated"),
            "trigger": alert.get("trigger"),
            "trigger_reason": alert.get("trigger_reason"),
            "gate_reason": (alert.get("gate") or {}).get("reason"),
            "smtp_ready": (alert.get("delivery") or {}).get("ready"),
            "smtp_delivery_state": (alert.get("delivery") or {}).get("delivery_state"),
            "smtp_message_id": (alert.get("delivery") or {}).get("message_id"),
            "smtp_mail_to_count": (alert.get("delivery") or {}).get("mail_to_count"),
            "smtp_missing_env": list((alert.get("delivery") or {}).get("missing_env") or []),
            "root_cause": summary.get("root_cause") or telemetry.get("root_cause"),
            "remediation": summary.get("remediation"),
            "top_warnings": list(summary.get("warnings") or telemetry.get("warnings") or [])[:8],
            "next_action": summary.get("remediation") or "Review the latest pipeline report and rerun the failed stage safely.",
        },
        "artifacts": {
            **dict(summary.get("artifact_paths") or {}),
            "latest_json_report_path": (summary.get("artifact_paths") or {}).get("latest_report_json_path"),
            "latest_text_report_path": (summary.get("artifact_paths") or {}).get("latest_report_text_path"),
            "latest_telemetry_path": (summary.get("artifact_paths") or {}).get("telemetry_path"),
        },
    }
    return report


def format_pipeline_daily_report(report: Dict[str, Any]) -> str:
    headline = report.get("headline") or {}
    freshness = report.get("freshness") or {}
    alignment = freshness.get("alignment") or {}
    freshness_health = (freshness.get("health") or {}) if isinstance(freshness.get("health"), dict) else {}
    operations = report.get("operations") or {}
    alerts = report.get("alerts") or {}
    artifacts = report.get("artifacts") or {}
    daily_artifacts = report.get("daily_artifact_paths") or {}
    email_delivery = report.get("email_delivery") or {}

    lines = [
        "SC_IDX / TECH100 Daily Telemetry Report",
        f"generated_at: {report.get('generated_at')}",
        "",
        "Section 1: Headline summary",
        f"- overall_health: {headline.get('overall_health') or 'Unknown'}",
        f"- latest_run_id: {headline.get('latest_run_id') or 'n/a'}",
        f"- terminal_status: {headline.get('terminal_status') or 'n/a'}",
        f"- started_at: {headline.get('started_at') or 'n/a'}",
        f"- ended_at: {headline.get('ended_at') or 'n/a'}",
        f"- total_duration_sec: {headline.get('total_duration_sec') if headline.get('total_duration_sec') is not None else 'n/a'}",
        f"- expected_target_date: {headline.get('expected_target_date') or 'n/a'}",
        f"- latest_successful_trade_date: {headline.get('latest_successful_trade_date') or 'n/a'}",
        f"- latest_complete_date: {headline.get('latest_complete_date') or 'n/a'}",
        f"- lag_days_latest_complete: {headline.get('lag_days_latest_complete')}",
        f"- todays_target_processed: {headline.get('today_target_processed')}",
        f"- portfolio_in_sync: {headline.get('portfolio_in_sync')}",
        f"- repo_root: {headline.get('repo_root') or 'n/a'}",
        f"- repo_head: {headline.get('repo_head') or 'n/a'}",
        "",
        "Section 2: Freshness and alignment",
        f"- expected_target_date: {freshness.get('expected_target_date') or 'n/a'}",
        f"- expected_target_source: {freshness.get('expected_target_source') or 'n/a'}",
        f"- calendar_max_date: {freshness.get('calendar_max_date') or 'n/a'}",
        f"- canon_max_date: {freshness.get('canon_max_date') or 'n/a'}",
        f"- levels_max_date: {freshness.get('levels_max_date') or 'n/a'}",
        f"- stats_max_date: {freshness.get('stats_max_date') or 'n/a'}",
        f"- portfolio_analytics_max_date: {freshness.get('portfolio_analytics_max_date') or 'n/a'}",
        f"- portfolio_position_max_date: {freshness.get('portfolio_position_max_date') or 'n/a'}",
        f"- latest_complete_date: {freshness.get('latest_complete_date') or 'n/a'}",
        f"- freshness_verdict: {freshness_health.get('verdict') or 'n/a'}",
        f"- freshness_reason: {freshness_health.get('reason') or 'n/a'}",
        f"- allowed_lag_days: {freshness.get('allowed_lag_days')}",
        f"- lag_days_latest_complete: {freshness.get('lag_days_latest_complete')}",
        f"- lag_days_canon: {(freshness.get('lag_days') or {}).get('canon')}",
        f"- lag_days_levels: {(freshness.get('lag_days') or {}).get('levels')}",
        f"- lag_days_stats: {(freshness.get('lag_days') or {}).get('stats')}",
        f"- lag_days_portfolio_analytics: {(freshness.get('lag_days') or {}).get('portfolio_analytics')}",
        f"- lag_days_portfolio_positions: {(freshness.get('lag_days') or {}).get('portfolio_positions')}",
        f"- alignment_verdict: {alignment.get('verdict') or 'n/a'}",
        f"- alignment_reason: {alignment.get('reason') or 'n/a'}",
        f"- stale_signals: {','.join(freshness_health.get('stale_signals') or []) or 'none'}",
        "",
        "Section 3: Stage-by-stage outcome",
    ]

    for stage in report.get("stage_outcomes") or []:
        warnings = ",".join(stage.get("warnings") or []) or "none"
        counts_json = json.dumps(stage.get("counts") or {}, sort_keys=True, default=str)
        lines.append(
            "- "
            f"{stage.get('stage')}: status={stage.get('status')} "
            f"duration_sec={stage.get('duration_sec')} "
            f"attempts={stage.get('attempts')} "
            f"warnings={warnings} "
            f"counts={counts_json}"
        )

    lines.extend(
        [
            "",
            "Section 4: Data quality and operations",
            f"- raw_upserts: {operations.get('raw_upserts', 'n/a')}",
            f"- canon_upserts: {operations.get('canon_upserts', 'n/a')}",
            f"- imputed_rows: {operations.get('imputed_rows', 'n/a')}",
            f"- replacement_calls_used: {operations.get('replacement_calls_used', 'n/a')}",
            f"- levels_rows: {operations.get('levels_rows', 'n/a')}",
            f"- stats_rows: {operations.get('stats_rows', 'n/a')}",
            f"- portfolio_analytics_rows: {operations.get('portfolio_analytics_rows', 'n/a')}",
            f"- portfolio_position_rows: {operations.get('portfolio_position_rows', 'n/a')}",
            f"- portfolio_optimizer_rows: {operations.get('portfolio_optimizer_rows', 'n/a')}",
            f"- provider_usage_current: {operations.get('provider_usage_current', 'n/a')}",
            f"- provider_usage_limit: {operations.get('provider_usage_limit', 'n/a')}",
            f"- provider_usage_remaining: {operations.get('provider_usage_remaining', 'n/a')}",
            f"- calls_used_today: {operations.get('calls_used_today', 'n/a')}",
            f"- oracle_preflight_clean: {operations.get('oracle_preflight_clean')}",
            f"- lock_contention: {operations.get('lock_contention')}",
            f"- retry_count: {operations.get('retry_count', 'n/a')}",
            "",
            "Section 5: Alerts and risk signals",
            f"- email_sent: {alerts.get('email_sent')}",
            f"- decision: {alerts.get('decision') or 'n/a'}",
            f"- trigger: {alerts.get('trigger') or 'n/a'}",
            f"- trigger_reason: {alerts.get('trigger_reason') or 'n/a'}",
            f"- deduplicated: {alerts.get('deduplicated')}",
            f"- gate_reason: {alerts.get('gate_reason') or 'n/a'}",
            f"- smtp_ready: {alerts.get('smtp_ready')}",
            f"- smtp_delivery_state: {alerts.get('smtp_delivery_state') or 'n/a'}",
            f"- smtp_message_id: {alerts.get('smtp_message_id') or 'n/a'}",
            f"- smtp_mail_to_count: {alerts.get('smtp_mail_to_count', 'n/a')}",
            f"- smtp_missing_env: {','.join(alerts.get('smtp_missing_env') or []) or 'none'}",
            f"- root_cause: {alerts.get('root_cause') or 'n/a'}",
            f"- remediation: {alerts.get('remediation') or 'n/a'}",
            f"- next_action: {alerts.get('next_action') or 'n/a'}",
            f"- daily_report_email_attempted: {email_delivery.get('attempted')}",
            f"- daily_report_email_state: {email_delivery.get('delivery_state') or 'n/a'}",
            "- top_warnings:",
        ]
    )
    warnings = alerts.get("top_warnings") or []
    if warnings:
        for warning in warnings:
            lines.append(f"  - {warning}")
    else:
        lines.append("  - none")

    lines.extend(
        [
            "",
            "Section 6: Artifact paths",
            f"- latest_text_report_path: {artifacts.get('latest_text_report_path') or 'n/a'}",
            f"- latest_json_report_path: {artifacts.get('latest_json_report_path') or 'n/a'}",
            f"- latest_telemetry_path: {artifacts.get('latest_telemetry_path') or 'n/a'}",
            f"- daily_report_text_path: {daily_artifacts.get('text_path') or 'n/a'}",
            f"- daily_report_json_path: {daily_artifacts.get('json_path') or 'n/a'}",
            f"- latest_daily_report_text_path: {daily_artifacts.get('latest_text_path') or 'n/a'}",
            f"- latest_daily_report_json_path: {daily_artifacts.get('latest_json_path') or 'n/a'}",
        ]
    )
    return "\n".join(lines)


def write_pipeline_run_artifacts(
    *,
    run_id: str,
    summary: Dict[str, Any],
    report_text: str,
    report_dir: Path,
) -> Dict[str, str]:
    report_dir.mkdir(parents=True, exist_ok=True)
    json_path = report_dir / f"sc_idx_pipeline_{run_id}.json"
    txt_path = report_dir / f"sc_idx_pipeline_{run_id}.txt"
    latest_json_path = report_dir / "sc_idx_pipeline_latest.json"
    latest_txt_path = report_dir / "sc_idx_pipeline_latest.txt"

    json_text = json.dumps(summary, indent=2, sort_keys=True, default=str) + "\n"
    txt_text = report_text + "\n"
    json_path.write_text(json_text, encoding="utf-8")
    txt_path.write_text(txt_text, encoding="utf-8")
    latest_json_path.write_text(json_text, encoding="utf-8")
    latest_txt_path.write_text(txt_text, encoding="utf-8")

    return {
        "json_path": str(json_path),
        "text_path": str(txt_path),
        "latest_json_path": str(latest_json_path),
        "latest_text_path": str(latest_txt_path),
    }


def write_pipeline_daily_artifacts(
    *,
    report_date: str,
    payload: Dict[str, Any],
    report_text: str,
    output_dir: Path,
) -> Dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"sc_idx_daily_report_{report_date}.json"
    txt_path = output_dir / f"sc_idx_daily_report_{report_date}.txt"
    latest_json_path = output_dir / "sc_idx_daily_report_latest.json"
    latest_txt_path = output_dir / "sc_idx_daily_report_latest.txt"

    json_text = json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n"
    txt_text = report_text + "\n"
    json_path.write_text(json_text, encoding="utf-8")
    txt_path.write_text(txt_text, encoding="utf-8")
    latest_json_path.write_text(json_text, encoding="utf-8")
    latest_txt_path.write_text(txt_text, encoding="utf-8")

    return {
        "json_path": str(json_path),
        "text_path": str(txt_path),
        "latest_json_path": str(latest_json_path),
        "latest_text_path": str(latest_txt_path),
    }


__all__ = [
    "PIPELINE_STAGE_ORDER",
    "build_pipeline_daily_summary",
    "build_pipeline_run_summary",
    "format_pipeline_daily_report",
    "format_pipeline_terminal_report",
    "format_run_report",
    "write_pipeline_daily_artifacts",
    "write_pipeline_run_artifacts",
]
