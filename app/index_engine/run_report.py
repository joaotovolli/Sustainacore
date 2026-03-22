"""Formatting helpers for SC_IDX run reports."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional


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

    return {
        "run_id": run_id,
        "terminal_status": terminal_status,
        "started_at": started_at,
        "ended_at": context.get("ended_at"),
        "status_reason": status_reason,
        "root_cause": root_cause,
        "remediation": remediation,
        "warnings": warnings,
        "counts": counts,
        "latest_data_date": (
            context.get("max_canon_after_ingest")
            or context.get("candidate_end_date")
            or context.get("max_canon_before")
        ),
        "impacted_date_range": {
            "start": context.get("calc_start_date") or context.get("ingest_start_date"),
            "end": context.get("calc_end_date") or context.get("ready_end_date") or context.get("candidate_end_date"),
        },
        "stage_statuses": stage_statuses,
        "stage_results": stage_results,
        "retry_counts": retry_counts,
        "provider_readiness": {
            "candidate_end_date": context.get("candidate_end_date"),
            "ready_end_date": context.get("ready_end_date"),
            "provider_usage_current": context.get("provider_usage_current"),
            "provider_usage_limit": context.get("provider_usage_limit"),
            "provider_usage_remaining": context.get("provider_usage_remaining"),
            "calls_used_today": context.get("calls_used_today"),
            "daily_limit": context.get("daily_limit"),
            "daily_buffer": context.get("daily_buffer"),
        },
        "alert_decision": context.get("alert_payload"),
        "report_paths": context.get("report_paths"),
    }


def format_pipeline_terminal_report(summary: Dict[str, Any]) -> str:
    lines = [
        f"run_id: {summary.get('run_id')}",
        f"terminal_status: {summary.get('terminal_status')}",
        f"status_reason: {summary.get('status_reason')}",
        f"root_cause: {summary.get('root_cause')}",
        f"started_at: {summary.get('started_at')}",
        f"latest_data_date: {summary.get('latest_data_date')}",
        f"impacted_start: {(summary.get('impacted_date_range') or {}).get('start')}",
        f"impacted_end: {(summary.get('impacted_date_range') or {}).get('end')}",
        f"remediation: {summary.get('remediation')}",
    ]
    counts = summary.get("counts") or {}
    for key in [
        "provider_calls_used",
        "raw_upserts",
        "canon_upserts",
        "total_imputed",
        "canon_imputed",
        "levels_rows",
        "constituent_rows",
        "contribution_rows",
        "stats_rows",
        "replacement_calls_used",
    ]:
        lines.append(f"{key}: {counts.get(key, 'n/a')}")

    lines.append("stage_statuses:")
    for stage_name, status in sorted((summary.get("stage_statuses") or {}).items()):
        lines.append(f"  {stage_name}: {status}")

    warnings = summary.get("warnings") or []
    lines.append("warnings:")
    if warnings:
        for warning in warnings:
            lines.append(f"  - {warning}")
    else:
        lines.append("  - none")

    lines.append("retry_counts:")
    for stage_name, retry_count in sorted((summary.get("retry_counts") or {}).items()):
        lines.append(f"  {stage_name}: {retry_count}")

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
    json_path.write_text(json_text, encoding="utf-8")
    txt_path.write_text(report_text + "\n", encoding="utf-8")
    latest_json_path.write_text(json_text, encoding="utf-8")
    latest_txt_path.write_text(report_text + "\n", encoding="utf-8")

    return {
        "json_path": str(json_path),
        "text_path": str(txt_path),
        "latest_json_path": str(latest_json_path),
        "latest_text_path": str(latest_txt_path),
    }


__all__ = [
    "build_pipeline_run_summary",
    "format_pipeline_terminal_report",
    "format_run_report",
    "write_pipeline_run_artifacts",
]
