"""Formatting helpers for SC_IDX run reports."""
from __future__ import annotations

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


__all__ = ["format_run_report"]
