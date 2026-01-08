"""Codex usage snapshot helpers."""
from __future__ import annotations

import json
import os
import re
import subprocess
from typing import Any, Dict, Optional, Tuple


_PCT_RE = re.compile(r"(\d+(?:\.\d+)?)%")


def _normalize_snapshot(raw: Dict[str, Any]) -> Dict[str, Any]:
    weekly = raw.get("weekly") or {}
    five_hour = raw.get("five_hour") or {}
    return {
        "available": True,
        "weekly": {
            "used_pct": weekly.get("used_pct"),
            "remaining_pct": weekly.get("remaining_pct"),
        },
        "five_hour": {
            "used_pct": five_hour.get("used_pct"),
            "remaining_pct": five_hour.get("remaining_pct"),
        },
        "breakdown": raw.get("breakdown") or {},
        "raw": raw,
    }


def _run_usage_command(cmd: list[str]) -> Tuple[Optional[str], Optional[str]]:
    try:
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            text=True,
            timeout=10,
            env={**os.environ, "HOME": "/home/opc", "USER": "opc"},
        )
    except (OSError, subprocess.TimeoutExpired):
        return None, None
    if result.returncode != 0:
        return None, result.stderr.strip()
    return result.stdout, None


def _parse_text_usage(text: str) -> Optional[Dict[str, Any]]:
    weekly_used = None
    five_used = None
    for line in text.splitlines():
        lower = line.lower()
        if "weekly" in lower:
            match = _PCT_RE.search(line)
            if match:
                weekly_used = float(match.group(1))
        if "5h" in lower or "5-hour" in lower or "five-hour" in lower:
            match = _PCT_RE.search(line)
            if match:
                five_used = float(match.group(1))
    if weekly_used is None and five_used is None:
        return None
    return {
        "weekly": {"used_pct": weekly_used, "remaining_pct": 100 - weekly_used if weekly_used is not None else None},
        "five_hour": {"used_pct": five_used, "remaining_pct": 100 - five_used if five_used is not None else None},
    }


def _parse_usage_file(path: str) -> Optional[Dict[str, Any]]:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return None
    usage = data.get("usage") or data.get("quota") or data.get("limits")
    if isinstance(usage, dict):
        return usage
    return None


def get_usage_snapshot() -> Dict[str, Any]:
    text, err = _run_usage_command(["codex", "usage", "--json"])
    if text:
        try:
            raw = json.loads(text)
            return _normalize_snapshot(raw)
        except json.JSONDecodeError:
            pass

    text, err = _run_usage_command(["codex", "usage"])
    if text:
        parsed = _parse_text_usage(text)
        if parsed:
            return _normalize_snapshot(parsed)

    for path in [
        "/home/opc/.codex/internal_storage.json",
        "/home/opc/.codex/usage.json",
        "/home/opc/.config/codex/usage.json",
    ]:
        if os.path.isfile(path):
            parsed = _parse_usage_file(path)
            if parsed:
                return _normalize_snapshot(parsed)

    return {"available": False, "reason": err or "usage_unavailable"}


def compute_usage_delta(before: Dict[str, Any], after: Dict[str, Any]) -> Dict[str, Any]:
    if not before.get("available") or not after.get("available"):
        return {"available": False, "reason": before.get("reason") or after.get("reason") or "usage_unavailable"}

    def delta(a: Optional[float], b: Optional[float]) -> Optional[float]:
        if a is None or b is None:
            return None
        return b - a

    weekly_before = before.get("weekly") or {}
    weekly_after = after.get("weekly") or {}
    five_before = before.get("five_hour") or {}
    five_after = after.get("five_hour") or {}
    breakdown_before = before.get("breakdown") or {}
    breakdown_after = after.get("breakdown") or {}
    breakdown_delta = {}
    for key in set(breakdown_before) | set(breakdown_after):
        breakdown_delta[key] = delta(breakdown_before.get(key), breakdown_after.get(key))
    return {
        "available": True,
        "weekly_used_pct_delta": delta(weekly_before.get("used_pct"), weekly_after.get("used_pct")),
        "five_hour_used_pct_delta": delta(five_before.get("used_pct"), five_after.get("used_pct")),
        "breakdown_delta": breakdown_delta,
    }
