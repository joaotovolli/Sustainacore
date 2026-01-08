"""Codex usage snapshot helpers."""
from __future__ import annotations

import json
import os
import re
import shlex
import shutil
import subprocess
import time
from typing import Any, Dict, Optional, Tuple


_PCT_RE = re.compile(r"(\d+(?:\.\d+)?)%")
_CTX_RE = re.compile(r"Context window:.*?(\d+)%\s+left", re.IGNORECASE)
_CTX_ALT_RE = re.compile(r"(\d+)%\s+context\s+left", re.IGNORECASE)
_FIVE_RE = re.compile(r"5h limit:.*?(\d+)%\s+left", re.IGNORECASE)
_WEEK_RE = re.compile(r"Weekly limit:.*?(\d+)%\s+left", re.IGNORECASE)


def _normalize_snapshot(raw: Dict[str, Any], *, source: str) -> Dict[str, Any]:
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
        "context_window": raw.get("context_window") or {},
        "breakdown": raw.get("breakdown") or {},
        "raw": raw,
        "source": source,
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


def _parse_status_text(text: str) -> Optional[Dict[str, Any]]:
    ctx = _CTX_RE.search(text)
    if not ctx:
        ctx = _CTX_ALT_RE.search(text)
    five = _FIVE_RE.search(text)
    week = _WEEK_RE.search(text)
    if not (ctx or five or week):
        return None
    if not five or not week:
        return None
    context_left = int(ctx.group(1)) if ctx else None
    five_left = int(five.group(1)) if five else None
    week_left = int(week.group(1)) if week else None
    payload: Dict[str, Any] = {
        "context_window": {"left_pct": context_left},
        "five_hour": {
            "used_pct": 100 - five_left if five_left is not None else None,
            "remaining_pct": five_left,
        },
        "weekly": {
            "used_pct": 100 - week_left if week_left is not None else None,
            "remaining_pct": week_left,
        },
    }
    return payload


def _resolve_codex_cmd() -> Tuple[Optional[list[str]], Optional[str]]:
    if shutil.which("codex"):
        return ["codex"], None
    parts = shlex.split(os.getenv("RESEARCH_CODEX_CMD", "npx @openai/codex@latest"))
    if parts and parts[0] == "npx" and shutil.which("npx"):
        return parts, None
    return None, "codex_not_found_in_path"


def _probe_status_via_pty(timeout_s: int = 12) -> Dict[str, Any]:
    cmd, reason = _resolve_codex_cmd()
    if not cmd:
        return {"available": False, "reason": reason or "codex_not_found_in_path", "source": "codex_status_pty"}

    try:
        import pexpect  # type: ignore
    except Exception:
        pexpect = None

    env = {**os.environ, "HOME": "/home/opc", "USER": "opc"}
    env.setdefault("TERM", "xterm-256color")
    if pexpect:
        try:
            child = pexpect.spawn(cmd[0], cmd[1:], env=env, encoding="utf-8", timeout=timeout_s)
            buffer = ""
            sent_status = False
            start = time.time()
            while time.time() - start < timeout_s:
                try:
                    chunk = child.read_nonblocking(size=4096, timeout=1)
                except pexpect.TIMEOUT:
                    chunk = ""
                except pexpect.EOF:
                    break
                buffer += chunk
                if "\x1b[6n" in buffer:
                    child.send("\x1b[1;1R")
                    buffer = buffer.replace("\x1b[6n", "")
                if (">" in buffer or "Type" in buffer or "Codex" in buffer) and not sent_status:
                    child.sendline("/status")
                    sent_status = True
                if "Weekly limit" in buffer and "5h limit" in buffer:
                    break
            child.sendline("/exit")
            child.close()
            parsed = _parse_status_text(buffer)
            if parsed:
                return _normalize_snapshot(parsed, source="codex_status_pty")
            tail = buffer[-200:] if buffer else ""
            return {
                "available": False,
                "reason": f"status_parse_failed:{tail}",
                "source": "codex_status_pty",
            }
        except Exception as exc:
            return {"available": False, "reason": f"pty_error:{exc}", "source": "codex_status_pty"}

    try:
        import pty
        import select

        master_fd, slave_fd = pty.openpty()
        proc = subprocess.Popen(
            cmd,
            stdin=slave_fd,
            stdout=slave_fd,
            stderr=slave_fd,
            env=env,
            text=False,
        )
        os.write(master_fd, b"/status\n")
        buffer = b""
        start = time.time()
        while time.time() - start < timeout_s:
            rlist, _, _ = select.select([master_fd], [], [], 1.0)
            if master_fd in rlist:
                chunk = os.read(master_fd, 4096)
                if not chunk:
                    break
                buffer += chunk
                if b"\x1b[6n" in buffer:
                    os.write(master_fd, b"\x1b[1;1R")
                if b"Weekly limit" in buffer and b"5h limit" in buffer:
                    break
        os.write(master_fd, b"/exit\n")
        proc.terminate()
        parsed = _parse_status_text(buffer.decode("utf-8", errors="ignore"))
        if parsed:
            return _normalize_snapshot(parsed, source="codex_status_pty")
        return {"available": False, "reason": "status_parse_failed", "source": "codex_status_pty"}
    except Exception as exc:
        return {"available": False, "reason": f"pty_error:{exc}", "source": "codex_status_pty"}


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
    err = None
    if shutil.which("codex"):
        text, err = _run_usage_command(["codex", "usage", "--json"])
        if text:
            try:
                raw = json.loads(text)
                return _normalize_snapshot(raw, source="codex_usage_json")
            except json.JSONDecodeError:
                pass

        text, err = _run_usage_command(["codex", "usage"])
        if text:
            parsed = _parse_text_usage(text)
            if parsed:
                return _normalize_snapshot(parsed, source="codex_usage_text")

    status = _probe_status_via_pty()
    if status.get("available"):
        return status

    for path in [
        "/home/opc/.codex/internal_storage.json",
        "/home/opc/.codex/usage.json",
        "/home/opc/.config/codex/usage.json",
    ]:
        if os.path.isfile(path):
            parsed = _parse_usage_file(path)
            if parsed:
                return _normalize_snapshot(parsed, source="codex_usage_file")

    return {"available": False, "reason": status.get("reason") or err or "usage_unavailable", "source": "codex_status_pty"}


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
