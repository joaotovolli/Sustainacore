"""Gemini CLI wrapper for research generator."""
from __future__ import annotations

import json
import logging
import subprocess
from typing import Optional

from . import config
from .quota_guard import QuotaGuard

LOGGER = logging.getLogger("research_generator.gemini_cli")

_GUARD = QuotaGuard()


class GeminiCLIError(RuntimeError):
    def __init__(self, message: str, *, returncode: Optional[int] = None, stderr: str = "") -> None:
        super().__init__(message)
        self.returncode = returncode
        self.stderr = stderr


def run_gemini(prompt: str, *, timeout: float = 60.0) -> str:
    if not prompt:
        return ""

    _GUARD.before_call()

    cmd = [config.GEMINI_BIN, "-m", config.MODEL_NAME, "-j", prompt]
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=config.WORKER_CWD,
            check=False,
        )
    except Exception as exc:
        raise GeminiCLIError("gemini_cli_invoke_failed") from exc

    _GUARD.record_call()

    if proc.returncode != 0:
        raise GeminiCLIError("gemini_cli_nonzero", returncode=proc.returncode, stderr=proc.stderr or "")

    stdout = proc.stdout or ""
    stdout = stdout.strip()
    if stdout.startswith("{"):
        try:
            payload = json.loads(stdout)
        except json.JSONDecodeError:
            return stdout
        candidates = payload.get("candidates") or []
        if candidates:
            parts = ((candidates[0].get("content") or {}).get("parts") or [])
            if parts and isinstance(parts[0], dict):
                return parts[0].get("text") or ""
    return stdout


def log_startup_config() -> None:
    LOGGER.info("gemini_cli_model=%s", config.MODEL_NAME)
    LOGGER.info(
        "quota_guard thresholds=%s_per_minute %s_per_day",
        config.MAX_CALLS_PER_MINUTE,
        config.MAX_CALLS_PER_DAY,
    )
