"""Gemini CLI wrapper for research generator."""
from __future__ import annotations

import json
import logging
import subprocess
import time
from typing import Optional

from . import config
from .quota_guard import QuotaGuard

LOGGER = logging.getLogger("research_generator.gemini_cli")

_GUARD = QuotaGuard()


def _tail(text: str, limit: int = 400) -> str:
    if not text:
        return ""
    payload = text.strip()
    if len(payload) <= limit:
        return payload
    return payload[-limit:]


class GeminiCLIError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        returncode: Optional[int] = None,
        stderr: str = "",
        stdout: str = "",
    ) -> None:
        super().__init__(message)
        self.returncode = returncode
        self.stderr = stderr
        self.stdout = stdout

    def __str__(self) -> str:
        parts = [self.args[0]]
        if self.returncode is not None:
            parts.append(f"exit={self.returncode}")
        if self.stderr:
            parts.append(f"stderr_tail={_tail(self.stderr)}")
        if self.stdout:
            parts.append(f"stdout_tail={_tail(self.stdout)}")
        return " ".join(parts)


def run_gemini(prompt: str, *, timeout: float = 60.0) -> str:
    if not prompt:
        return ""

    cmd = [config.GEMINI_BIN, "-m", config.MODEL_NAME, "-j", prompt]
    LOGGER.info("gemini_cmd=%s -m %s -j <prompt>", config.GEMINI_BIN, config.MODEL_NAME)

    attempts = 2
    for attempt in range(1, attempts + 1):
        _GUARD.before_call()
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

        if proc.returncode == 0:
            stdout = (proc.stdout or "").strip()
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

        stderr = proc.stderr or ""
        stdout = proc.stdout or ""
        if "429" in stderr or "429" in stdout or "rate limit" in stderr.lower():
            if attempt < attempts:
                LOGGER.warning("gemini_cli_rate_limited retrying attempt=%s", attempt + 1)
                time.sleep(config.SLEEP_ON_MINUTE_LIMIT)
                continue
        raise GeminiCLIError(
            "gemini_cli_nonzero",
            returncode=proc.returncode,
            stderr=stderr,
            stdout=stdout,
        )

    return ""


def is_quota_near_limit() -> bool:
    return _GUARD.near_limit()


def log_startup_config() -> None:
    LOGGER.info("gemini_cli_model=%s", config.MODEL_NAME)
    LOGGER.info(
        "quota_guard thresholds=%s_per_minute %s_per_day",
        config.MAX_CALLS_PER_MINUTE,
        config.MAX_CALLS_PER_DAY,
    )
