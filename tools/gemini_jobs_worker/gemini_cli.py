"""Worker-only Gemini CLI wrapper with quota guard."""
from __future__ import annotations

import logging
import os
import subprocess
from typing import Optional

from .learned_notes import LearnedNote, append_learned_note
from .quota_guard import QuotaGuard

LOGGER = logging.getLogger("gemini_jobs_worker.gemini_cli")

MODEL_NAME = "gemini-2.5-flash"
BINARY = os.getenv("GEMINI_BIN", "gemini")
WORKER_CWD = "/home/opc/Sustainacore/tools/gemini_jobs_worker"

_GUARD = QuotaGuard()


class GeminiCLIError(RuntimeError):
    def __init__(self, message: str, *, returncode: Optional[int] = None, stderr: str = "") -> None:
        super().__init__(message)
        self.returncode = returncode
        self.stderr = stderr


def run_gemini(prompt: str, *, timeout: float = 30.0) -> str:
    if not prompt:
        return ""

    _GUARD.before_call()

    cmd = [BINARY, "-m", MODEL_NAME, "-j", prompt]
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=WORKER_CWD,
            check=False,
        )
    except Exception as exc:
        append_learned_note(
            LearnedNote("Gemini CLI invocation failed", context=str(exc)[:200])
        )
        raise GeminiCLIError("gemini_cli_invoke_failed") from exc

    _GUARD.record_call()

    if proc.returncode != 0:
        append_learned_note(
            LearnedNote(
                "Gemini CLI returned non-zero",
                context=(proc.stderr or "").strip()[:200],
            )
        )
        raise GeminiCLIError("gemini_cli_nonzero", returncode=proc.returncode, stderr=proc.stderr or "")

    return proc.stdout or ""


def log_startup_config() -> None:
    LOGGER.info("gemini_cli_model=%s", MODEL_NAME)
    LOGGER.info("quota_guard thresholds=%s_per_minute %s_per_day", 50, 950)
