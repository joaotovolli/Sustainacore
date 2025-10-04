"""Lightweight wrapper around the Gemini CLI."""
from __future__ import annotations

import json
import os
import subprocess
import threading
from typing import Optional

_DEFAULT_TIMEOUT = float(os.getenv("RAG_GEMINI_TIMEOUT", "8"))
_DEFAULT_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
_DEFAULT_BIN = os.getenv("GEMINI_BIN", "gemini")
_DETECT_TIMEOUT = float(os.getenv("RAG_GEMINI_DETECT_TIMEOUT", "4"))


class GeminiCLIError(Exception):
    """Container for Gemini CLI failures so callers can log rich detail."""

    def __init__(self, message: str, *, returncode: Optional[int] = None, stderr: str = "") -> None:
        super().__init__(message)
        self.returncode = returncode
        self.stderr = stderr or ""

    @property
    def first_line(self) -> str:
        for line in self.stderr.splitlines():
            candidate = line.strip()
            if candidate:
                return candidate
        return ""


def _extract_text(payload: object) -> Optional[str]:
    """Return the best-effort text field from a Gemini JSON response."""
    if isinstance(payload, str):
        text = payload.strip()
        return text or None
    if isinstance(payload, list):
        for item in payload:
            text = _extract_text(item)
            if text:
                return text
        return None
    if isinstance(payload, dict):
        for key in ("text", "output", "answer", "content"):
            value = payload.get(key)
            text = _extract_text(value)
            if text:
                return text
        return None
    return None


_FLAG_LOCK = threading.Lock()
_FLAG_SUPPORTS_LONG: Optional[bool] = None
_CLI_AVAILABLE: Optional[bool] = None
_LAST_ERROR: Optional[GeminiCLIError] = None


def detect_gemini_flags() -> bool:
    """Probe the installed CLI once to determine whether long flags are supported."""

    global _FLAG_SUPPORTS_LONG, _CLI_AVAILABLE

    with _FLAG_LOCK:
        if _FLAG_SUPPORTS_LONG is not None:
            return _FLAG_SUPPORTS_LONG

        binary = os.getenv("GEMINI_BIN", _DEFAULT_BIN)

        try:
            subprocess.run(
                [binary, "--version"],
                capture_output=True,
                text=True,
                timeout=_DETECT_TIMEOUT,
                check=False,
            )
        except (FileNotFoundError, subprocess.SubprocessError, OSError):
            _CLI_AVAILABLE = False
            _FLAG_SUPPORTS_LONG = False
            return _FLAG_SUPPORTS_LONG

        _CLI_AVAILABLE = True

        try:
            help_proc = subprocess.run(
                [binary, "--help"],
                capture_output=True,
                text=True,
                timeout=_DETECT_TIMEOUT,
                check=False,
            )
        except (FileNotFoundError, subprocess.SubprocessError, OSError):
            _FLAG_SUPPORTS_LONG = False
            return _FLAG_SUPPORTS_LONG

        output = ((help_proc.stdout or "") + "\n" + (help_proc.stderr or "")).lower()
        _FLAG_SUPPORTS_LONG = "--model" in output and "--json_input" in output
        return _FLAG_SUPPORTS_LONG


def gemini_cli_available() -> bool:
    """Return True when the CLI appears available."""

    detect_gemini_flags()
    return bool(_CLI_AVAILABLE)


def get_last_error() -> Optional[GeminiCLIError]:
    """Expose the most recent CLI error for observability."""

    return _LAST_ERROR


def gemini_call(prompt: str, timeout: Optional[float] = None, model: Optional[str] = None) -> Optional[str]:
    """Invoke the Gemini CLI.

    Returns the textual response when successful, otherwise ``None`` so callers
    can fall back to deterministic messaging.
    """

    global _LAST_ERROR

    if not prompt:
        return None

    _LAST_ERROR = None

    effective_timeout = timeout if timeout is not None else _DEFAULT_TIMEOUT
    effective_model = model or _DEFAULT_MODEL
    binary = os.getenv("GEMINI_BIN", _DEFAULT_BIN)

    supports_long = detect_gemini_flags()

    if _CLI_AVAILABLE is False:
        _LAST_ERROR = GeminiCLIError("gemini_cli_unavailable")
        return None

    if supports_long:
        cmd = [binary, "--model", effective_model, "--json_input", "-p", prompt]
    else:
        cmd = [binary, "-m", effective_model, "-j", "-p", prompt]

    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=effective_timeout,
            check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired) as exc:
        _LAST_ERROR = GeminiCLIError("gemini_cli_unavailable", stderr=str(exc))
        return None
    except Exception as exc:  # pragma: no cover - defensive catch
        _LAST_ERROR = GeminiCLIError("gemini_cli_error", stderr=str(exc))
        return None

    if proc.returncode != 0:
        _LAST_ERROR = GeminiCLIError(
            "gemini_cli_exit",
            returncode=proc.returncode,
            stderr=proc.stderr or proc.stdout or "",
        )
        return None

    stdout = (proc.stdout or "").strip()
    if not stdout:
        _LAST_ERROR = GeminiCLIError("gemini_cli_empty", stderr=proc.stderr or "")
        return None

    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError:
        _LAST_ERROR = None
        return stdout

    text = _extract_text(payload)
    if text:
        _LAST_ERROR = None
        return text

    _LAST_ERROR = None
    return stdout


__all__ = [
    "GeminiCLIError",
    "detect_gemini_flags",
    "gemini_call",
    "gemini_cli_available",
    "get_last_error",
]
