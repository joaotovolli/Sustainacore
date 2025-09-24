"""Lightweight wrapper around the Gemini CLI."""
from __future__ import annotations

import json
import os
import subprocess
from typing import Optional

_DEFAULT_TIMEOUT = float(os.getenv("RAG_GEMINI_TIMEOUT", "8"))
_DEFAULT_MODEL = os.getenv("GEMINI_MODEL", "gemini-1.5-pro")
_DEFAULT_BIN = os.getenv("GEMINI_BIN", "gemini")


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


def gemini_call(prompt: str, timeout: Optional[float] = None, model: Optional[str] = None) -> Optional[str]:
    """Invoke the Gemini CLI.

    Returns the textual response when successful, otherwise ``None`` so callers
    can fall back to deterministic messaging.
    """

    if not prompt:
        return None

    effective_timeout = timeout if timeout is not None else _DEFAULT_TIMEOUT
    effective_model = model or _DEFAULT_MODEL
    binary = os.getenv("GEMINI_BIN", _DEFAULT_BIN)

    cmd = [binary, "--model", effective_model, "--json_input", "-p", prompt]
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=effective_timeout,
            check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return None
    except Exception:
        return None

    if proc.returncode != 0:
        return None

    stdout = (proc.stdout or "").strip()
    if not stdout:
        return None

    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError:
        return stdout

    text = _extract_text(payload)
    return text if text else stdout
