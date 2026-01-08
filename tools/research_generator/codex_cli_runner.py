"""Codex CLI runner for research generator."""
from __future__ import annotations

import json
import logging
import os
import shlex
import subprocess
import tempfile
import time
from typing import Any, Dict, Optional, Union

from . import config

LOGGER = logging.getLogger("research_generator.codex_cli")


class CodexCLIError(RuntimeError):
    def __init__(self, message: str, *, exit_code: Optional[int] = None, output_tail: str = "") -> None:
        super().__init__(message)
        self.exit_code = exit_code
        self.output_tail = output_tail

    def __str__(self) -> str:
        parts = [self.args[0]]
        if self.exit_code is not None:
            parts.append(f"exit={self.exit_code}")
        if self.output_tail:
            parts.append(f"output_tail={self.output_tail}")
        return " ".join(parts)


def _strip_fences(text: str) -> str:
    stripped = text.strip()
    if stripped.startswith("```"):
        stripped = stripped.strip("`")
        if stripped.lower().startswith("json"):
            stripped = stripped[4:]
        stripped = stripped.strip()
    return stripped


def _parse_json(text: str) -> Dict[str, Any]:
    cleaned = _strip_fences(text)
    return json.loads(cleaned)


def _build_command() -> list[str]:
    base = shlex.split(config.CODEX_CMD)
    cmd = base + [
        "exec",
        "-m",
        config.CODEX_MODEL,
        "-C",
        os.path.abspath(os.path.join(config.WORKER_CWD, "..", "..")),
        "--output-last-message",
    ]
    return cmd


def call_codex(prompt: str, *, purpose: str, expect_json: bool = True) -> Union[Dict[str, Any], str]:
    attempts = max(config.CODEX_MAX_ATTEMPTS, 1)
    for attempt in range(attempts):
        with tempfile.NamedTemporaryFile(prefix="codex_last_", delete=False) as handle:
            output_path = handle.name
        cmd = _build_command() + [output_path]
        env = os.environ.copy()
        env.setdefault("HOME", "/home/opc")
        env.setdefault("USER", "opc")
        env.setdefault("PATH", "/usr/local/bin:/usr/bin:/bin")
        try:
            LOGGER.info("codex_call purpose=%s attempt=%s", purpose, attempt + 1)
            result = subprocess.run(
                cmd,
                input=prompt.encode("utf-8"),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=env,
                timeout=config.CODEX_TIMEOUT_SECONDS,
                check=False,
            )
            if result.returncode != 0:
                tail = (result.stderr or result.stdout).decode("utf-8", errors="ignore")[-400:]
                raise CodexCLIError("codex_cli_nonzero", exit_code=result.returncode, output_tail=tail)
            with open(output_path, "r", encoding="utf-8") as handle:
                content = handle.read()
            if expect_json:
                return _parse_json(content)
            return content.strip()
        except (CodexCLIError, json.JSONDecodeError) as exc:
            if attempt + 1 >= attempts:
                raise CodexCLIError(str(exc)) from exc
            time.sleep(1 + attempt * 2)
        except subprocess.TimeoutExpired as exc:
            if attempt + 1 >= attempts:
                raise CodexCLIError("codex_cli_timeout") from exc
            time.sleep(1 + attempt * 2)
        finally:
            try:
                os.unlink(output_path)
            except OSError:
                pass
    raise CodexCLIError("codex_cli_failed")


def log_startup_config() -> None:
    LOGGER.info("codex_cmd=%s", config.CODEX_CMD)
    LOGGER.info("codex_model=%s", config.CODEX_MODEL)
