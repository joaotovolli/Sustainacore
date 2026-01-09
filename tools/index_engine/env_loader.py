"""Environment loader for running SC_IDX tools manually (no external deps)."""
from __future__ import annotations

import os
from pathlib import Path


def load_env_file(path: str | os.PathLike) -> None:
    """
    Best-effort load KEY=VALUE lines into the current process environment.

    - Ignores blank lines and comments.
    - Supports optional leading 'export '.
    - Uses os.environ.setdefault(key, value) so explicit env vars win.
    """

    env_path = Path(path)
    try:
        raw_text = env_path.read_text(encoding="utf-8")
    except (FileNotFoundError, PermissionError):
        return
    except Exception:
        return

    for raw_line in raw_text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        name, sep, value = line.partition("=")
        if sep != "=":
            continue
        key = name.strip()
        if not key:
            continue
        cleaned = value.strip().strip("'").strip('"')
        os.environ.setdefault(key, cleaned)


def load_default_env() -> None:
    """Load known env files used by SC_IDX services (best-effort)."""

    load_env_file("/etc/sustainacore/db.env")
    load_env_file("/etc/sustainacore/index.env")
    load_env_file("/etc/sustainacore-ai/secrets.env")


__all__ = ["load_default_env", "load_env_file"]
