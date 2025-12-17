"""Environment bootstrap helpers for Oracle connectivity on VM1."""

from __future__ import annotations

import os
from typing import Iterable, Mapping


DEFAULT_ENV_FILES: tuple[str, ...] = (
    "/etc/sustainacore/db.env",
    "/etc/sustainacore-ai/app.env",
)


def _strip_quotes(value: str) -> str:
    text = value.strip()
    if not text:
        return text
    return text.strip("\"'")


def _should_set_env(key: str, *, override: bool) -> bool:
    if override:
        return True
    existing = os.environ.get(key)
    if existing is None:
        return True
    return str(existing).strip() == ""


def load_env_files(paths: Iterable[str] = DEFAULT_ENV_FILES, *, override: bool = False) -> Mapping[str, bool]:
    """Load key=value pairs from the given env files into ``os.environ``.

    Lines are parsed as simple ``KEY=VALUE`` entries. Blank lines, comments, and
    malformed entries are ignored. Values are stripped of surrounding quotes and
    whitespace before being applied. Existing environment variables are left
    unchanged unless ``override`` is True or the current value is empty.

    Returns a dict of booleans keyed by every environment variable encountered
    (after applying the override rules); values indicate whether the variable is
    present in ``os.environ`` after processing.
    """

    seen: dict[str, bool] = {}
    for path in paths:
        if not path:
            continue
        if not os.path.isfile(path):
            continue
        try:
            with open(path, "r", encoding="utf-8") as handle:
                for raw_line in handle:
                    line = raw_line.strip()
                    if not line or line.startswith("#"):
                        continue
                    name, sep, value = line.partition("=")
                    if sep != "=" or not name.strip():
                        continue
                    key = name.strip()
                    val = _strip_quotes(value)
                    if _should_set_env(key, override=override):
                        os.environ[key] = val
                    seen[key] = key in os.environ and str(os.environ.get(key) or "").strip() != ""
        except (OSError, UnicodeDecodeError):
            continue
    return seen


def required_keys_present() -> dict[str, bool]:
    """Return presence booleans for required Oracle connectivity envs."""

    password_present = any(
        bool(os.environ.get(key)) for key in ("DB_PASSWORD", "DB_PASS", "DB_PWD")
    )
    return {
        "DB_USER": bool(os.environ.get("DB_USER")),
        "DB_DSN": bool(os.environ.get("DB_DSN")),
        "TNS_ADMIN": bool(os.environ.get("TNS_ADMIN")),
        "DB_PASSWORD|DB_PASS|DB_PWD": password_present,
        "WALLET_PWD": bool(os.environ.get("WALLET_PWD")),
    }


__all__ = ["DEFAULT_ENV_FILES", "load_env_files", "required_keys_present"]
