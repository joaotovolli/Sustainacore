"""Append-only learned notes for worker instructions."""
from __future__ import annotations

import datetime as dt
import hashlib
import logging
import os
from dataclasses import dataclass
from typing import Optional

LOGGER = logging.getLogger("gemini_jobs_worker.learned_notes")

DEFAULT_NOTES_PATH = "/home/opc/Sustainacore/tools/gemini_jobs_worker/GEMINI.md"
SECTION_HEADER = "## Learned Notes"


@dataclass
class LearnedNote:
    message: str
    context: Optional[str] = None

    def marker(self) -> str:
        base = f"{self.message}\n{self.context or ''}".strip()
        return hashlib.sha1(base.encode("utf-8", errors="ignore")).hexdigest()[:12]

    def render(self) -> str:
        ts = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%SZ")
        context = f" | context={self.context}" if self.context else ""
        return f"- [{ts}] {self.message}{context} [note-id:{self.marker()}]"


def _ensure_section(content: str) -> str:
    if SECTION_HEADER in content:
        return content
    return content.rstrip() + "\n\n" + SECTION_HEADER + "\n"


def append_learned_note(note: LearnedNote, *, notes_path: Optional[str] = None) -> bool:
    path = notes_path or DEFAULT_NOTES_PATH
    if not os.path.isfile(path):
        LOGGER.warning("learned_notes_missing_file: %s", path)
        return False
    try:
        with open(path, "r", encoding="utf-8") as handle:
            content = handle.read()
    except OSError as exc:
        LOGGER.warning("learned_notes_read_failed: %s", exc)
        return False

    content = _ensure_section(content)
    if f"[note-id:{note.marker()}]" in content:
        return False

    updated = content.rstrip() + "\n" + note.render() + "\n"
    try:
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(updated)
    except OSError as exc:
        LOGGER.warning("learned_notes_write_failed: %s", exc)
        return False
    return True
