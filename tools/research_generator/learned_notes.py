"""Append-only learned notes for research generator failures."""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
from typing import Any, Dict

from . import config

NOTES_PATH = os.path.join(config.WORKER_CWD, "LEARNED_NOTES.json")


def _load_notes() -> Dict[str, Any]:
    if not os.path.exists(NOTES_PATH):
        return {"notes": []}
    with open(NOTES_PATH, "r", encoding="utf-8") as handle:
        try:
            return json.load(handle)
        except json.JSONDecodeError:
            return {"notes": []}


def append_note(*, failure_type: str, fix_hint: str, report_type: str) -> None:
    payload = _load_notes()
    notes = payload.get("notes", [])
    stamp = dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    base = f"{failure_type}|{fix_hint}|{report_type}"
    note_id = hashlib.sha1(base.encode("utf-8")).hexdigest()[:12]
    for note in notes:
        if note.get("note_id") == note_id:
            return
    notes.append(
        {
            "note_id": note_id,
            "timestamp": stamp,
            "failure_type": failure_type,
            "fix_hint": fix_hint,
            "report_type": report_type,
        }
    )
    payload["notes"] = notes
    with open(NOTES_PATH, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
