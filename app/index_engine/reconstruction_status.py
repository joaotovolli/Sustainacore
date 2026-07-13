"""Atomic, secrets-free status reporting for controlled SC_IDX reconstruction."""

from __future__ import annotations

import datetime as dt
import json
import os
from pathlib import Path
from typing import Any

ALLOWED_STATUSES = {
    "PENDING",
    "RUNNING",
    "FAILED",
    "COMPENSATING",
    "ROLLED_BACK",
    "VERIFYING",
    "SUCCEEDED",
}


def write_reconstruction_status(path: Path, **values: Any) -> None:
    status = str(values.get("status") or "")
    if status not in ALLOWED_STATUSES:
        raise ValueError(f"invalid_reconstruction_status:{status}")
    payload = {
        "run_id": values.get("run_id"),
        "backup_tag": values.get("backup_tag"),
        "revision": values.get("revision"),
        "repair_start": values.get("repair_start"),
        "repair_end": values.get("repair_end"),
        "stage": values.get("stage"),
        "stage_started_at": values.get("stage_started_at"),
        "last_completed_date": values.get("last_completed_date"),
        "rows_processed": int(values.get("rows_processed") or 0),
        "model_code": values.get("model_code"),
        "completed_model_count": int(values.get("completed_model_count") or 0),
        "analytics_rows_committed": int(values.get("analytics_rows_committed") or 0),
        "position_rows_committed": int(values.get("position_rows_committed") or 0),
        "optimizer_rows_committed": int(values.get("optimizer_rows_committed") or 0),
        "status": status,
        "failure_class": values.get("failure_class"),
        "rollback_status": values.get("rollback_status"),
        "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, sort_keys=True, separators=(",", ":"))
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def read_reconstruction_status(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


__all__ = ["ALLOWED_STATUSES", "read_reconstruction_status", "write_reconstruction_status"]
