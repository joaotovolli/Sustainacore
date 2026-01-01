"""Approval handling helpers."""
from __future__ import annotations

import datetime as dt
from typing import Optional

from .oracle import ApprovalRecord, append_decision_notes


def approval_is_applied(approval: ApprovalRecord) -> bool:
    notes = (approval.decision_notes or "").upper()
    return "APPLIED" in notes


def approval_is_final(approval: ApprovalRecord) -> bool:
    status = (approval.status or "").upper()
    return status in {"APPROVED", "REJECTED"}


def mark_applied(conn, approval: ApprovalRecord) -> None:
    timestamp = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%SZ")
    note = f"APPLIED {timestamp}"
    append_decision_notes(conn, approval.approval_id, note)
