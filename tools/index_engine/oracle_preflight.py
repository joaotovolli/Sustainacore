"""Oracle connection preflight + wallet diagnostics for SC_IDX tools."""
from __future__ import annotations

import os
import re
from pathlib import Path

from db_helper import get_connection


def probe_oracle_user() -> str:
    """Return the Oracle USER via a lightweight select."""

    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT USER FROM dual")
        row = cur.fetchone()
        return str(row[0]).strip() if row and row[0] is not None else ""


_DIRECTORY_RE = re.compile(r"DIRECTORY\\s*=\\s*\"?([^\\)\"\\s]+)\"?", re.IGNORECASE)


def _parse_wallet_dir_from_sqlnet(sqlnet_text: str) -> Path | None:
    match = _DIRECTORY_RE.search(sqlnet_text)
    if not match:
        return None
    raw = match.group(1).strip()
    if not raw:
        return None
    return Path(raw)


def collect_wallet_diagnostics() -> dict[str, object]:
    """Return best-effort wallet diagnostics for ORA-28759 style issues."""

    tns_admin = os.getenv("TNS_ADMIN") or "/opt/adb_wallet"
    tns_path = Path(tns_admin)
    sqlnet_path = tns_path / "sqlnet.ora"

    sqlnet_exists = sqlnet_path.exists()
    wallet_dir: Path | None = None
    sqlnet_preview: str | None = None
    if sqlnet_exists:
        try:
            sqlnet_text = sqlnet_path.read_text(encoding="utf-8", errors="ignore")
            sqlnet_preview = "\n".join(sqlnet_text.splitlines()[:25])
            wallet_dir = _parse_wallet_dir_from_sqlnet(sqlnet_text)
        except Exception:
            sqlnet_preview = None
            wallet_dir = None

    if wallet_dir is None and tns_path.exists():
        wallet_dir = tns_path

    cwallet_path = wallet_dir / "cwallet.sso" if wallet_dir else None
    ewallet_path = wallet_dir / "ewallet.p12" if wallet_dir else None

    return {
        "tns_admin": str(tns_admin),
        "tns_admin_exists": tns_path.exists(),
        "sqlnet_path": str(sqlnet_path),
        "sqlnet_exists": sqlnet_exists,
        "wallet_dir": str(wallet_dir) if wallet_dir else None,
        "cwallet_sso_exists": bool(cwallet_path and cwallet_path.exists()),
        "ewallet_p12_exists": bool(ewallet_path and ewallet_path.exists()),
        "sqlnet_preview": sqlnet_preview,
    }


def format_wallet_diagnostics(diagnostics: dict[str, object]) -> str:
    """Return a stable, human-readable diagnostic string (no secrets)."""

    lines: list[str] = []
    lines.append(f"TNS_ADMIN={diagnostics.get('tns_admin')}")
    lines.append(f"tns_admin_exists={diagnostics.get('tns_admin_exists')}")
    lines.append(f"sqlnet_path={diagnostics.get('sqlnet_path')}")
    lines.append(f"sqlnet_exists={diagnostics.get('sqlnet_exists')}")
    lines.append(f"wallet_dir={diagnostics.get('wallet_dir')}")
    lines.append(f"cwallet_sso_exists={diagnostics.get('cwallet_sso_exists')}")
    lines.append(f"ewallet_p12_exists={diagnostics.get('ewallet_p12_exists')}")

    preview = diagnostics.get("sqlnet_preview")
    if isinstance(preview, str) and preview.strip():
        lines.append("sqlnet_preview_begin")
        lines.append(preview)
        lines.append("sqlnet_preview_end")
    return "\n".join(lines)


__all__ = [
    "collect_wallet_diagnostics",
    "format_wallet_diagnostics",
    "probe_oracle_user",
]
