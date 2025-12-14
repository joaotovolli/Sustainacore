"""Simple Twelve Data SMTP test sender (stdlib-only)."""
from __future__ import annotations

import datetime as _dt
import os
import smtplib
import socket
import sys
from email.message import EmailMessage
from typing import Tuple


def _env(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value


def _load_config() -> Tuple[str, int, str, str, str, str]:
    host = _env("SMTP_HOST", "smtp.ionos.co.uk")
    port_raw = _env("SMTP_PORT", "587")
    try:
        port = int(port_raw) if port_raw is not None else 587
    except (TypeError, ValueError):
        port = 587

    user = _env("SMTP_USER")
    password = _env("SMTP_PASS")
    mail_from = _env("MAIL_FROM", user or "")
    mail_to = _env("MAIL_TO", "joaotovolli@outlook.com")

    return host, port, user or "", password or "", mail_from or "", mail_to or ""


def main() -> int:
    host, port, user, password, mail_from, mail_to = _load_config()

    if not user or not password:
        print("ERROR: SMTP_USER and SMTP_PASS are required", file=sys.stderr)
        return 1

    if not mail_from:
        mail_from = user

    now = _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")
    hostname = socket.gethostname()

    msg = EmailMessage()
    msg["From"] = mail_from
    msg["To"] = mail_to
    msg["Subject"] = "SustainaCore VM1 SMTP test"
    msg.set_content(f"VM1 SMTP test from {hostname} at {now}")

    try:
        with smtplib.SMTP(host, port, timeout=30) as client:
            client.ehlo()
            client.starttls()
            client.ehlo()
            client.login(user, password)
            client.send_message(msg)
    except Exception as exc:  # pragma: no cover - network specific
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    print(f"OK: email sent to {mail_to}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
