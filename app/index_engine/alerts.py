"""Email alerts for SC_IDX ingest."""
from __future__ import annotations

import os
import smtplib
from email.message import EmailMessage
from typing import Optional


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value


def send_email(subject: str, body: str) -> None:
    """Send an email using SMTP STARTTLS. No-op if required env vars are missing."""

    host = _env("SMTP_HOST", "smtp.ionos.co.uk")
    port_raw = _env("SMTP_PORT", "587")
    try:
        port = int(port_raw) if port_raw is not None else 587
    except (TypeError, ValueError):
        port = 587

    user = _env("SMTP_USER")
    password = _env("SMTP_PASS")
    mail_from = _env("MAIL_FROM", user)
    mail_to = _env("MAIL_TO")

    if not host or not user or not password or not mail_to or not mail_from:
        return

    msg = EmailMessage()
    msg["From"] = mail_from
    msg["To"] = mail_to
    msg["Subject"] = subject
    msg.set_content(body)

    try:
        with smtplib.SMTP(host, port, timeout=30) as client:
            client.ehlo()
            client.starttls()
            client.ehlo()
            client.login(user, password)
            client.send_message(msg)
    except Exception:
        # Swallow to avoid cascading failures; logging handled by caller.
        return


__all__ = ["send_email"]
