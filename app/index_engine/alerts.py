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


def _send_email_message(mail_to: str, subject: str, body: str, mail_from: Optional[str]) -> None:
    host = _env("SMTP_HOST", "smtp.ionos.co.uk")
    port_raw = _env("SMTP_PORT", "587")
    try:
        port = int(port_raw) if port_raw is not None else 587
    except (TypeError, ValueError):
        port = 587

    user = _env("SMTP_USER")
    password = _env("SMTP_PASS")
    resolved_from = mail_from or _env("MAIL_FROM", user)

    if not host or not user or not password or not mail_to or not resolved_from:
        return

    msg = EmailMessage()
    msg["From"] = resolved_from
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


def send_email(subject: str, body: str) -> None:
    """Send an email using SMTP STARTTLS. No-op if required env vars are missing."""

    mail_to = _env("MAIL_TO")
    if not mail_to:
        return
    _send_email_message(mail_to, subject, body, mail_from=None)


def send_email_to(mail_to: str, subject: str, body: str, *, mail_from: Optional[str] = None) -> None:
    """Send an email to the requested recipient using the configured SMTP settings."""

    if not mail_to:
        return
    _send_email_message(mail_to, subject, body, mail_from=mail_from)


__all__ = ["send_email", "send_email_to"]
