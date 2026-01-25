"""Email alerts for SC_IDX ingest."""
from __future__ import annotations

import logging
import os
import smtplib
from email.message import EmailMessage
from typing import Optional

_LOGGER = logging.getLogger("app.email")


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value


def _send_email_message(mail_to: str, subject: str, body: str, mail_from: Optional[str]) -> bool:
    host = _env("SMTP_HOST", "smtp.ionos.co.uk")
    port_raw = _env("SMTP_PORT", "587")
    timeout_raw = _env("SMTP_TIMEOUT_SEC", "10")
    try:
        port = int(port_raw) if port_raw is not None else 587
    except (TypeError, ValueError):
        port = 587
    try:
        timeout = int(timeout_raw) if timeout_raw is not None else 10
    except (TypeError, ValueError):
        timeout = 10

    user = _env("SMTP_USER")
    password = _env("SMTP_PASS")
    resolved_from = mail_from or _env("MAIL_FROM", user)

    missing = []
    if not host:
        missing.append("SMTP_HOST")
    if not user:
        missing.append("SMTP_USER")
    if not password:
        missing.append("SMTP_PASS")
    if not mail_to:
        missing.append("MAIL_TO")
    if not resolved_from:
        missing.append("MAIL_FROM")
    if missing:
        _LOGGER.warning("email_send skipped missing_env=%s", ",".join(missing))
        return False

    msg = EmailMessage()
    msg["From"] = resolved_from
    msg["To"] = mail_to
    msg["Subject"] = subject
    msg.set_content(body)

    try:
        with smtplib.SMTP(host, port, timeout=timeout) as client:
            client.ehlo()
            client.starttls()
            client.ehlo()
            client.login(user, password)
            client.send_message(msg)
            return True
    except Exception as exc:
        _LOGGER.warning(
            "email_send failed error_class=%s error=%s host=%s port=%s",
            type(exc).__name__,
            str(exc),
            host,
            port,
        )
        return False
    return False


def send_email(subject: str, body: str) -> bool:
    """Send an email using SMTP STARTTLS. No-op if required env vars are missing."""

    mail_to = _env("MAIL_TO")
    if not mail_to:
        return False
    return _send_email_message(mail_to, subject, body, mail_from=None)


def send_email_to(mail_to: str, subject: str, body: str, *, mail_from: Optional[str] = None) -> bool:
    """Send an email to the requested recipient using the configured SMTP settings."""

    if not mail_to:
        return False
    return _send_email_message(mail_to, subject, body, mail_from=mail_from)


__all__ = ["send_email", "send_email_to"]
