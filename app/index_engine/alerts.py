"""Email alerts for SC_IDX ingest."""
from __future__ import annotations

import logging
import os
import smtplib
import time
from email.message import EmailMessage
from uuid import uuid4
from typing import Optional

_LOGGER = logging.getLogger("app.email")

_CIRCUIT_OPEN_UNTIL: float = 0.0
_CONSECUTIVE_FAILURES: int = 0
_ENV_LOGGED: bool = False


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value


def _log_env_once() -> None:
    global _ENV_LOGGED
    if _ENV_LOGGED:
        return
    _ENV_LOGGED = True
    required = ["SMTP_HOST", "SMTP_PORT", "SMTP_USER", "SMTP_PASS", "MAIL_FROM"]
    status = {key: bool(_env(key)) for key in required}
    _LOGGER.info("smtp_env_status %s", status)


def _circuit_open() -> bool:
    return time.time() < _CIRCUIT_OPEN_UNTIL


def _record_failure() -> None:
    global _CONSECUTIVE_FAILURES, _CIRCUIT_OPEN_UNTIL
    _CONSECUTIVE_FAILURES += 1
    max_fails = _env("SMTP_CIRCUIT_BREAKER_FAILS", "3")
    window_sec = _env("SMTP_CIRCUIT_BREAKER_SEC", "30")
    try:
        max_fails_int = int(max_fails)
    except (TypeError, ValueError):
        max_fails_int = 3
    try:
        window_int = int(window_sec)
    except (TypeError, ValueError):
        window_int = 30
    if _CONSECUTIVE_FAILURES >= max_fails_int:
        _CIRCUIT_OPEN_UNTIL = time.time() + max(1, window_int)


def _record_success() -> None:
    global _CONSECUTIVE_FAILURES, _CIRCUIT_OPEN_UNTIL
    _CONSECUTIVE_FAILURES = 0
    _CIRCUIT_OPEN_UNTIL = 0.0


def _send_email_message(
    mail_to: str,
    subject: str,
    body: str,
    mail_from: Optional[str],
    *,
    timeout_override: Optional[float] = None,
    retry_attempts_override: Optional[int] = None,
    retry_base_override: Optional[float] = None,
    html_body: Optional[str] = None,
) -> bool:
    _log_env_once()
    if _circuit_open():
        _LOGGER.warning("email_send skipped circuit_open=1")
        return False
    host = _env("SMTP_HOST", "smtp.ionos.co.uk")
    port_raw = _env("SMTP_PORT", "587")
    timeout_raw = timeout_override if timeout_override is not None else _env("SMTP_TIMEOUT_SEC", "5")
    retry_raw = (
        retry_attempts_override if retry_attempts_override is not None else _env("SMTP_RETRY_ATTEMPTS", "2")
    )
    retry_base_raw = retry_base_override if retry_base_override is not None else _env("SMTP_RETRY_BASE_SEC", "0.5")
    try:
        port = int(port_raw) if port_raw is not None else 587
    except (TypeError, ValueError):
        port = 587
    try:
        timeout = int(timeout_raw) if timeout_raw is not None else 10
    except (TypeError, ValueError):
        timeout = 5
    try:
        retries = int(retry_raw) if retry_raw is not None else 2
    except (TypeError, ValueError):
        retries = 2
    try:
        retry_base = float(retry_base_raw) if retry_base_raw is not None else 0.5
    except (TypeError, ValueError):
        retry_base = 0.5

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
    if "Message-ID" not in msg:
        msg["Message-ID"] = f"<sc-{uuid4().hex}@sustainacore.org>"
    msg.set_content(body)
    if html_body:
        msg.add_alternative(html_body, subtype="html")

    attempts = max(0, retries) + 1
    for attempt in range(1, attempts + 1):
        try:
            with smtplib.SMTP(host, port, timeout=timeout) as client:
                client.ehlo()
                client.starttls()
                client.ehlo()
                client.login(user, password)
                refused = client.send_message(msg)
                if refused:
                    _record_failure()
                    _LOGGER.warning(
                        "email_send failed error_class=SMTPRecipientsRefused host=%s port=%s refused=%s",
                        host,
                        port,
                        len(refused),
                    )
                    return False
                _record_success()
                to_count = len([part for part in str(msg.get("To", "")).split(",") if part.strip()])
                _LOGGER.info(
                    "email_send ok message_id=%s host=%s port=%s to_count=%s",
                    msg.get("Message-ID"),
                    host,
                    port,
                    to_count or 0,
                )
                return True
        except smtplib.SMTPAuthenticationError as exc:
            _record_failure()
            _LOGGER.warning(
                "email_send failed error_class=%s error=%s host=%s port=%s attempt=%s",
                type(exc).__name__,
                str(exc),
                host,
                port,
                attempt,
            )
            return False
        except smtplib.SMTPRecipientsRefused as exc:
            _record_failure()
            _LOGGER.warning(
                "email_send failed error_class=%s error=%s host=%s port=%s attempt=%s",
                type(exc).__name__,
                str(exc),
                host,
                port,
                attempt,
            )
            return False
        except Exception as exc:
            _record_failure()
            _LOGGER.warning(
                "email_send failed error_class=%s error=%s host=%s port=%s attempt=%s",
                type(exc).__name__,
                str(exc),
                host,
                port,
                attempt,
            )
            if attempt < attempts:
                time.sleep(max(0.0, retry_base * (2 ** (attempt - 1))))
                continue
            return False
    return False


def send_email(
    subject: str,
    body: str,
    *,
    timeout_sec: Optional[float] = None,
    retry_attempts: Optional[int] = None,
    retry_base_sec: Optional[float] = None,
    html_body: Optional[str] = None,
) -> bool:
    """Send an email using SMTP STARTTLS. No-op if required env vars are missing."""

    mail_to = _env("MAIL_TO")
    if not mail_to:
        return False
    return _send_email_message(
        mail_to,
        subject,
        body,
        mail_from=None,
        timeout_override=timeout_sec,
        retry_attempts_override=retry_attempts,
        retry_base_override=retry_base_sec,
        html_body=html_body,
    )


def send_email_to(
    mail_to: str,
    subject: str,
    body: str,
    *,
    mail_from: Optional[str] = None,
    timeout_sec: Optional[float] = None,
    retry_attempts: Optional[int] = None,
    retry_base_sec: Optional[float] = None,
    html_body: Optional[str] = None,
) -> bool:
    """Send an email to the requested recipient using the configured SMTP settings."""

    if not mail_to:
        return False
    return _send_email_message(
        mail_to,
        subject,
        body,
        mail_from=mail_from,
        timeout_override=timeout_sec,
        retry_attempts_override=retry_attempts,
        retry_base_override=retry_base_sec,
        html_body=html_body,
    )


__all__ = ["send_email", "send_email_to"]
