"""Alerting helpers for research generator."""
from __future__ import annotations

import logging
import os
import smtplib
from email.message import EmailMessage
from typing import Dict

LOGGER = logging.getLogger("research_generator.alerting")

ALERT_ENV_PATH = "/etc/sustainacore/alerts.env"


def _load_env_file(path: str) -> Dict[str, str]:
    if not os.path.isfile(path):
        return {}
    payload: Dict[str, str] = {}
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            text = line.strip()
            if not text or text.startswith("#") or "=" not in text:
                continue
            key, value = text.split("=", 1)
            payload[key.strip()] = value.strip().strip('"').strip("'")
    return payload


def send_email(subject: str, body: str) -> bool:
    env = _load_env_file(ALERT_ENV_PATH)
    host = env.get("SMTP_HOST")
    port = int(env.get("SMTP_PORT", "0") or 0)
    user = env.get("SMTP_USER")
    password = env.get("SMTP_PASS")
    sender = env.get("SMTP_FROM")
    recipient = env.get("SMTP_TO")

    if not host or not sender or not recipient:
        LOGGER.warning("email_not_configured")
        return False

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = recipient
    msg.set_content(body)

    try:
        with smtplib.SMTP(host, port or 25, timeout=15) as smtp:
            smtp.ehlo()
            if port in (465, 587):
                try:
                    smtp.starttls()
                except Exception:
                    pass
            if user and password:
                smtp.login(user, password)
            smtp.send_message(msg)
        return True
    except Exception as exc:
        LOGGER.warning("email_send_failed %s", exc)
        return False
