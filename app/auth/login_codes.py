from __future__ import annotations

import base64
import hashlib
import hmac
import json
import re
import secrets
from datetime import datetime, timedelta, timezone
from typing import Optional

import db_helper
from app.index_engine.alerts import send_email_to

EMAIL_REGEX = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

CODE_TTL_SECONDS = 120
TOKEN_TTL_SECONDS = 30 * 24 * 60 * 60
MAX_ATTEMPTS = 5
EMAIL_RATE_LIMIT_MAX = 3
EMAIL_RATE_LIMIT_MINUTES = 15
IP_RATE_LIMIT_MAX = 10
IP_RATE_LIMIT_HOURS = 1

MAIL_FROM_LOGIN = "info@sustainacore.org"


def normalize_email(value: str) -> str:
    return value.strip().lower()


def is_valid_email(value: str) -> bool:
    if not value:
        return False
    return bool(EMAIL_REGEX.match(value))


def generate_code() -> str:
    return f"{secrets.randbelow(1_000_000):06d}"


def generate_salt() -> str:
    return secrets.token_hex(16)


def hash_code(email_normalized: str, code: str, salt: str) -> str:
    payload = f"{email_normalized}:{code}:{salt}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _base64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def build_jwt(email_normalized: str, signing_key: str, expires_in_seconds: int) -> str:
    now = int(datetime.now(timezone.utc).timestamp())
    payload = {
        "sub": email_normalized,
        "iat": now,
        "exp": now + expires_in_seconds,
    }
    header = {"alg": "HS256", "typ": "JWT"}
    header_b64 = _base64url(json.dumps(header, separators=(",", ":")).encode("utf-8"))
    payload_b64 = _base64url(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    signing_input = f"{header_b64}.{payload_b64}".encode("ascii")
    signature = hmac.new(signing_key.encode("utf-8"), signing_input, hashlib.sha256).digest()
    return f"{header_b64}.{payload_b64}.{_base64url(signature)}"


def _coerce_datetime(value: Optional[object]) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return None
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    return None


def _is_expired(expires_at: Optional[object]) -> bool:
    expires = _coerce_datetime(expires_at)
    if not expires:
        return True
    return expires < datetime.now(timezone.utc)


def send_login_email(to_email: str, code: str) -> None:
    subject = "Your login code"
    body = "\n".join(
        [
            f"Your login code: {code}",
            "Expires in 2 minutes",
            "If you didn't request this, ignore",
        ]
    )
    send_email_to(to_email, subject, body, mail_from=MAIL_FROM_LOGIN)


def request_login_code(email_normalized: str, request_ip: str) -> bool:
    request_ip = request_ip or "unknown"
    code = generate_code()
    salt = generate_salt()
    code_hash = hash_code(email_normalized, code, salt)

    sql_count_email = (
        "SELECT COUNT(*) FROM SC_AUTH_LOGIN_CODES "
        "WHERE email_normalized = :email "
        "AND created_at >= (SYSTIMESTAMP - INTERVAL '15' MINUTE)"
    )
    sql_count_ip = (
        "SELECT COUNT(*) FROM SC_AUTH_LOGIN_CODES "
        "WHERE request_ip = :request_ip "
        "AND created_at >= (SYSTIMESTAMP - INTERVAL '1' HOUR)"
    )
    sql_insert = (
        "INSERT INTO SC_AUTH_LOGIN_CODES "
        "(email_normalized, code_hash, salt, expires_at, created_at, request_ip, attempts) "
        "VALUES (:email, :code_hash, :salt, "
        "SYSTIMESTAMP + INTERVAL '2' MINUTE, SYSTIMESTAMP, :request_ip, 0)"
    )

    try:
        with db_helper.get_connection() as conn:
            cur = conn.cursor()
            cur.execute(sql_count_email, {"email": email_normalized})
            email_count = int((cur.fetchone() or [0])[0] or 0)
            cur.execute(sql_count_ip, {"request_ip": request_ip})
            ip_count = int((cur.fetchone() or [0])[0] or 0)
            if email_count >= EMAIL_RATE_LIMIT_MAX or ip_count >= IP_RATE_LIMIT_MAX:
                return False
            cur.execute(
                sql_insert,
                {
                    "email": email_normalized,
                    "code_hash": code_hash,
                    "salt": salt,
                    "request_ip": request_ip,
                },
            )
            conn.commit()
    except Exception:
        return False

    send_login_email(email_normalized, code)
    return True


def verify_login_code(email_normalized: str, code: str, signing_key: str) -> Optional[str]:
    sql_select = (
        "SELECT id, code_hash, salt, expires_at, attempts "
        "FROM SC_AUTH_LOGIN_CODES "
        "WHERE email_normalized = :email AND used_at IS NULL "
        "ORDER BY created_at DESC FETCH FIRST 1 ROWS ONLY"
    )
    sql_update_attempts = (
        "UPDATE SC_AUTH_LOGIN_CODES "
        "SET attempts = attempts + 1, last_attempt_at = SYSTIMESTAMP "
        "WHERE id = :id"
    )
    sql_update_used = (
        "UPDATE SC_AUTH_LOGIN_CODES "
        "SET used_at = SYSTIMESTAMP "
        "WHERE id = :id"
    )

    try:
        with db_helper.get_connection() as conn:
            cur = conn.cursor()
            cur.execute(sql_select, {"email": email_normalized})
            row = cur.fetchone()
            if not row:
                return None
            code_id, code_hash, salt, expires_at, attempts = row
            if attempts is None:
                attempts = 0
            if attempts >= MAX_ATTEMPTS:
                return None
            if _is_expired(expires_at):
                return None

            expected = hash_code(email_normalized, code, str(salt))
            if not hmac.compare_digest(str(code_hash), expected):
                cur.execute(sql_update_attempts, {"id": code_id})
                conn.commit()
                return None

            cur.execute(sql_update_used, {"id": code_id})
            conn.commit()
    except Exception:
        return None

    return build_jwt(email_normalized, signing_key, TOKEN_TTL_SECONDS)


__all__ = [
    "CODE_TTL_SECONDS",
    "TOKEN_TTL_SECONDS",
    "MAX_ATTEMPTS",
    "normalize_email",
    "is_valid_email",
    "request_login_code",
    "verify_login_code",
    "hash_code",
    "send_login_email",
]
