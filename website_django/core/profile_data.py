from __future__ import annotations

from typing import Dict, Optional

import oracledb

from core.oracle_db import get_connection


TABLE_NAME = "SC_USER_PROFILE"


def _ensure_profile_table(cursor: oracledb.Cursor) -> None:
    try:
        cursor.execute(
            f"""
            CREATE TABLE {TABLE_NAME} (
                email_normalized VARCHAR2(320) PRIMARY KEY,
                name VARCHAR2(200),
                country VARCHAR2(120),
                company VARCHAR2(200),
                phone VARCHAR2(80),
                created_at TIMESTAMP DEFAULT SYSTIMESTAMP,
                updated_at TIMESTAMP DEFAULT SYSTIMESTAMP
            )
            """
        )
    except oracledb.DatabaseError as exc:
        error = exc.args[0] if exc.args else None
        if getattr(error, "code", None) == 955:
            return
        raise


def get_profile(email_normalized: str) -> Optional[Dict[str, str]]:
    if not email_normalized:
        return None
    with get_connection() as conn:
        with conn.cursor() as cursor:
            _ensure_profile_table(cursor)
            cursor.execute(
                f"""
                SELECT email_normalized, name, country, company, phone
                FROM {TABLE_NAME}
                WHERE email_normalized = :email
                """,
                {"email": email_normalized},
            )
            row = cursor.fetchone()
            if not row:
                return None
            return {
                "email_normalized": row[0],
                "name": row[1] or "",
                "country": row[2] or "",
                "company": row[3] or "",
                "phone": row[4] or "",
            }

def _normalize_optional(value: str) -> Optional[str]:
    stripped = (value or "").strip()
    return stripped if stripped else None


def upsert_profile(
    email_normalized: str,
    name: str,
    country: str,
    company: str,
    phone: str,
) -> None:
    if not email_normalized:
        return
    with get_connection() as conn:
        with conn.cursor() as cursor:
            _ensure_profile_table(cursor)
            cursor.execute(
                f"""
                MERGE INTO {TABLE_NAME} target
                USING (
                    SELECT :email email_normalized,
                           :name name,
                           :country country,
                           :company company,
                           :phone phone
                    FROM dual
                ) src
                ON (target.email_normalized = src.email_normalized)
                WHEN MATCHED THEN
                    UPDATE SET target.name = src.name,
                               target.country = src.country,
                               target.company = src.company,
                               target.phone = src.phone,
                               target.updated_at = SYSTIMESTAMP
                WHEN NOT MATCHED THEN
                    INSERT (email_normalized, name, country, company, phone, created_at, updated_at)
                    VALUES (src.email_normalized, src.name, src.country, src.company, src.phone, SYSTIMESTAMP, SYSTIMESTAMP)
                """,
                {
                    "email": email_normalized,
                    "name": _normalize_optional(name),
                    "country": _normalize_optional(country),
                    "company": _normalize_optional(company),
                    "phone": _normalize_optional(phone),
                },
            )
            conn.commit()
