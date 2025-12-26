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
                SELECT email_normalized, country, company, phone
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
                "country": row[1] or "",
                "company": row[2] or "",
                "phone": row[3] or "",
            }


def upsert_profile(email_normalized: str, country: str, company: str, phone: str) -> None:
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
                           :country country,
                           :company company,
                           :phone phone
                    FROM dual
                ) src
                ON (target.email_normalized = src.email_normalized)
                WHEN MATCHED THEN
                    UPDATE SET target.country = src.country,
                               target.company = src.company,
                               target.phone = src.phone,
                               target.updated_at = SYSTIMESTAMP
                WHEN NOT MATCHED THEN
                    INSERT (email_normalized, country, company, phone, created_at, updated_at)
                    VALUES (src.email_normalized, src.country, src.company, src.phone, SYSTIMESTAMP, SYSTIMESTAMP)
                """,
                {
                    "email": email_normalized,
                    "country": country,
                    "company": company,
                    "phone": phone,
                },
            )
            conn.commit()


def is_profile_complete(profile: Optional[Dict[str, str]]) -> bool:
    if not profile:
        return False
    return bool((profile.get("country") or "").strip()) and bool(
        (profile.get("company") or "").strip()
    )
