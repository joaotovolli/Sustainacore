"""Initialize PROC_RESEARCH_SETTINGS table."""
from __future__ import annotations

import logging
import os
import sys

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

import db_helper  # noqa: E402
from tools.oracle.env_bootstrap import load_env_files  # noqa: E402

LOGGER = logging.getLogger("research_generator.init_proc_research_settings")


def main() -> int:
    load_env_files()
    conn = db_helper.get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            BEGIN
                EXECUTE IMMEDIATE '
                    CREATE TABLE proc_research_settings (
                        settings_id NUMBER PRIMARY KEY,
                        schedule_enabled CHAR(1) DEFAULT ''Y'' NOT NULL,
                        schedule_tz VARCHAR2(16) DEFAULT ''UTC'' NOT NULL,
                        schedule_hour NUMBER DEFAULT 3 NOT NULL,
                        schedule_minute NUMBER DEFAULT 0 NOT NULL,
                        schedule_freq VARCHAR2(16) DEFAULT ''DAILY'' NOT NULL,
                        schedule_dow_mask VARCHAR2(64),
                        max_context_pct NUMBER DEFAULT 10 NOT NULL,
                        saver_mode VARCHAR2(16) DEFAULT ''MEDIUM'' NOT NULL,
                        dev_noop CHAR(1) DEFAULT ''N'' NOT NULL,
                        updated_at TIMESTAMP WITH TIME ZONE,
                        updated_by VARCHAR2(80)
                    )
                ';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -955 THEN
                        RAISE;
                    END IF;
            END;
            """
        )
        cur.execute(
            """
            MERGE INTO proc_research_settings target
            USING (SELECT 1 AS settings_id FROM dual) src
            ON (target.settings_id = src.settings_id)
            WHEN NOT MATCHED THEN
              INSERT (
                settings_id, schedule_enabled, schedule_tz, schedule_hour,
                schedule_minute, schedule_freq, schedule_dow_mask,
                max_context_pct, saver_mode, dev_noop, updated_at, updated_by
              ) VALUES (
                1, 'Y', 'UTC', 3, 0, 'DAILY', NULL,
                10, 'MEDIUM', 'N', SYSTIMESTAMP, 'system'
              )
            """
        )
        conn.commit()
    finally:
        conn.close()
    LOGGER.info("PROC_RESEARCH_SETTINGS ready")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
