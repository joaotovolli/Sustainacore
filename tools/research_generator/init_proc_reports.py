"""Initialize PROC_REPORTS table."""
from __future__ import annotations

import logging
import os
import sys

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

import db_helper  # noqa: E402
from tools.oracle.env_bootstrap import load_env_files

LOGGER = logging.getLogger("research_generator.init_proc_reports")


def main() -> int:
    load_env_files()
    conn = db_helper.get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            BEGIN
                EXECUTE IMMEDIATE '
                    CREATE TABLE proc_reports (
                        report_key VARCHAR2(200) PRIMARY KEY,
                        report_value CLOB,
                        updated_at TIMESTAMP
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
        conn.commit()
    finally:
        conn.close()
    LOGGER.info("PROC_REPORTS ready")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
