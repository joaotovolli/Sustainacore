"""Verify EU canonicalization in DIM_JURISDICTION and dependent tables."""
from __future__ import annotations

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.oracle.env_bootstrap import load_env_files
from db_helper import get_connection


def main() -> int:
    load_env_files()
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT jurisdiction_sk, iso_code, name
            FROM dim_jurisdiction
            WHERE UPPER(iso_code) IN ('EU', 'EUR')
               OR UPPER(name) LIKE '%EUROPEAN UNION%'
               OR UPPER(name) = 'EU'
            ORDER BY jurisdiction_sk
            """
        )
        rows = cur.fetchall()
        print("EU_DIM_ROWS=" + str(len(rows)))
        for row in rows:
            print(f"EU_DIM_ROW={row[0]}|{row[1]}|{row[2]}")

        cur.execute(
            """
            SELECT COUNT(*)
            FROM dim_jurisdiction
            WHERE UPPER(iso_code) = 'EU'
              AND UPPER(name) = 'EUROPEAN UNION'
            """
        )
        canon_count = int(cur.fetchone()[0] or 0)
        print(f"EU_CANONICAL_COUNT={canon_count}")

        cur.execute(
            """
            SELECT COUNT(*)
            FROM fact_instrument_snapshot s
            JOIN dim_jurisdiction j ON s.jurisdiction_sk = j.jurisdiction_sk
            WHERE UPPER(j.iso_code) IN ('EU', 'EUR')
               OR UPPER(j.name) LIKE '%EUROPEAN UNION%'
               OR UPPER(j.name) = 'EU'
            """
        )
        snap_count = int(cur.fetchone()[0] or 0)
        print(f"EU_SNAPSHOT_ROWS={snap_count}")

        cur.execute(
            """
            SELECT COUNT(*)
            FROM stg_ai_reg_record_raw
            WHERE UPPER(jurisdiction_iso_code) IN ('EU', 'EUR')
               OR UPPER(jurisdiction_name) LIKE '%EUROPEAN UNION%'
               OR UPPER(jurisdiction_name) = 'EU'
            """
        )
        stg_count = int(cur.fetchone()[0] or 0)
        print(f"EU_STG_ROWS={stg_count}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
