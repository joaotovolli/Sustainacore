# AI regulation Oracle platform runbook

## Overview
This runbook loads the AI regulation Oracle bundle into the VM1 Oracle schema.
The bundle contains the DDL, CSVs, and model README. The loader is dependency-aware
and uses the DDL foreign keys to order inserts.

## Layout
- `infra/geo_ai_reg/load/load_bundle.py`: loader (DDL parse, load order, CSV ingest)
- `infra/geo_ai_reg/load/run_all.sh`: wrapper (preflight + loader options)
- `infra/geo_ai_reg/sql/verify.sql`: verification queries
- `infra/geo_ai_reg/DATA_DICTIONARY.md`: table/column dictionary
- `infra/geo_ai_reg/output/load_evidence.md`: release evidence template

## Preflight (Oracle)
Always run this before Oracle work:

```bash
cd /home/opc/Sustainacore
python3 tools/oracle/preflight_oracle.py
```

## Unzip the bundle
Keep the bundle in place or unzip it into a working folder:

```bash
cd /home/opc/Sustainacore
rm -rf /tmp/ai_reg_bundle_work
mkdir -p /tmp/ai_reg_bundle_work
unzip -o /home/opc/incoming/ai_reg/ai_reg_oracle_output_bundle.zip -d /tmp/ai_reg_bundle_work
```

## Patch missing AS_OF_DATE values (bundle fix)
If the CSVs contain blank `AS_OF_DATE` values, patch the extracted bundle in-place
using the patcher (day-first parsing is default for this dataset):

```bash
cd /home/opc/Sustainacore
python3 infra/geo_ai_reg/load/patch_bundle_csvs.py \
  --input-dir /tmp/ai_reg_bundle_work \
  --day-first
```

Patch audit output is appended to:
`infra/geo_ai_reg/output/patch_audit.md`.

## Apply the DDL
If this is a fresh schema (or you need a clean rebuild), apply the DDL.

```bash
cd /home/opc/Sustainacore
python3 infra/geo_ai_reg/load/load_bundle.py \
  --dir /tmp/ai_reg_bundle_work \
  --ddl-only
```

To drop and recreate all tables before applying the DDL:

```bash
cd /home/opc/Sustainacore
python3 infra/geo_ai_reg/load/load_bundle.py \
  --dir /tmp/ai_reg_bundle_work \
  --drop-and-recreate --ddl-only
```

## Load the CSVs
Default load (appends new snapshots; preserves history) from the extracted directory:

```bash
cd /home/opc/Sustainacore
./infra/geo_ai_reg/load/run_all.sh \
  --dir /tmp/ai_reg_bundle_work
```

Reload a clean dataset (drops and recreates tables first):

```bash
cd /home/opc/Sustainacore
./infra/geo_ai_reg/load/run_all.sh \
  --dir /tmp/ai_reg_bundle_work \
  --drop-and-recreate
```

Reload into existing schema by truncating tables:

```bash
cd /home/opc/Sustainacore
./infra/geo_ai_reg/load/run_all.sh \
  --dir /tmp/ai_reg_bundle_work \
  --truncate
```

Dry-run (shows dependency order + CSV row counts, no DB writes):

```bash
cd /home/opc/Sustainacore
./infra/geo_ai_reg/load/run_all.sh \
  --dir /tmp/ai_reg_bundle_work \
  --dry-run
```

## Verification
Run the verification SQL and capture output for release evidence:

```bash
cd /home/opc/Sustainacore
python3 - <<'PY'
from pathlib import Path
from tools.oracle.env_bootstrap import load_env_files
import db_helper

load_env_files()
sql = Path("infra/geo_ai_reg/sql/verify.sql").read_text(encoding="utf-8")
statements = [s.strip() for s in sql.split(";") if s.strip()]

with db_helper.get_connection() as conn:
    cur = conn.cursor()
    for stmt in statements:
        cur.execute(stmt)
        rows = cur.fetchall()
        print("--", stmt.replace("\n", " ")[:120])
        for row in rows:
            print(row)
PY
```

## Troubleshooting
- If preflight fails, stop and report only the presence lines and error line.
- If you see `row_count_mismatch`, confirm the CSV counts with `--dry-run` and
  re-run with `--truncate` or `--drop-and-recreate`.
- If a load fails mid-table, re-run with `--truncate` to ensure referential
  integrity before loading again.
- For ORA-02291/ORA-02292, confirm you did not load child tables before parents.
  The loader uses FK order from the DDL; validate the DDL matches the bundle.
