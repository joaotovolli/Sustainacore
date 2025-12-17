# Oracle unused objects audit (VM1) + repo-wide scan

This repo includes scripts to generate a **read-only** Oracle audit bundle (TABLE/VIEW objects) and to scan the repository for references, separating **DDL-only mentions** from **runtime code mentions**.

## Preflight (mandatory)

Run the canonical connectivity probe first:

```bash
python3 tools/oracle/preflight_oracle.py
```

Continue only if it prints `CONNECTIVITY_OK`.

## Create an audit bundle under `ops/audits/`

```bash
ts=$(date +%Y%m%d_%H%M%S)
OUT="ops/audits/oracle_unused_audit/$ts"
mkdir -p "$OUT"

python3 tools/oracle_audit/run_unused_tables_audit.py --out-dir "$OUT"
python3 tools/oracle_audit/repo_scan_and_rename_plan.py --out-dir "$OUT"
```

The bundle includes Oracle evidence CSVs (`ALL_DEPENDENCIES`, `ALL_CONSTRAINTS`, `ALL_SYNONYMS`, count-only source hits) plus:
- `repo_hits_runtime.csv` / `repo_hits_ddl.csv` / `repo_hits_all.csv`
- `rename_plan_proposed.csv` with explicit `DO_NOT_RENAME_INTERNAL` for Oracle-managed internals (e.g., `DR$`, `VECTOR$`)

## Safety
- Oracle access is **SELECT-only** (no DDL).
- Artifacts contain only object names, counts, and repo file paths/line numbers (no credentials or wallet contents).

