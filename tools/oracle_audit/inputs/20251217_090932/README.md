# Oracle unused tables audit inputs (20251217_090932)

This folder contains read-only artifacts generated on VM1 from an Oracle schema audit and repo usage scan.

## Contents
- `unused_tables.txt`: tables classified as `UNUSED_CANDIDATE` by the audit rules.
- `usage_summary_tables.csv`: per-table evidence signals and classification.
- `rename_plan_full.csv`: proposed rename actions, including Oracle Text (`DR$`) mapping notes.
- `repo_hits_vm1.csv`: VM1 repo scan results for candidate tables.
- `rename_candidates_tables.txt`: tables with zero repo hits (safe-to-rename candidates).
- `rename_blocked_tables.txt`: tables with any repo hits (blocked, with paths/lines).

## Safety / secrets
These files include only object names (tables/indexes) and counts/paths/line numbers.
They do **not** include database credentials, environment values, or wallet contents.

## How generated (high level)
- Oracle connectivity bootstrapped via `tools/oracle/env_bootstrap.py` and `db_helper.get_connection()`.
- Audit and plan scripts: `tools/oracle_audit/run_unused_tables_audit.py`, `tools/oracle_audit/build_rename_plan_from_unused.py`.
