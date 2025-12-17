# Oracle unused objects audit bundle (20251217_100747)

This folder contains read-only artifacts generated on VM1 for the current Oracle schema using the canonical bootstrap (`tools/oracle/env_bootstrap.py`) and connection helper (`db_helper.get_connection()`), plus a repo-wide reference scan.

## Oracle (SELECT-only) signals
- `objects_inventory.csv`: TABLE/VIEW inventory from `USER_OBJECTS`.
- `dependency_counts.csv`: counts of stored-object dependencies from `ALL_DEPENDENCIES` (referencing this TABLE/VIEW).
- `fk_inbound_counts.csv`: inbound FK counts per TABLE (0 for VIEWs).
- `synonym_counts.csv`: synonym counts from `ALL_SYNONYMS`.
- `text_hits.csv`: count-only hits from `ALL_SOURCE` (and `ALL_VIEWS`/`ALL_TRIGGERS` where accessible).
- `row_signals.csv`: informational stats (and optional COUNT(*) for very small tables only).
- `usage_summary_objects.csv`: combined evidence + classification for TABLE/VIEW objects.

## Unused lists
- `unused_objects.txt`: all objects classified `UNUSED_CANDIDATE` (prefix `TABLE:` / `VIEW:`).
- `unused_tables.txt`, `unused_views.txt`: type-specific lists.

## Repo scan (runtime vs DDL-only)
Repo scanning is case-insensitive and identifier-boundary aware, with separate accounting for:
- DDL-only mentions: `db/schema/`, `db/migrations/`, `app/apex/`
- Runtime mentions: everything else (including `website_django/`)

Outputs:
- `repo_hits_all.csv`, `repo_hits_runtime.csv`, `repo_hits_ddl.csv`

## Rename planning
- `rename_candidates_tables_views.txt`: objects with `dependency_count == 0` and `runtime_hits == 0` and not Oracle-managed internals.
- `rename_blocked_tables_views.txt`: everything else with a short reason.
- `rename_plan_proposed.csv`: consolidated plan (includes explicit `DO_NOT_RENAME_INTERNAL` for Oracle-managed internals like `DR$`, `VECTOR$`, etc.).

## Safety / secrets
Artifacts include only object names, counts, and repo file paths/line numbers. No credentials, environment values, or wallet material are included.

