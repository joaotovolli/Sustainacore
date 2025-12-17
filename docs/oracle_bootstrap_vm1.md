# Oracle bootstrap contract (VM1)

Use this one-page checklist whenever running Oracle-facing scripts from Codex CLI or automation on VM1. Shells often start without `DB_*` or `TNS_ADMIN` set; the bootstrap handles that safely without sourcing shell-incompatible env files.

## Preflight (always run)

```
python3 tools/oracle/preflight_oracle.py
```

What you’ll see:

- Presence summary for required envs (`present`/`missing` only)
- `CONNECTIVITY_OK elapsed_ms=...` on success
- `CONNECTIVITY_FAIL ...` with a brief error if the connection fails

If it fails, stop and share only the presence lines and the error line—never paste secrets.

## When writing or running scripts

1) Always call `load_env_files()` from `tools.oracle.env_bootstrap` before any Oracle call.
2) Always use `db_helper.get_connection()` for Oracle access (no ad-hoc connections).
3) Never `source` `/etc/sustainacore/*.env` or `/etc/sustainacore-ai/*.env`; they are not bash-safe.

## Quick connectivity check (alternative)

```
python3 tools/test_db_connect.py
```

This script also loads env files and performs a minimal `SELECT 1` with fast-fail timeouts; it redacts secrets in its output.
