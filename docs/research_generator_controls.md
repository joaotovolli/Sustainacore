# Research Generator Controls (VM1/VM2)

Purpose
- Document the control-plane for scheduled research runs.
- Defines the single-row Oracle settings table and how VM1/VM2 interact.

Architecture
- VM1
  - Runs the research generator and systemd timers.
  - Reads `PROC_RESEARCH_SETTINGS` (row-mode) at startup.
  - Enforces `SCHEDULE_ENABLED` and `DEV_NOOP` on scheduled runs.
- VM2
  - Admin portal UI writes settings into `PROC_RESEARCH_SETTINGS`.
  - Must treat it as a single row (SETTINGS_ID=1), not key/value pairs.

Oracle Table (ROW-MODE)
`PROC_RESEARCH_SETTINGS` columns:
- SETTINGS_ID (PK; row 1 is canonical)
- SCHEDULE_ENABLED (CHAR(1) Y/N)
- DEV_NOOP (CHAR(1) Y/N)
- SAVER_MODE (VARCHAR2; MINIMAL/LOW/MEDIUM)
- MAX_CONTEXT_PCT (NUMBER; context budget cap)
- SCHEDULE_FREQ (VARCHAR2; DAILY/WEEKLY)
- SCHEDULE_HOUR (NUMBER 0-23)
- SCHEDULE_MINUTE (NUMBER 0-59)
- SCHEDULE_TZ (VARCHAR2; UTC/GMT)
- SCHEDULE_DOW_MASK (VARCHAR2; optional)
- UPDATED_AT, UPDATED_BY

Settings Semantics
- SCHEDULE_ENABLED
  - `Y` = allow scheduled runs.
  - `N` = scheduled runs exit immediately (timer can remain enabled).
- DEV_NOOP
  - `Y` = detect triggers only, then exit with no LLM calls.
  - `N` = normal scheduled path.
- SAVER_MODE
  - `MINIMAL`/`LOW`/`MEDIUM` adjust angles, metrics, charts/tables, and iterations.
- MAX_CONTEXT_PCT
  - If low, the generator clamps to a smaller profile regardless of SAVER_MODE.

Runbook (No-Cost Verification)
1) Init settings table:
   - `python3 tools/research_generator/init_proc_research_settings.py`
2) Disable schedule and verify early exit:
   - `python3 -m tools.research_generator.run_generator --set-settings SCHEDULE_ENABLED=N`
   - `python3 -m tools.research_generator.run_generator --once --scheduled --log-level INFO`
3) Dev-noop path (trigger detection only):
   - `python3 -m tools.research_generator.run_generator --set-settings SCHEDULE_ENABLED=Y DEV_NOOP=Y`
   - `python3 -m tools.research_generator.run_generator --once --scheduled --log-level INFO`

Logs
- Scheduled: `journalctl -u research-generator.service -n 200 --no-pager`
- Manual: `journalctl -u research-generator-manual.service -n 200 --no-pager`

Recovery
- If the settings table is missing:
  - Run `python3 tools/research_generator/init_proc_research_settings.py`
