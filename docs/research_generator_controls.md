# Research Generator Controls (VM1/VM2)
<!-- cspell:ignore VARCHAR -->

## Purpose
- Document the control-plane for scheduled research runs.
- Define the single-row Oracle settings table and how VM1/VM2 interact.

## Architecture
- VM1
  - Runs the research generator and systemd timers.
  - Reads `PROC_RESEARCH_SETTINGS` (row-mode) at startup.
  - Enforces `SCHEDULE_ENABLED` and `DEV_NOOP` on scheduled runs.
- VM2
  - Admin portal UI writes settings into `PROC_RESEARCH_SETTINGS`.
  - Must treat it as a single row (`SETTINGS_ID=1`), not a key/value table.

## Oracle Table (Row-Mode)
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

## Settings Reference
| Setting | Type | Allowed values | Effect | Default |
| --- | --- | --- | --- | --- |
| SCHEDULE_ENABLED | CHAR(1) | Y/N | Enables or disables scheduled runs. | Y |
| DEV_NOOP | CHAR(1) | Y/N | Detect triggers only; exit without LLM calls. | N |
| SAVER_MODE | VARCHAR2 | MINIMAL/LOW/MEDIUM | Reduces angles, metrics, charts, and iterations. | MEDIUM |
| MAX_CONTEXT_PCT | NUMBER | 1-100 | Clamps profile based on budget. | 10 |
| SCHEDULE_FREQ | VARCHAR2 | DAILY/WEEKLY | Schedule policy (future UI). | DAILY |
| SCHEDULE_HOUR | NUMBER | 0-23 | Scheduled hour (UTC/GMT). | 3 |
| SCHEDULE_MINUTE | NUMBER | 0-59 | Scheduled minute. | 0 |
| SCHEDULE_TZ | VARCHAR2 | UTC/GMT | Schedule time zone. | UTC |
| SCHEDULE_DOW_MASK | VARCHAR2 | MON,TUE... | Weekly mask (future UI). | null |

## Quick Start / Toggle Checklist (No-Cost)
1) Initialize settings table:
   - `python3 tools/research_generator/init_proc_research_settings.py`
2) Disable schedule and verify early exit:
   - `python3 -m tools.research_generator.run_generator --set-settings SCHEDULE_ENABLED=N`
   - `python3 -m tools.research_generator.run_generator --once --scheduled --log-level INFO`
3) Dev-noop path (trigger detection only):
   - `python3 -m tools.research_generator.run_generator --set-settings SCHEDULE_ENABLED=Y DEV_NOOP=Y`
   - `python3 -m tools.research_generator.run_generator --once --scheduled --log-level INFO`

## Logs
- Scheduled: `journalctl -u research-generator.service -n 200 --no-pager`
- Manual: `journalctl -u research-generator-manual.service -n 200 --no-pager`
- Timers: `systemctl status research-generator.timer research-generator-manual.timer --no-pager`

## Recovery
- If the settings table is missing:
  - Run `python3 tools/research_generator/init_proc_research_settings.py`
