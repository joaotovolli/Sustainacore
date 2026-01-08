# Research Generator Data Dictionary
<!-- cspell:ignore herfindahl VARCHAR -->

## Oracle Tables

### TECH11_AI_GOV_ETH_INDEX
- port_date (DATE)
- company_name (string)
- ticker (string)
- port_weight (NUMBER)
- gics_sector (string)
- aiges_composite_average (NUMBER)
- summary (text)
- source_links (text)
- Optional pillars (if present):
  - aiges_pillar_policy
  - aiges_pillar_transparency
  - aiges_pillar_accountability
  - aiges_pillar_safety

Notes:
- Core Index = rows with port_weight > 0
- Coverage Universe = all rows for a port_date
- Zero-weight slice = rows with port_weight = 0

### SC_IDX_STATS_DAILY
- trade_date (DATE)
- ret_1d (NUMBER)
- ret_5d (NUMBER)
- ret_20d (NUMBER)
- vol_20d (NUMBER)
- max_drawdown_252d (NUMBER)
- n_constituents (NUMBER)
- top5_weight (NUMBER)
- herfindahl (NUMBER)

### SC_IDX_LEVELS
- trade_date (DATE)
- level_tr (NUMBER)

### SC_IDX_CONTRIBUTION_DAILY
- trade_date (DATE)
- ticker (string)
- contribution (NUMBER)

### PROC_REPORTS
- report_key (string)
- report_value (string)
- updated_at (TIMESTAMP)

### PROC_RESEARCH_REQUESTS
- request_id (NUMBER)
- status (string)
- request_type (string)
- company_ticker (string)
- window_start (TIMESTAMP)
- window_end (TIMESTAMP)
- editor_notes (text)
- source_approval_id (NUMBER)
- created_by (string)
- created_at (TIMESTAMP)
- updated_at (TIMESTAMP)
- result_text (text)

### PROC_RESEARCH_SETTINGS (Row-mode)
- settings_id (NUMBER, PK; use row 1)
- schedule_enabled (CHAR(1), Y/N)
- dev_noop (CHAR(1), Y/N)
- saver_mode (string; MINIMAL/LOW/MEDIUM)
- max_context_pct (NUMBER; budget cap)
- schedule_freq (string; DAILY/WEEKLY)
- schedule_hour (NUMBER 0-23)
- schedule_minute (NUMBER 0-59)
- schedule_tz (string; UTC/GMT)
- schedule_dow_mask (string; optional)
- updated_at (TIMESTAMP WITH TIME ZONE)
- updated_by (string)

### PROC_GEMINI_APPROVALS
- approval_id (NUMBER)
- request_type (string)
- title (string)
- proposed_text (text)
- details (text)
- file_name (string)
- file_mime (string)
- file_blob (BLOB)
- status (VARCHAR2)
- created_at (TIMESTAMP)
