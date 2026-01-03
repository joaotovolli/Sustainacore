# Research Generator Data Dictionary

## Oracle Tables

### TECH11_AI_GOV_ETH_INDEX
- port_date (DATE)
- company_name (VARCHAR2)
- ticker (VARCHAR2)
- port_weight (NUMBER)
- gics_sector (VARCHAR2)
- aiges_composite_average (NUMBER)
- summary (CLOB)
- source_links (CLOB)
- Optional pillars (if present):
  - aiges_pillar_policy
  - aiges_pillar_transparency
  - aiges_pillar_accountability
  - aiges_pillar_safety

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
- ticker (VARCHAR2)
- contribution (NUMBER)

### PROC_REPORTS
- report_key (VARCHAR2)
- report_value (VARCHAR2)
- updated_at (TIMESTAMP)

### PROC_RESEARCH_REQUESTS
- request_id (NUMBER)
- status (VARCHAR2)
- request_type (VARCHAR2)
- company_ticker (VARCHAR2)
- window_start (TIMESTAMP)
- window_end (TIMESTAMP)
- editor_notes (CLOB)
- source_approval_id (NUMBER)
- created_by (VARCHAR2)
- created_at (TIMESTAMP)
- updated_at (TIMESTAMP)
- result_text (CLOB)

### PROC_GEMINI_APPROVALS
- approval_id (NUMBER)
- request_type (VARCHAR2)
- title (VARCHAR2)
- proposed_text (CLOB)
- details (CLOB)
- file_name (VARCHAR2)
- file_mime (VARCHAR2)
- file_blob (BLOB)
- status (VARCHAR2)
- created_at (TIMESTAMP)
