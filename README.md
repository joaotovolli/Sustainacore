# Sustainacore

Monorepo for APEX app, Oracle DB scripts, REST API, and ETL.

Generated on **2025-08-09T15:41:05.338186Z**

## Layout
- `api/` — Flask API (`/ask`) with CORS enabled for APEX
- `etl/` — scripts for benchmark data and ingestion
- `db/` — DDL/views/procs for index, risk, attribution
- `ords/` — ORDS modules/routes (optional)
- `apex/` — APEX App Export (.sql)
- `scripts/` — VM bootstrap and utilities

## Quick Start (VM)
```bash
cd api
python3 -m venv ../venv && source ../venv/bin/activate
pip install -r requirements.txt
export TNS_ADMIN=$HOME/wallet
export DB_DSN=dbri4x6_high
export DB_USER=WKSP_ESGAPEX
export DB_PASS='<your password>'
python api_app.py
```

## APEX wiring
See `apex/README_APEX.md` for a minimal page that calls your `/ask` endpoint.
