# AI regulation Oracle load evidence

Run metadata:
- Run date (UTC): TBD
- Bundle: /home/opc/incoming/ai_reg/ai_reg_oracle_output_bundle.zip
- As-of date(s) in data: 2026-01-09

Row counts (expected minimums from CSVs):
- DIM_LOAD_BATCH: 22
- STG_AI_REG_RECORD_RAW: 439
- DIM_JURISDICTION: 145
- DIM_INSTRUMENT: 236
- FACT_INSTRUMENT_SNAPSHOT: 236
- DIM_TAG: 25
- BRG_SNAPSHOT_TAG: 360
- DIM_SECTOR: 11
- BRG_SNAPSHOT_SECTOR: 146
- DIM_AUTHORITY: 152
- BRG_SNAPSHOT_AUTHORITY: 166
- FACT_SNAPSHOT_OBLIGATION: 562
- DIM_SOURCE: 340
- BRG_SNAPSHOT_SOURCE: 342
- FACT_SNAPSHOT_MILESTONE_DATE: 97
- BRG_SNAPSHOT_BATCH: 439

Warnings:
- Load not executed yet. Replace counts with actual DB counts after running verify.sql.
- Update the run date and capture any loader warnings or row count mismatches.

## How to fill this after running verify.sql
1) Run the verification command in `infra/geo_ai_reg/README.md` and capture output.
2) Replace the row counts above with the actual DB counts from verify.sql.
3) Update the run date and record the min/max as-of date reported by verify.sql.
4) Note any non-zero orphan counts or loader warnings in the Warnings section.
