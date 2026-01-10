# AI regulation Oracle load evidence

Run metadata:
- Run date (UTC): 2026-01-10 17:02:56 UTC
- Bundle: /home/opc/incoming/ai_reg/ai_reg_oracle_output_bundle.zip
- Bundle sha256: bf319ef396d507fb0f5c70d9423898646f16d3dc3eade324261bb31dfdafcbac
- As-of date(s) in data: 2026-01-09
- Patch audit: `infra/geo_ai_reg/output/patch_audit.md`

Commands executed:
- `python3 tools/oracle/preflight_oracle.py`
- `rm -rf /tmp/ai_reg_bundle_work && mkdir -p /tmp/ai_reg_bundle_work`
- `unzip -q /home/opc/incoming/ai_reg/ai_reg_oracle_output_bundle.zip -d /tmp/ai_reg_bundle_work`
- `python3 infra/geo_ai_reg/load/patch_bundle_csvs.py --input-dir /tmp/ai_reg_bundle_work`
- `python3 infra/geo_ai_reg/load/load_bundle.py --dir /tmp/ai_reg_bundle_work --drop-and-recreate --ddl-only`
- `./infra/geo_ai_reg/load/run_all.sh --dir /tmp/ai_reg_bundle_work`
- `python3 - <<'PY' ... infra/geo_ai_reg/sql/verify.sql ... PY`
- `sha256sum /home/opc/incoming/ai_reg/ai_reg_oracle_output_bundle.zip`

Patch summary:
- FACT_INSTRUMENT_SNAPSHOT.csv: blanks fixed=8, derived as-of=2026-01-09
- STG_AI_REG_RECORD_RAW.csv: blanks fixed=95, derived as-of=2026-01-09

Verification output (verify.sql):
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
- AS_OF_RANGE: min=2026-01-09 max=2026-01-09
- orphan_snapshot_jur: 0
- orphan_snapshot_inst: 0
- orphan_source_bridge: 0
- orphan_tag_bridge: 0
- orphan_sector_bridge: 0
- orphan_authority_bridge: 0
- snapshots_without_source: 21

Warnings:
- None.
