# GEO AI regulation Oracle data platform

This documentation covers the Oracle AI regulation dataset used for the GEO
AI regulation experiences (heat map, globe, and drilldown views). It provides
repeatable loading, verification, and data dictionary references for analysts
and developers.

## What this enables
- Heat map tiles by jurisdiction and status.
- Globe overlays for jurisdiction-level regulation coverage.
- Drilldown views with obligations, sources, and milestone dates.

## Key references
- Runbook: `../../infra/geo_ai_reg/README.md`
- Data dictionary: `../../infra/geo_ai_reg/DATA_DICTIONARY.md`
- Verification SQL: `../../infra/geo_ai_reg/sql/verify.sql`
- Release evidence template: `../../infra/geo_ai_reg/output/load_evidence.md`

## Data flow overview
1) Load DDL and CSVs from the bundle using the runbook.
2) Verify counts and joins using `verify.sql`.
3) Use `FACT_INSTRUMENT_SNAPSHOT` as the core fact table and join to bridge
   tables for tags, sectors, authorities, sources, and milestone dates.
