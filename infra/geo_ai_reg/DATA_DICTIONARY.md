# AI regulation Oracle data dictionary

This dictionary reflects the bundle DDL (`ai_reg_oracle_schema.sql`) and
model README. Column types are listed as defined in the DDL.

## Core joins
- `FACT_INSTRUMENT_SNAPSHOT` -> `DIM_JURISDICTION` on `JURISDICTION_SK`.
- `FACT_INSTRUMENT_SNAPSHOT` -> `DIM_INSTRUMENT` on `INSTRUMENT_SK`.
- `FACT_INSTRUMENT_SNAPSHOT` -> `BRG_SNAPSHOT_TAG` -> `DIM_TAG`.
- `FACT_INSTRUMENT_SNAPSHOT` -> `BRG_SNAPSHOT_SECTOR` -> `DIM_SECTOR`.
- `FACT_INSTRUMENT_SNAPSHOT` -> `BRG_SNAPSHOT_AUTHORITY` -> `DIM_AUTHORITY`.
- `FACT_INSTRUMENT_SNAPSHOT` -> `BRG_SNAPSHOT_SOURCE` -> `DIM_SOURCE`.
- `FACT_INSTRUMENT_SNAPSHOT` -> `FACT_SNAPSHOT_OBLIGATION` on `SNAPSHOT_SK`.
- `FACT_INSTRUMENT_SNAPSHOT` -> `FACT_SNAPSHOT_MILESTONE_DATE` on `SNAPSHOT_SK`.
- `FACT_INSTRUMENT_SNAPSHOT` -> `BRG_SNAPSHOT_BATCH` -> `DIM_LOAD_BATCH`.

## Recommended queries
- Snapshot coverage by jurisdiction:
  ```sql
  SELECT j.NAME, COUNT(*) AS snapshots
  FROM FACT_INSTRUMENT_SNAPSHOT s
  JOIN DIM_JURISDICTION j ON s.JURISDICTION_SK = j.JURISDICTION_SK
  GROUP BY j.NAME
  ORDER BY snapshots DESC;
  ```
- Heat map inputs (jurisdiction x status counts):
  ```sql
  SELECT j.NAME, s.STATUS, COUNT(*) AS snapshot_count
  FROM FACT_INSTRUMENT_SNAPSHOT s
  JOIN DIM_JURISDICTION j ON s.JURISDICTION_SK = j.JURISDICTION_SK
  GROUP BY j.NAME, s.STATUS
  ORDER BY j.NAME, s.STATUS;
  ```
- Evidence coverage (snapshots with primary sources):
  ```sql
  SELECT COUNT(DISTINCT s.SNAPSHOT_SK) AS snapshots_with_primary
  FROM FACT_INSTRUMENT_SNAPSHOT s
  JOIN BRG_SNAPSHOT_SOURCE b ON s.SNAPSHOT_SK = b.SNAPSHOT_SK
  WHERE b.SOURCE_TYPE = 'primary';
  ```

## DIM_LOAD_BATCH
- Grain: one row per source file (batch).
- Primary key: `BATCH_SK`.
- Foreign keys: none.
- Intended usage: provenance and audit for every batch/file ingested.

Columns:
- `BATCH_SK` (NUMBER, PK): surrogate key for the batch.
- `BATCH_NAME` (VARCHAR2(200)): human-friendly file name.
- `SOURCE_MODEL` (VARCHAR2(20)): `gpt`, `copilot`, or `unknown`.
- `SOURCE_REGION` (VARCHAR2(200)): region label inferred from file name.
- `SOURCE_FILE_PATH` (VARCHAR2(500)): source path used during generation.
- `FILE_HASH` (VARCHAR2(64)): SHA-256 of source file contents.
- `LOAD_TS_UTC` (TIMESTAMP): conversion/load timestamp (UTC).

## STG_AI_REG_RECORD_RAW
- Grain: one row per raw JSON object.
- Primary key: `RAW_SK`.
- Foreign keys: `BATCH_SK` -> `DIM_LOAD_BATCH.BATCH_SK`.
- Intended usage: raw audit trail and reprocessing source.

Columns:
- `RAW_SK` (NUMBER, PK): surrogate key for raw record.
- `BATCH_SK` (NUMBER, FK): provenance link to batch.
- `OBJ_HASH` (VARCHAR2(64)): SHA-256 hash of the JSON payload.
- `AS_OF_DATE` (DATE): as-of date in source payload.
- `JURISDICTION_ISO_CODE` (VARCHAR2(10)): ISO alpha-2 or special codes.
- `JURISDICTION_NAME` (VARCHAR2(200)): jurisdiction name.
- `INSTRUMENT_ID` (VARCHAR2(200)): instrument identifier from payload.
- `VERIFICATION_STATUS` (VARCHAR2(60)): verification status.
- `CONFIDENCE` (VARCHAR2(10)): confidence label.
- `JSON_PAYLOAD` (CLOB): full raw JSON payload.
- `LOAD_TS_UTC` (TIMESTAMP): conversion/load timestamp (UTC).

## DIM_JURISDICTION
- Grain: one row per jurisdiction.
- Primary key: `JURISDICTION_SK`.
- Foreign keys: none.
- Intended usage: jurisdiction dimension for snapshot facts.

Columns:
- `JURISDICTION_SK` (NUMBER, PK): surrogate key.
- `ISO_CODE` (VARCHAR2(10)): ISO alpha-2 or special code (e.g., EU).
- `NAME` (VARCHAR2(200)): jurisdiction display name.

## DIM_INSTRUMENT
- Grain: one row per instrument.
- Primary key: `INSTRUMENT_SK`.
- Foreign keys: none.
- Intended usage: canonical instrument dimension keyed by `INSTRUMENT_ID`.

Columns:
- `INSTRUMENT_SK` (NUMBER, PK): surrogate key.
- `INSTRUMENT_ID` (VARCHAR2(200), unique): stable instrument slug.
- `TITLE_OFFICIAL` (VARCHAR2(1000)): official title.
- `TITLE_ENGLISH` (VARCHAR2(1000)): English title if provided.
- `INSTRUMENT_TYPE` (VARCHAR2(40)): normalized instrument type.

## FACT_INSTRUMENT_SNAPSHOT
- Grain: one row per jurisdiction x instrument x as_of_date.
- Primary key: `SNAPSHOT_SK`.
- Foreign keys: `JURISDICTION_SK` -> `DIM_JURISDICTION`, `INSTRUMENT_SK` -> `DIM_INSTRUMENT`.
- Intended usage: core snapshot fact for analytics and time-series history.

Columns:
- `SNAPSHOT_SK` (NUMBER, PK): surrogate key.
- `JURISDICTION_SK` (NUMBER, FK): jurisdiction link.
- `INSTRUMENT_SK` (NUMBER, FK): instrument link.
- `AS_OF_DATE` (DATE): snapshot date.
- `STATUS` (VARCHAR2(40)): status (e.g., draft, in_force, unknown).
- `APPLIES_TO_JURISDICTION` (CHAR(1)): Y/N/U.
- `EXTRATERRITORIAL_REACH` (VARCHAR2(10)): yes/no/unclear.
- `VERIFICATION_STATUS` (VARCHAR2(60)): primary_verified / secondary_only / no_ai_specific_binding_instrument_found.
- `CONFIDENCE` (VARCHAR2(10)): high/medium/low.
- `ANNOUNCED_DATE` (DATE): milestone date.
- `DRAFT_PUBLISHED_DATE` (DATE): milestone date.
- `CONSULTATION_START_DATE` (DATE): milestone date.
- `CONSULTATION_END_DATE` (DATE): milestone date.
- `INTRODUCED_DATE` (DATE): milestone date.
- `PASSED_DATE` (DATE): milestone date.
- `PUBLISHED_DATE` (DATE): milestone date.
- `SIGNED_DATE` (DATE): milestone date.
- `PHASE_IN_NOTES` (CLOB): phase-in notes.
- `SUMMARY_NEUTRAL` (CLOB): neutral summary.
- `PENALTIES_SUMMARY` (CLOB): penalties summary.
- `VERIFICATION_GAP` (CLOB): missing-evidence narrative.
- `TREATY_STATUS` (VARCHAR2(40)): treaty status for jurisdiction.
- `TREATY_DEPOSITARY` (VARCHAR2(200)): treaty depositary/body.
- `OBJ_HASH` (VARCHAR2(64)): snapshot payload hash.

## DIM_TAG
- Grain: one row per AI scope tag.
- Primary key: `TAG_SK`.
- Foreign keys: none.
- Intended usage: flexible tag dimension for AI scope.

Columns:
- `TAG_SK` (NUMBER, PK): surrogate key.
- `TAG_CODE` (VARCHAR2(60), unique): tag code.

## BRG_SNAPSHOT_TAG
- Grain: one row per snapshot x tag.
- Primary key: composite (`SNAPSHOT_SK`, `TAG_SK`).
- Foreign keys: `SNAPSHOT_SK` -> `FACT_INSTRUMENT_SNAPSHOT`, `TAG_SK` -> `DIM_TAG`.
- Intended usage: many-to-many tag membership.

Columns:
- `SNAPSHOT_SK` (NUMBER, PK/FK): snapshot link.
- `TAG_SK` (NUMBER, PK/FK): tag link.

## DIM_SECTOR
- Grain: one row per sector code.
- Primary key: `SECTOR_SK`.
- Foreign keys: none.
- Intended usage: sector scope dimension.

Columns:
- `SECTOR_SK` (NUMBER, PK): surrogate key.
- `SECTOR_CODE` (VARCHAR2(60), unique): sector code.

## BRG_SNAPSHOT_SECTOR
- Grain: one row per snapshot x sector.
- Primary key: composite (`SNAPSHOT_SK`, `SECTOR_SK`).
- Foreign keys: `SNAPSHOT_SK` -> `FACT_INSTRUMENT_SNAPSHOT`, `SECTOR_SK` -> `DIM_SECTOR`.
- Intended usage: many-to-many sector scope mapping.

Columns:
- `SNAPSHOT_SK` (NUMBER, PK/FK): snapshot link.
- `SECTOR_SK` (NUMBER, PK/FK): sector link.

## DIM_AUTHORITY
- Grain: one row per authority.
- Primary key: `AUTHORITY_SK`.
- Foreign keys: none.
- Intended usage: enforcing/issuing authority dimension.

Columns:
- `AUTHORITY_SK` (NUMBER, PK): surrogate key.
- `AUTHORITY_NAME` (VARCHAR2(500), unique): authority name.

## BRG_SNAPSHOT_AUTHORITY
- Grain: one row per snapshot x authority.
- Primary key: composite (`SNAPSHOT_SK`, `AUTHORITY_SK`).
- Foreign keys: `SNAPSHOT_SK` -> `FACT_INSTRUMENT_SNAPSHOT`, `AUTHORITY_SK` -> `DIM_AUTHORITY`.
- Intended usage: many-to-many authority mapping.

Columns:
- `SNAPSHOT_SK` (NUMBER, PK/FK): snapshot link.
- `AUTHORITY_SK` (NUMBER, PK/FK): authority link.

## FACT_SNAPSHOT_OBLIGATION
- Grain: one row per snapshot x obligation sequence.
- Primary key: composite (`SNAPSHOT_SK`, `OBLIGATION_SEQ`).
- Foreign keys: `SNAPSHOT_SK` -> `FACT_INSTRUMENT_SNAPSHOT`.
- Intended usage: ordered obligations per instrument.

Columns:
- `SNAPSHOT_SK` (NUMBER, PK/FK): snapshot link.
- `OBLIGATION_SEQ` (NUMBER, PK): obligation sequence.
- `OBLIGATION_TEXT` (CLOB): obligation text.

## DIM_SOURCE
- Grain: one row per evidence source.
- Primary key: `SOURCE_SK`.
- Foreign keys: none.
- Intended usage: normalized sources for provenance and citations.

Columns:
- `SOURCE_SK` (NUMBER, PK): surrogate key.
- `SOURCE_TYPE` (VARCHAR2(10)): primary or secondary.
- `TITLE` (VARCHAR2(1000)): source title.
- `PUBLISHER` (VARCHAR2(500)): publisher name.
- `SOURCE_DATE_STR` (VARCHAR2(20)): raw date string.
- `SOURCE_DATE` (DATE): parsed date when available.
- `URL` (VARCHAR2(2000)): source URL.

## BRG_SNAPSHOT_SOURCE
- Grain: one row per snapshot x source.
- Primary key: composite (`SNAPSHOT_SK`, `SOURCE_SK`).
- Foreign keys: `SNAPSHOT_SK` -> `FACT_INSTRUMENT_SNAPSHOT`, `SOURCE_SK` -> `DIM_SOURCE`.
- Intended usage: evidence linkage for snapshots.

Columns:
- `SNAPSHOT_SK` (NUMBER, PK/FK): snapshot link.
- `SOURCE_SK` (NUMBER, PK/FK): source link.
- `SOURCE_TYPE` (VARCHAR2(10)): primary or secondary.
- `CITATION_NOTE` (CLOB): evidence note.

## FACT_SNAPSHOT_MILESTONE_DATE
- Grain: one row per snapshot x milestone type x date.
- Primary key: composite (`SNAPSHOT_SK`, `MILESTONE_TYPE`, `MILESTONE_DATE`).
- Foreign keys: `SNAPSHOT_SK` -> `FACT_INSTRUMENT_SNAPSHOT`.
- Intended usage: multi-valued milestone dates.

Columns:
- `SNAPSHOT_SK` (NUMBER, PK/FK): snapshot link.
- `MILESTONE_TYPE` (VARCHAR2(30)): `effective` or `enforcement_start`.
- `MILESTONE_DATE` (DATE): milestone date.

## BRG_SNAPSHOT_BATCH
- Grain: one row per snapshot x batch.
- Primary key: composite (`SNAPSHOT_SK`, `BATCH_SK`).
- Foreign keys: `SNAPSHOT_SK` -> `FACT_INSTRUMENT_SNAPSHOT`, `BATCH_SK` -> `DIM_LOAD_BATCH`.
- Intended usage: provenance mapping between snapshots and batches.

Columns:
- `SNAPSHOT_SK` (NUMBER, PK/FK): snapshot link.
- `BATCH_SK` (NUMBER, PK/FK): batch link.
