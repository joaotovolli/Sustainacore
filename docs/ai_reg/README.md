# AI regulation MVP (Django)

## Overview

The `/ai-regulation/` page and data endpoints query the Oracle GEO loader tables directly.
All queries use the shared Oracle connection utility from `website_django/core/oracle_db.py`.

## Endpoints

### GET /ai-regulation/

HTML page with filters and placeholder panels for the globe and drilldown.

### GET /ai-regulation/data/as-of-dates

Returns the available snapshot dates from `FACT_INSTRUMENT_SNAPSHOT`.

Response:
```json
{
  "as_of_dates": ["2025-01-15", "2024-12-15"]
}
```

### GET /ai-regulation/data/heatmap?as_of=YYYY-MM-DD

Returns the heatmap dataset per jurisdiction.

Response:
```json
{
  "as_of": "2025-01-15",
  "jurisdictions": [
    {
      "iso2": "US",
      "name": "United States",
      "instruments_count": 12,
      "instrument_count": 12,
      "primary_verified_count": 6,
      "secondary_only_count": 3,
      "no_instrument_found_count": 3,
      "milestones_upcoming_count": 2,
      "data_quality": {
        "snapshots_without_source": 1,
        "flag": true
      }
    }
  ]
}
```

Notes:
- `instrument_count` is an alias of `instruments_count` for compatibility with draft specs.
- `milestones_upcoming_count` is computed for milestone dates in the next 24 months relative to `as_of`.
- `data_quality.flag` is true when `snapshots_without_source > 0`.

### GET /ai-regulation/data/jurisdiction/<iso2>/?as_of=YYYY-MM-DD

Returns the jurisdiction drilldown bundle.

Response:
```json
{
  "as_of": "2025-01-15",
  "jurisdiction": {
    "iso2": "US",
    "name": "United States",
    "obligations_count": 14,
    "data_quality": {
      "snapshots_without_source": 1,
      "flag": true
    }
  },
  "instruments": [
    {
      "title_english": "AI Act",
      "title_official": "Regulation on AI",
      "instrument_type": "law",
      "status": "in_force"
    }
  ],
  "milestones": [
    {"milestone_type": "effective", "milestone_date": "2026-01-01"}
  ],
  "sources": [
    {"title": "Official Gazette", "url": "https://example.com"}
  ]
}
```

### GET /ai-regulation/data/jurisdiction/<iso2>/instruments?as_of=YYYY-MM-DD

Returns the instruments list for a jurisdiction.

### GET /ai-regulation/data/jurisdiction/<iso2>/timeline?as_of=YYYY-MM-DD

Returns milestone dates (`effective`, `enforcement_start`) for a jurisdiction.

## Data sources

All queries target the GEO AI regulation schema created by the loader in `infra/geo_ai_reg`:
- `FACT_INSTRUMENT_SNAPSHOT`
- `DIM_JURISDICTION`
- `DIM_INSTRUMENT`
- `FACT_SNAPSHOT_OBLIGATION`
- `FACT_SNAPSHOT_MILESTONE_DATE`
- `BRG_SNAPSHOT_SOURCE`
- `DIM_SOURCE`

## Caching

Heatmap responses are cached per `as_of` date using the Django default cache backend.
