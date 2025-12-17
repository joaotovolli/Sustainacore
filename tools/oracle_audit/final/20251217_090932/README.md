# Runtime rename plan summary

## Scan parameters
- Case-insensitive, word-boundary matching
- Include paths: app/**, tools/**, index/**, retrieval/**, website_django/**, infra/**
- Exclude paths: db/schema/**, db/migrations/**, app/apex/**, **/*.md

## Counts
- Tables to rename: 31
- Tables skipped due to runtime signals or VECTOR$ internals: 3
- Oracle Text indexes to rename: 5
- Oracle Text indexes skipped: 0
- VECTOR$ internal objects logged: 3

## Artifacts
- `repo_hits_runtime.csv`: runtime reference counts with sample locations.
- `final_rename_tables.csv` and `.txt`: table rename plan with Oracle-safe `OLD_` targets.
- `final_rename_text_indexes.csv` and `.txt`: Oracle Text index rename plan (deduped by text index name).
- `final_vector_internals.txt`: VECTOR$ internals flagged for vector-index-level handling on VM1.
