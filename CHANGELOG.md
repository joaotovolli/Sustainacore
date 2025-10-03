# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]
- Canonicalized live assets under `app/apex/` and `db/schema/` with documentation refresh.
- Archived dated backups into the structured `archive/` tree and published a manifest of moves.
- Added repo hygiene defaults (.gitignore, .editorconfig, cspell workflow) and README polish.

## 2025-09-10 – Gemini-first orchestration
- Added Gemini CLI planning + composition as the only generation path.
- Implemented Oracle 23ai retriever with filter-first vector search, aggressive deduplication, and clean citation payloads.
- Introduced per-request telemetry, daily Top-10 Miss reports, and IP rate limiting.
- Updated `/ask2` output schema to `answer + sources` with developer diagnostics in `meta.debug` only.
- Documented configuration, feature flags, and runbook in the new README.

**Rollback:** set `GEMINI_FIRST_ENABLED=0` and reload the service to return to the previous behaviour without redeploying code.
