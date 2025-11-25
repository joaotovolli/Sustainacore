# Security Review – VM2

## Summary of changes in this revision
- Removed the legacy hard-coded SECRET_KEY from settings.py so the key no longer appears in the repository at HEAD; SECRET_KEY is now sourced exclusively from DJANGO_SECRET_KEY.

## Risks and mitigations
- host names are pinned to known domains to prevent accidental exposure on unexpected hosts.
- Environment variables store all sensitive configuration, including `DJANGO_SECRET_KEY`.
