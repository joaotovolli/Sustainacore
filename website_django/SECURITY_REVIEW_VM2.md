# Security Review – VM2

## Summary of changes in this revision
- Confirmed that the legacy hard-coded SECRET_KEY has been removed from settings.py; Django now requires DJANGO_SECRET_KEY from the environment at startup.

## Risks and mitigations
- Host names are pinned to known domains to prevent accidental exposure on unexpected hosts.
- Environment variables store all sensitive configuration, including `DJANGO_SECRET_KEY`.
