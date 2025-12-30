# Web Telemetry (Consent-Aware)

This document describes the privacy-friendly telemetry system for sustainacore.org.

## Principles
- No non-essential analytics cookies or client-side events until explicit opt-in.
- Server-side telemetry is minimal and used for reliability, security, and product improvement.
- IP addresses are truncated for storage, with a salted hash for dedupe and abuse prevention.

## Consent model
- Consent is stored in a first-party cookie (`sc_consent`) after the user makes a choice.
- If the user does nothing, analytics consent is treated as **no**.
- Choices are reflected in the banner or the Privacy settings modal.
- Once consent is set, the banner does not reappear on subsequent pages.

## What is logged
Server-side (always, minimal):
- Page views (HTML responses)
- API calls (TECH100 data endpoints, Ask2 API)
- Downloads (including gated/allowed status)
- Ask2 chat metadata (latency, success, size)

Client-side (only if analytics consent = yes):
- `filter_applied`
- `search_submitted`
- `download_click`
- `ask2_opened`
- `tab_changed`

Ask2 content (stored in Oracle):
- Ask2 user and assistant messages are stored in `W_WEB_ASK2_CONVERSATION` and `W_WEB_ASK2_MESSAGE`.
- Content is truncated to 20,000 characters per message.
- Use `manage.py purge_ask2_chats --days N` to remove older conversations.

## Data minimisation
- `ip_trunc`: IPv4 /24 or IPv6 /48
- `ip_hash`: salted hash (uses `TELEMETRY_HASH_SALT` or `SECRET_KEY`)
- User agent and referrer are stored when present.

## Retention
- Default retention is 180 days for telemetry events.
- `manage.py purge_web_telemetry --days N` removes older rows.

## Deployment & migrations
- Production uses the same Oracle config as the website (preferred):
  - `DB_USER`, `DB_PASSWORD` (or `DB_PASS`), `DB_DSN`
  - Fallback: `ORACLE_USER`, `ORACLE_PASSWORD`, `ORACLE_DSN` or `ORACLE_CONNECT_STRING`
- Gunicorn should load `/etc/sustainacore.env` plus `/etc/sustainacore/db.env`.
- Run migrations against the active production database:
  - `python website_django/manage.py migrate --noinput`
- Verify Oracle + telemetry tables:
  - `python website_django/manage.py diagnose_db --fail-on-sqlite --verify-insert`
 - Verify Ask2 storage:
   - `python website_django/manage.py diagnose_ask2_storage --fail-on-sqlite --verify-insert`

## Oracle permissions checklist
If migrations fail or tables are missing, confirm the Oracle user has:
- `CREATE TABLE` privilege
- Sufficient tablespace quota for the target schema

## Configuration
Environment variables (optional):
- `TELEMETRY_POLICY_VERSION` (default `2025-12-30`)
- `TELEMETRY_HASH_SALT` (default `SECRET_KEY`)
- `TELEMETRY_RETENTION_DAYS` (default `180`)
- `TELEMETRY_TRUST_X_FORWARDED_FOR` (`1` to trust proxy headers)
- `TELEMETRY_STORE_ASK2_TEXT` (`1` to store Ask2 message text; default OFF)
- `ASK2_STORE_CONVERSATIONS` (`1` to store Ask2 prompts + replies in events; default OFF)
