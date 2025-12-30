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

## Data minimisation
- `ip_trunc`: IPv4 /24 or IPv6 /48
- `ip_hash`: salted hash (uses `TELEMETRY_HASH_SALT` or `SECRET_KEY`)
- User agent and referrer are stored when present.

## Retention
- Default retention is 180 days for telemetry events.
- `manage.py purge_web_telemetry --days N` removes older rows.

## Configuration
Environment variables (optional):
- `TELEMETRY_POLICY_VERSION` (default `2025-12-30`)
- `TELEMETRY_HASH_SALT` (default `SECRET_KEY`)
- `TELEMETRY_RETENTION_DAYS` (default `180`)
- `TELEMETRY_TRUST_X_FORWARDED_FOR` (`1` to trust proxy headers)
- `TELEMETRY_STORE_ASK2_TEXT` (`1` to store Ask2 message text; default OFF)
