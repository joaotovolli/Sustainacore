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

Analytics identifiers (only if analytics consent = yes):
- `session_key`: server session key used to group events
- `user_id`: anonymous numeric ID derived from the first-party analytics cookie

Ask2 content (stored in Oracle):
- Ask2 user and assistant messages are stored in `W_WEB_ASK2_CONVERSATION` and `W_WEB_ASK2_MESSAGE`.
- Content is truncated to 20,000 characters per message.
- Use `manage.py purge_ask2_chats --days N` to remove older conversations.

## Data minimisation
- `ip_trunc`: IPv4 /24 or IPv6 /48
- `ip_hash`: salted hash (uses `TELEMETRY_HASH_SALT` or `SECRET_KEY`)
- `W_WEB_EVENT` now stores compact event rows by default:
  - `event_name` for UI/download/Ask2 subtypes
  - host-only referrer (`referrer` / `referrer_host`), not the full URL
  - compact user-agent family tokens (`browser:chrome`, `bot:googlebot`, etc.), not the full header
  - short `debug_json` only for event types that need compact metadata
- Full query strings and large JSON blobs are disabled by default. Set
  `TELEMETRY_STORE_LEGACY_EVENT_BLOBS=1` only for temporary incident diagnostics.

## Storage model
- Raw request/event rows stay in `W_WEB_EVENT` for a short operational window.
- Long-lived rollups live in `W_WEB_EVENT_DAILY`.
- The daily table keeps:
  - UTC day
  - event type + subtype
  - page path
  - referrer host
  - consent flag
  - bot flag
  - country
  - status class
  - event counts, unique session/user/visitor counts, and latency summaries
- This keeps long-term analytics useful without retaining one full blob-heavy row per request.

## Country enrichment + consent
- Country/region is derived from trusted headers (or GeoIP fallback) and stored for server-side events.
- Analytics identifiers (`session_key`, `user_id`) remain consent-gated.
- Reporting defaults to **consented analytics** for country breakdowns; non-consented traffic is reported as counts only unless policy allows otherwise.

IP selection (server-side):
- Preferred order: `X-Forwarded-For` (first public IP), `X-Real-IP`, then `REMOTE_ADDR`.
- Private/reserved/bogon ranges are ignored unless all candidates are non-public.

## Retention
- Default retention is 35 days for raw events.
- Default retention is 400 days for daily aggregates.
- `manage.py aggregate_web_telemetry --date YYYY-MM-DD` rolls one UTC day into `W_WEB_EVENT_DAILY`.
- `manage.py purge_web_telemetry --raw-days 35 --aggregates --aggregate-days 400 --sessions --consents`
  removes expired raw/session/consent rows.
- Recommended steady-state flow on VM2:
  1. roll yesterday into `W_WEB_EVENT_DAILY`
  2. purge expired raw/session/consent/aggregate rows

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
 - Install the VM2 rollup timer after Oracle writes are healthy:
   - `sudo cp infra/systemd/sc-web-telemetry-rollup.* /etc/systemd/system/`
   - `sudo systemctl daemon-reload`
   - `sudo systemctl enable --now sc-web-telemetry-rollup.timer`

## Oracle permissions checklist
If migrations fail or tables are missing, confirm the Oracle user has:
- `CREATE TABLE` privilege
- Sufficient tablespace quota for the target schema

## Configuration
Environment variables (optional):
- `TELEMETRY_POLICY_VERSION` (default `2025-12-30`)
- `TELEMETRY_HASH_SALT` (default `SECRET_KEY`)
- `TELEMETRY_RAW_RETENTION_DAYS` (default `35`)
- `TELEMETRY_AGGREGATE_RETENTION_DAYS` (default `400`)
- `TELEMETRY_SESSION_RETENTION_DAYS` (default `35`)
- `TELEMETRY_CONSENT_RETENTION_DAYS` (default `180`)
- `TELEMETRY_TRUST_X_FORWARDED_FOR` (`1` to trust proxy headers; default on)
- `TELEMETRY_STORE_ASK2_TEXT` (`1` to store Ask2 message text; default OFF)
- `TELEMETRY_STORE_LEGACY_EVENT_BLOBS` (`1` to temporarily store full query string / payload blobs again)
- `ASK2_STORE_CONVERSATIONS` (`1` to store Ask2 prompts + replies in events; default OFF)
- `TELEMETRY_GEO_COUNTRY_HEADERS` (comma-separated header names for country code)
- `TELEMETRY_GEO_REGION_HEADERS` (comma-separated header names for region code)
- `TELEMETRY_DEBUG_HEADERS` (`1` to expose a local-only header presence endpoint)
- `TELEMETRY_GEOIP_ENABLED` (`1` to enable GeoIP fallback; default OFF)
- `TELEMETRY_GEOIP_DB_PATH` (filesystem path to the GeoIP database file)

## Geo enablement
- Country/region enrichment requires upstream headers (set in `TELEMETRY_GEO_COUNTRY_HEADERS` and
  `TELEMETRY_GEO_REGION_HEADERS`).
- For local verification, enable `TELEMETRY_DEBUG_HEADERS=1` and call
  `/telemetry/debug/headers/` to see which header names are present (values are not returned).
- If header values include comma-separated values, only the first entry is used.
- To enable geo enrichment, configure the edge/reverse proxy to inject
  `X-Country-Code` / `X-Region-Code` (or equivalent) and then set
  `TELEMETRY_GEO_COUNTRY_HEADERS` / `TELEMETRY_GEO_REGION_HEADERS` accordingly.
- Optional GeoIP fallback (off by default):
  - Provision a GeoIP database file on the VM (outside the repo).
  - Set `TELEMETRY_GEOIP_ENABLED=1` and `TELEMETRY_GEOIP_DB_PATH=/path/to/db`.
  - Restart `gunicorn.service` to pick up the environment changes.
  - Current ops configuration uses DB-IP Lite City MMDB from:
    - https://db-ip.com/db/download/ip-to-city-lite
  - Installed path on VM2:
    - `/opt/sustainacore/geoip/dbip-city-lite.mmdb`

## Telemetry usage report (VM1)
The VM1 report script aggregates `WKSP_ESGAPEX.W_WEB_EVENT` with bot/dev filtering:
```bash
python tools/telemetry/usage_report.py --dry-run
python tools/telemetry/usage_report.py --all --dry-run
python tools/telemetry/usage_report.py --all --send
python tools/telemetry/usage_report.py --window 7d --json-out /tmp/telemetry.json
```

Filters (env-driven, no secrets in Git):
- `TELEMETRY_EXCLUDE_IP_HASHES` (comma-separated)
- `TELEMETRY_EXCLUDE_SESSION_KEYS`
- `TELEMETRY_EXCLUDE_USER_IDS`
- `TELEMETRY_EXCLUDE_UA_SUBSTRINGS` (comma-separated; used to filter your traffic)
- `TELEMETRY_BOT_UA_REGEX` (optional)
- `TELEMETRY_PROBE_PATH_REGEX` (optional)
- `TELEMETRY_REPORT_RECIPIENTS` (comma-separated email list; fallback to `MAIL_TO`)
- `TELEMETRY_REPORT_CALL_TIMEOUT_MS` (per-query timeout; default 30000ms)

Systemd scheduling (VM1):
```bash
sudo cp infra/systemd/sc-telemetry-report.* /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now sc-telemetry-report.timer
```
Default schedule is **06:45 UTC** with a lock (`/tmp/sc-telemetry-report.lock`) to prevent overlap.

## Tech100 related companies (VM1)
The related companies job precomputes top candidates for Tech100 company pages using consented telemetry:
```bash
python tools/telemetry/related_companies.py --window-days 30 --top-k 12
python tools/telemetry/related_companies.py --window-days 30 --top-k 12 --dry-run
```

Filters (env-driven, no secrets in Git):
- `RELATED_EXCLUDE_IP_HASHES` (comma-separated)
- `RELATED_EXCLUDE_USER_IDS`
- `RELATED_EXCLUDE_UA_SUBSTRINGS`
- `RELATED_BOT_UA_REGEX` (optional)
- `RELATED_PROBE_PATH_REGEX` (optional)
- `RELATED_WINDOW_DAYS` (default 30)
- `RELATED_TOP_K` (default 12)
- `RELATED_GOOGLE_BOOST_WEIGHT` (default 3.0)
- `RELATED_MAX_EVENTS` (optional cap for local diagnostics; default 0 = no cap)

Systemd scheduling (VM1):
```bash
sudo cp infra/systemd/sc-tech100-related.* /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now sc-tech100-related.timer
```
Default schedule is **07:15 UTC** with a lock (`/tmp/sc-tech100-related.lock`) to prevent overlap.

## Recommended rollout
1. Apply the telemetry migration so `W_WEB_EVENT` gains lean columns and `W_WEB_EVENT_DAILY`.
2. Deploy the slimmer writer.
3. Schedule a daily rollup:
   - `sudo cp infra/systemd/sc-web-telemetry-rollup.* /etc/systemd/system/`
   - `sudo systemctl daemon-reload`
   - `sudo systemctl enable --now sc-web-telemetry-rollup.timer`
4. Shorten raw retention only after the rollup is running:
   - `python website_django/manage.py purge_web_telemetry --aggregates --sessions --consents`

## Systemd scheduling (VM2)
Use the repo-managed VM2 timer once Oracle can accept writes again:
```bash
sudo cp infra/systemd/sc-web-telemetry-rollup.* /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now sc-web-telemetry-rollup.timer
```

Manual run:
```bash
sudo systemctl start sc-web-telemetry-rollup.service
sudo journalctl -u sc-web-telemetry-rollup.service -n 200 --no-pager
```

Default schedule is **00:35 UTC** with a lock (`/tmp/sc-web-telemetry-rollup.lock`) to prevent overlap.
