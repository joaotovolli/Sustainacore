<!-- cspell:ignore googlebot cooldown tablespace GEOIP MMDB geoip dbip mmdb -->
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
- API calls only when explicitly enabled for successful requests; API errors are still logged by default
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

## Data minimization
- `ip_trunc`: IPv4 /24 or IPv6 /48
- `ip_hash`: salted hash (uses `TELEMETRY_HASH_SALT` or `SECRET_KEY`)
- `W_WEB_EVENT` now stores compact event rows by default:
  - `event_name` for UI/download/Ask2 subtypes
  - host-only referrer (`referrer` / `referrer_host`), not the full URL
  - compact user-agent family tokens (`browser:chrome`, `bot:googlebot`, etc.), not the full header
  - short `debug_json` only for error/debug flows
- Full query strings and large JSON blobs are disabled by default. Set
  `TELEMETRY_STORE_LEGACY_EVENT_BLOBS=1` only for temporary incident diagnostics.
- Client-side UI events are deduplicated in-session before POSTing:
  - `filter_applied`: 30s
  - `search_submitted`: 15s
  - `ask2_opened`: 60s
  - `tab_changed`: 30s
  - `download_click`: no client cooldown

## Storage model
- Raw request/event rows stay in `W_WEB_EVENT` for a short operational window.
- Long-lived aggregates live in `W_WEB_EVENT_DAILY`.
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
- `TELEMETRY_LOG_SUCCESS_API_CALLS` (`1` to keep successful generic API-call logging; default OFF)
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

## Web telemetry usage report (VM1, ad hoc)
The VM1 web telemetry usage script still reads `WKSP_ESGAPEX.W_WEB_EVENT` directly today.
It is no longer the scheduled SC_IDX daily email path:
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

## SC_IDX operational telemetry (VM1)

The SC_IDX / TECH100 pipeline now emits lightweight server-side operational telemetry as part of the
LangGraph orchestration path. This is distinct from website telemetry and does not involve client-side
tracking.

Files written on each run:

- `tools/audit/output/pipeline_telemetry/sc_idx_pipeline_<RUN_ID>.json`
- `tools/audit/output/pipeline_telemetry/sc_idx_pipeline_latest.json`

Signals include:

- terminal status
- overall operator health verdict
- node timings
- retry counts
- provider readiness outcome
- ingest/imputation/index/statistics counts
- report generation status
- email decision and SMTP delivery state
- expected target date and source
- freshness dates for canon, levels, and stats
- portfolio analytics freshness and alignment
- stale signals and latest complete lag
- deployed `repo_root` and `repo_head`
- remediation and artifact paths

Use the pipeline smoke path for no-provider verification:

```bash
python tools/index_engine/run_pipeline.py --smoke --smoke-scenario degraded --restart
```

Failure alert semantics for the LangGraph pipeline:

- `failed` and `blocked` attempt email by default
- `stale` attempts email by default, even if the graph technically concluded
- repeated `success_with_degradation` attempts email by default after
  `SC_IDX_ALERT_DEGRADED_REPEAT_THRESHOLD` consecutive degraded runs
- single `success_with_degradation` only attempts email when `SC_IDX_EMAIL_ON_DEGRADED=1`
- `daily_budget_stop` only attempts email when `SC_IDX_EMAIL_ON_BUDGET_STOP=1`
- `clean_skip` only stays quiet when freshness is not stale
- smoke runs do not send email

Duplicate-suppression rules:

- alert gate state is stored in `SC_IDX_ALERT_STATE`
- the once-per-day gate is evaluated before send
- the gate is only marked after a successful SMTP delivery
- if SMTP config is missing or delivery fails, the report records `send_failed` and the gate is not consumed

Required SMTP env names for SC_IDX pipeline alerts:

- `SMTP_USER`
- `SMTP_PASS`
- `MAIL_FROM`
- `MAIL_TO`

## SC_IDX daily telemetry report (VM1)

VM1 now has a dedicated daily SC_IDX operator report instead of sending the website `W_WEB_EVENT`
usage report. The scheduled path is:

- `sc-telemetry-report.timer` -> `sc-telemetry-report.service`
- entrypoint: `python tools/index_engine/daily_telemetry_report.py --send`

The service loads:

- `/etc/sustainacore/db.env`
- `/etc/sustainacore/index.env`
- `/etc/sustainacore-ai/secrets.env`
- `TNS_ADMIN=/opt/adb_wallet`

Artifacts written by the daily report:

- `tools/audit/output/pipeline_daily/sc_idx_daily_report_<UTC_DATE>.json`
- `tools/audit/output/pipeline_daily/sc_idx_daily_report_<UTC_DATE>.txt`
- `tools/audit/output/pipeline_daily/sc_idx_daily_report_latest.json`
- `tools/audit/output/pipeline_daily/sc_idx_daily_report_latest.txt`

Recipient resolution order:

- `SC_IDX_DAILY_REPORT_RECIPIENTS`
- `TELEMETRY_REPORT_RECIPIENTS`
- `MAIL_TO`

Report sections:

- headline summary
- freshness and alignment
- stage-by-stage outcome
- data quality and operations
- alerts and risk signals
- artifact paths

Operator interpretation notes:

- Treat `overall_health=Stale` as an incident even if `terminal_status` is `clean_skip` or
  `success_with_degradation`.
- Compare `expected_target_date` to `latest_complete_date` before deciding whether the pipeline was
  truly up to date.
- Use `repo_root` and `repo_head` in the health snapshot and daily report to verify that VM1 is
  actually running the intended checkout.
- If trading-day refresh degraded on a timeout or 403, the report will show whether a bounded
  weekday fallback was used or whether the expected target had to remain an estimate.

Safe verification commands:

```bash
source .venv/bin/activate
python tools/index_engine/run_pipeline.py --smoke --smoke-scenario degraded --restart
python tools/index_engine/daily_telemetry_report.py --skip-db --dry-run
python - <<'PY'
from app.index_engine.alerts import smtp_configuration_status
print(smtp_configuration_status())
PY
```

Expected signals:

- the smoke run writes fresh pipeline JSON/text and telemetry artifacts
- the daily report renders from those artifacts without provider calls
- `smtp_configuration_status()` prints only booleans, counts, and missing env names, never secret values

Systemd scheduling (VM1):

```bash
sudo cp infra/systemd/sc-telemetry-report.* /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now sc-telemetry-report.timer
```

Default schedule is **06:45 UTC** with a lock (`/tmp/sc-telemetry-report.lock`) to prevent overlap.

## Tech100 related companies (VM1)
The related companies job pre-computes top candidates for Tech100 company pages using consented telemetry:
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

## Remaining raw-table dependencies
- `tools/telemetry/usage_report.py`
  - still needs raw rows for exact cross-group uniques in rolling windows
- `tools/telemetry/daily_report.py`
  - still needs raw rows for exact day-level unique session/visitor counts
- `tools/telemetry/related_companies.py`
  - intentionally stays on raw rows because it depends on event ordering within sessions
- `U_UX_EVENTS`
  - `new_verified_users` still comes from the legacy auth event stream, not `W_WEB_EVENT_DAILY`

## Reporting migration checklist
1. Move rolling totals, top pages, top referrers, country breakdowns, and error counts to `W_WEB_EVENT_DAILY`.
2. Keep `related_companies.py` on raw rows unless a separate session-transition rollup is introduced.
3. If exact unique session/visitor counts must leave raw storage, add a second daily summary table or an approximate distinct-count strategy.
4. Leave `new_verified_users` on `U_UX_EVENTS` until the auth/reporting stream is unified.

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
