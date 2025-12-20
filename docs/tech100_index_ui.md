# TECH100 Index Analytics UI (VM2)

## Overview
This UI surfaces TECH100 index performance and risk analytics directly from Oracle SC_IDX tables on VM2.

See `docs/frontend_workflow_vm2.md` for the VM2 frontend workflow, UI quality gate, and screenshot requirements.

## URLs
- `/` — Home page with TECH100 snapshot (chart + KPIs)
- `/tech100/index/` — Performance & risk overview
- `/tech100/performance/` — Full analytics hub (overview, attribution, holdings)
- `/tech100/constituents/` — Constituents & weights
- `/tech100/attribution/` — Daily contribution
- `/tech100/stats/` — SC_IDX_STATS_DAILY snapshot

JSON endpoints:
- `/api/tech100/index-levels?range=1m|3m|6m|ytd|1y|max`
- `/api/tech100/index/attribution?range=1d|mtd|ytd`
- `/api/tech100/index/holdings?date=YYYY-MM-DD`
- `/api/tech100/kpis?date=YYYY-MM-DD`
- `/api/tech100/constituents?date=YYYY-MM-DD`
- `/api/tech100/attribution?date=YYYY-MM-DD`
- `/api/tech100/stats?date=YYYY-MM-DD`

## Data access
- Uses `core.oracle_db.get_connection()` (thick mode, wallet already configured on VM2).
- Queries SC_IDX_LEVELS, SC_IDX_STATS_DAILY, SC_IDX_CONSTITUENT_DAILY, SC_IDX_CONTRIBUTION_DAILY, TECH11_AI_GOV_ETH_INDEX.

## Caching
The Django cache is configured as local memory and the query layer applies TTLs:
- Index levels + returns: 10 minutes
- Stats: 10 minutes
- Constituents + attribution: 5 minutes
- Latest trade date + bounds: 5-10 minutes

## Running locally on VM2
Ensure the same environment files as gunicorn:
```bash
cd /opt/code/Sustainacore/website_django
sudo systemd-run --quiet --wait --pty \
  --unit=vm2-tech100-runserver \
  --property=EnvironmentFile=/etc/sustainacore.env \
  --property=EnvironmentFile=/etc/sustainacore/db.env \
  /opt/sustainacore/website_django/venv/bin/python manage.py runserver 127.0.0.1:8001
```

Open:
- `http://127.0.0.1:8001/`
- `http://127.0.0.1:8001/tech100/index/`
- `http://127.0.0.1:8001/tech100/performance/`
- `http://127.0.0.1:8001/tech100/constituents/`
- `http://127.0.0.1:8001/tech100/attribution/`
- `http://127.0.0.1:8001/tech100/stats/`

## Tests
```bash
cd /opt/code/Sustainacore/website_django
/opt/code/Sustainacore/website_django/venv/bin/python manage.py check
/opt/code/Sustainacore/website_django/venv/bin/python -m pytest
```

## Screenshots
Requires Node.js and Playwright.
```bash
cd /opt/code/Sustainacore/website_django
npm install
npx playwright install --with-deps
node scripts/run_tech100_screenshots_ci.mjs
TECH100_SCREENSHOT_MODE=before node scripts/run_tech100_screenshots_ci.mjs
TECH100_SCREENSHOT_MODE=after TECH100_UI_DATA_MODE=fixture node scripts/run_tech100_screenshots_ci.mjs
TECH100_SCREENSHOT_MODE=after TECH100_UI_DATA_MODE=oracle node scripts/run_tech100_screenshots_ci.mjs
node scripts/tech100_screenshots.mjs --base-url http://127.0.0.1:8001 --mode before
node scripts/tech100_screenshots.mjs --base-url http://127.0.0.1:8001 --mode after
node scripts/tech100_screenshot_diff.mjs
```

Artifacts are stored in `docs/screenshots/tech100/{before,after,diff}`.

### Screenshot runner notes
- The runner picks a free port in the `8001..8050` range and starts `manage.py runserver --noreload`.
- Logs are written to `/tmp/tech100_runserver_<PORT>.log`.
- If a selector is missing, the runner prints a short HTML snippet for debugging.
- An Oracle smoke test runs before screenshots; output goes to `/tmp/tech100_oracle_smoke_<PORT>.log`.
- On HTTP 500, the response body is saved to `/tmp/tech100_500_body_<PORT>.html` and the run aborts.
- The runner fails if the Tech100 pages show empty-state banners or if charts/tables have no data markers.
- Use `TECH100_UI_DATA_MODE=fixture` to force deterministic data for screenshots while keeping Oracle smoke checks.
- Use `TECH100_UI_DATA_MODE=oracle` to validate screenshots against live Oracle data on VM2.
- You can force a specific Tech100 path with `TECH100_SCREENSHOT_PATH=/tech100/` or run only one mode with `TECH100_SCREENSHOT_MODE=before|after`.
- If readiness fails, the runner saves the probe response to `/tmp/tech100_readiness_body_<PORT>.txt`.
