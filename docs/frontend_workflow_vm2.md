# Frontend Workflow (VM2) — UI Quality Gate

This page is the authoritative workflow for UI changes on VM2 (Django + Playwright).
Follow it for every frontend change to ensure consistent validation and review artifacts.

## Environments
- Production: `https://sustainacore.org`
- Preview: `https://preview.sustainacore.org` (Basic Auth, noindex, PREVIEW banner)
- Local automation: `http://127.0.0.1` (Nginx/Gunicorn on VM2)
- Runserver (branch-only): `http://127.0.0.1:<port>` (used when new routes exist)

Why `127.0.0.1` is not clickable: it resolves to the local machine. Use `preview.sustainacore.org`
for external review and screenshots from your laptop.

## Golden Path (UI Quality Gate)
1) Create a branch:
```bash
git checkout -b feature/<your-ui-change>
```

2) Install dependencies (first time only):
```bash
cd /opt/code/Sustainacore/website_django
npm install
npx playwright install --with-deps
```

3) Run targeted Django tests:
```bash
DJANGO_SECRET_KEY=test ./venv/bin/python manage.py test core.tests.test_tech100_index_data core.tests.test_tech100_index_views core.tests.test_tech100_views
node --test scripts/tech100_url_candidates.test.mjs
```

4) Capture **BEFORE** screenshots on main (use existing Nginx/Gunicorn):
```bash
git checkout main
TECH100_SCREENSHOT_MODE=before \
TECH100_USE_EXISTING_SERVER=1 \
TECH100_BASE_URL=http://127.0.0.1 \
node scripts/run_tech100_screenshots_ci.mjs
```

5) Capture **AFTER** screenshots on your branch (fixture mode for determinism):
```bash
git checkout feature/<your-ui-change>
TECH100_SCREENSHOT_MODE=after TECH100_UI_DATA_MODE=fixture node scripts/run_tech100_screenshots_ci.mjs
```

6) If Oracle data is expected, run once in Oracle-backed mode:
```bash
TECH100_SCREENSHOT_MODE=after TECH100_UI_DATA_MODE=oracle node scripts/run_tech100_screenshots_ci.mjs
```

7) Iterate until PASS, then open PR with artifacts and preview link.

## PR Gate (Home Only)
PRs run a lightweight home-page screenshot compare (prod vs preview) in GitHub Actions.
Workflow: `.github/workflows/ui_screenshot_compare.yml`.

Required secrets (names only):
- `PREVIEW_BASIC_AUTH_USER`
- `PREVIEW_BASIC_AUTH_PASS`

Artifacts:
- `ui-home-compare` (before/after/diff under `artifacts/ui_home/`)
- `artifacts/ui_home/report/ui_compare_report.json`
- `artifacts/ui_home/report/ui_compare_summary.txt`

## Fixture mode vs Oracle-backed mode
- Fixture mode (deterministic, recommended for screenshots):
  - `TECH100_UI_DATA_MODE=fixture`
- Oracle-backed mode (real data):
  - `TECH100_UI_DATA_MODE=oracle` or omit if default is Oracle

## Existing server mode (baseline reliability)
To avoid transient runserver issues for baseline screenshots:
- Set `TECH100_USE_EXISTING_SERVER=1`
- Set `TECH100_BASE_URL=http://127.0.0.1`

This uses Nginx/Gunicorn instead of spawning a runserver unit.

## Preview screenshots (clickable review)
Use preview with Basic Auth for external review:
```bash
TECH100_SCREENSHOT_DIR=preview \
TECH100_BASE_URL=https://preview.sustainacore.org \
TECH100_BASIC_AUTH_USER=<<<USER>>> \
TECH100_BASIC_AUTH_PASS=<<<PASS>>> \
TECH100_SCREENSHOT_MODE=after \
node scripts/run_tech100_screenshots_ci.mjs
```

Artifacts go to: `docs/screenshots/preview/{before,after,diff}`.

## UI Quality Gate — PASS/FAIL
### Automated (Playwright)
- Data presence checks:
  - Charts have >= 10 points
  - Tables have >= 5 rows
  - No “empty state” banners
- Tech100 route auto-discovery:
  - Uses `website_django/scripts/tech100_url_candidates.mjs`

### Manual review (required)
- Visual sanity:
  - No overlap, clipping, or broken grid alignment
  - Typography consistent, no missing fonts
  - Mobile view not broken (check 375x812 or use browser responsive mode)
- Functional sanity:
  - Primary buttons clickable
  - Range toggles update charts
  - Filters change table results

**Policy:** If any automated or manual checks fail, stop and iterate until PASS.

## Artifacts & output
- Screenshots:
  - `docs/screenshots/<area>/{before,after,diff}`
- Logs:
  - Runserver: `/tmp/tech100_runserver_<port>.log`
  - Oracle smoke: `/tmp/tech100_oracle_smoke_<port>.log`
  - Readiness: `/tmp/tech100_readiness_body_<port>.txt`
  - 500 body: `/tmp/tech100_500_body_<port>.html`
  - Failure body: `/tmp/tech100_failure_body_<port>.html`

## Script reference (current)
- `website_django/scripts/run_tech100_screenshots_ci.mjs`
- `website_django/scripts/tech100_screenshots.mjs`
- `website_django/scripts/tech100_screenshot_diff.mjs`
- `website_django/scripts/tech100_url_candidates.mjs`

NPM aliases (from `website_django/package.json`):
- `npm run screenshots:tech100:before`
- `npm run screenshots:tech100:after`
- `npm run screenshots:tech100:diff`
- `npm run screenshots:tech100:ci`

## PR template snippet (copy/paste)
```
## Summary
- <what changed>

## Preview
- https://preview.sustainacore.org (Basic Auth: <<<USER>>> / <<<PASS>>>)

## Screenshots
- Before: docs/screenshots/<area>/before/...
- After: docs/screenshots/<area>/after/...
- Diff: docs/screenshots/<area>/diff/...

## Tests
<commands run + results>

## Notes
- Fixture vs Oracle data:
- Known limitations:
```

## Troubleshooting
- Baseline ready check times out: confirm Nginx/Gunicorn is running on VM2 and use `TECH100_USE_EXISTING_SERVER=1`.
- Port in use: rerun `node scripts/run_tech100_screenshots_ci.mjs` (auto-picks free port).
- 404 path: check `tech100_url_candidates.mjs` and confirm URL patterns.
- 500 errors: ensure `DJANGO_DEBUG=1` during runs; see `/tmp/tech100_500_body_<port>.html`.
- Oracle issues: verify wallet perms and `TNS_ADMIN`; check smoke log in `/tmp/tech100_oracle_smoke_<port>.log`.
- Basic Auth: verify credentials in `TECH100_BASIC_AUTH_USER/PASS` or `/etc/nginx/.htpasswd_preview_sustainacore`.
- “No data available” in UI: run fixture mode and confirm Oracle connectivity separately.
