# Frontend Screenshot Compare Workflow

This document defines the canonical screenshot-compare process for VM2 UI changes.
It replaces the old Mobile VRT hard gate with deterministic before/after/diff screenshots.

## Local workflow (VM2)
Run from `website_django` so Node resolves `node_modules` correctly.

```bash
cd /opt/code/Sustainacore/website_django

# Start local server in fixture mode (separate terminal)
source /opt/code/Sustainacore/.venv/bin/activate
DJANGO_SECRET_KEY=test DJANGO_DEBUG=1 \
TECH100_UI_DATA_MODE=fixture NEWS_UI_DATA_MODE=fixture AI_REG_UI_DATA_MODE=fixture \
python manage.py runserver 127.0.0.1:8042 --noreload

# Run screenshot compare (fast)
SCREENSHOT_MODE=compare FAST=1 PAGES=home,tech100 \
SCREENSHOT_DIR=preview USE_EXISTING_SERVER=1 BASE_URL=http://127.0.0.1:8042 \
node scripts/run_preview_screenshots.mjs
```

Outputs:
- `docs/screenshots/preview/before/*.png`
- `docs/screenshots/preview/after/*.png`
- `docs/screenshots/preview/diff/diff_*.png`

## Preview TLS check
Preview comparisons require valid TLS for `preview.sustainacore.org`.
Verify SAN contains the preview host:

```bash
openssl s_client -connect preview.sustainacore.org:443 -servername preview.sustainacore.org </dev/null 2>/dev/null \
  | openssl x509 -noout -subject -issuer -dates -ext subjectAltName
```

Temporary workaround (local only):

```bash
SCREENSHOT_IGNORE_HTTPS_ERRORS=1 node scripts/run_preview_screenshots.mjs
```

## CI (PR gate)
CI runs a home-only screenshot compare job and uploads artifacts.
Workflow: `.github/workflows/ui_screenshot_compare.yml`.

Secrets required (names only):
- `PREVIEW_BASIC_AUTH_USER`
- `PREVIEW_BASIC_AUTH_PASS`

Artifacts:
- `before.png`, `after.png`, `diff.png` in the `ui-screenshot-compare-home` artifact.

To expand later:
- add pages and viewports in `website_django/scripts/screenshot_compare_onepage.mjs`
- update the workflow inputs accordingly.

## PR checklist (required)
- Preview links: `https://preview.sustainacore.org/...`
- Screenshot paths (before/after/diff) from `docs/screenshots/preview`
- Commands run (copy/paste exact commands)

## Notes
- The script prints progress logs + heartbeat to avoid silent timeouts.
- Per-page failures are collected and reported at the end; the run exits non-zero.

CI trigger: 2026-01-16
