# AI regulation globe growth fix evidence

## Summary
- Issue: globe/map height continuously expanded, likely due to resize feedback loop between canvas sizing and auto-height container.
- Fix: set a deterministic container height via CSS and debounce ResizeObserver updates with size change guards.

## Commands
- Before (main, existing server):
  - `AI_REG_SCREENSHOT_MODE=before AI_REG_USE_EXISTING_SERVER=1 AI_REG_BASE_URL=http://127.0.0.1 node scripts/run_ai_reg_screenshots_ci.mjs`
- After (branch, manual runserver on 127.0.0.1:8001):
  - `sudo systemd-run --collect --unit vm2-ai-reg-runserver-growthfix --property EnvironmentFile=/etc/sustainacore.env --property EnvironmentFile=/etc/sustainacore/db.env --property EnvironmentFile=-/etc/sysconfig/sustainacore-django.env --property EnvironmentFile=/tmp/ai_reg_env_override_manual.env --property WorkingDirectory=/opt/code/Sustainacore/website_django /home/ubuntu/.venvs/sustainacore_vm2/bin/python manage.py runserver 127.0.0.1:8001 --noreload`
  - `AI_REG_SCREENSHOT_MODE=after AI_REG_USE_EXISTING_SERVER=1 AI_REG_BASE_URL=http://127.0.0.1:8001 node scripts/run_ai_reg_screenshots_ci.mjs`
- Diff:
  - `node scripts/ai_reg_screenshot_diff.mjs`

## Artifacts (stored outside repo per denylist)
- Before desktop: `/tmp/ai_reg_growth_fix/ai_reg/before/ai_regulation_desktop.png`
- Before mobile: `/tmp/ai_reg_growth_fix/ai_reg/before/ai_regulation_mobile.png`
- After desktop: `/tmp/ai_reg_growth_fix/ai_reg/after/ai_regulation_desktop.png`
- After mobile: `/tmp/ai_reg_growth_fix/ai_reg/after/ai_regulation_mobile.png`
- Diff desktop: `/tmp/ai_reg_growth_fix/ai_reg/diff/diff_ai_regulation_desktop.png`
- Diff mobile: `/tmp/ai_reg_growth_fix/ai_reg/diff/diff_ai_regulation_mobile.png`

## Notes
- After screenshots used a local runserver to serve updated static assets on `http://127.0.0.1:8001`.
