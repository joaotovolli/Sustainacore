# UI Change Workflow (CI Screenshot Compare)

This workflow is the canonical UI validation gate for Sustainacore PRs. It replaces
local Playwright runs on VM2 (1GB RAM) with a lightweight CI-only compare.

## PR Gate (Home Only)
- Workflow: `.github/workflows/ui_screenshot_compare.yml`
- Scope: home page `/` only, single viewport 1440x900
- Comparison: Production vs Preview
  - Production: `https://sustainacore.org/`
  - Preview: `https://preview.sustainacore.org/`

## Required GitHub Secrets
- `PREVIEW_BASIC_AUTH_USER`
- `PREVIEW_BASIC_AUTH_PASS`

## Artifacts
The workflow uploads `ui-home-compare` containing:
- `artifacts/ui_home/before/home.png`
- `artifacts/ui_home/after/home.png`
- `artifacts/ui_home/diff/home_diff.png`
- `artifacts/ui_home/report/ui_compare_report.json`
- `artifacts/ui_home/report/ui_compare_summary.txt`

## CI Job Summary
The job summary includes:
- Production link
- Preview link
- Artifact name
- Diff percent and overflow offenders (if any)

## PR Body Template
```
## Summary
- ...

## Changes
- ...

## Testing
- CI only (ui_screenshot_compare)

## Evidence
- Preview: https://preview.sustainacore.org/
- Production: https://sustainacore.org/
- Actions artifacts: ui-home-compare

## Notes / Follow-ups
- ...
```

## Troubleshooting
- Secrets missing: workflow fails with a clear message. Set the two preview secrets.
- Preview TLS: ensure `https://preview.sustainacore.org/` is valid and reachable.
- VM2 constraints: do not run Playwright locally; rely on CI artifacts.
