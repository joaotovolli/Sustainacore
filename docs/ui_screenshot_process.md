# UI Screenshot Compare Process (CI-only)

## What CI does
- Captures **home page** screenshots from:
  - Production: `https://sustainacore.org/`
  - Preview: `https://preview.sustainacore.org/`
- Generates a pixel diff.
- Writes a report with diff stats and overflow offenders.

Artifacts (CI) under `artifacts/ui/`:
- `before/home.png`
- `after/home.png`
- `diff/home_diff.png`
- `report/ui_compare_report.json`
- `report/ui_compare_summary.txt`

## What “pass” means
- Workflow completes.
- Diff is within threshold (see report `diff.mismatchPixels`).

## What “diff” means
- Any mismatch indicates a UI change between prod and preview.
- Review the diff image and the report; iterate until acceptable.

## How to iterate (agent)
1) Download artifacts:
```
gh run download <run_id> -n ui-home-compare
```
2) Inspect `report/ui_compare_report.json`:
   - `diffPixels` and `diffPct`
   - `overflowOffendersTop15` for layout issues
3) Fix preview, push, repeat until diff acceptable.

## Why CI-only
VM2 is 1GB RAM and cannot reliably run Playwright. All UI compare runs happen in
GitHub Actions.
