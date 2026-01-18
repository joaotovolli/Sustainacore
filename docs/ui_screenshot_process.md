# UI Screenshot Compare Process (CI-only)
<!-- cspell:ignore mismatchPercent ui-home-compare workflow_dispatch -->

## What CI does
- Captures **home page** screenshots from:
  - Production: `https://sustainacore.org/`
  - Preview (public): `https://preview.sustainacore.org/`
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
- Diff is within threshold (see report `diff.mismatchPixels` / `diff.mismatchPercent`).

## What “diff” means
- Any mismatch indicates a UI change between prod and preview.
- Review the diff image and the report; iterate until acceptable.

## How to iterate (agent)
1) Download artifacts:
```
gh run download <run_id> -n ui-home-compare
```
2) Inspect `report/ui_compare_report.json`:
   - `diff.mismatchPixels`, `diff.mismatchPercent`
   - `layout.before`/`layout.after` and `overflowOffendersTop15`
3) Commit snapshots PR244-style:
```
docs/screenshots/ui/home/pr-<PR>/run-<RUN_ID>/...
```
4) Fix preview, push, repeat until diff acceptable.
5) Poll PR checks using the fast script (avoid long-running `--watch`):
```
tools/ci/poll_pr_checks.sh <pr-number>
```

## Why CI-only
VM2 is 1GB RAM and cannot reliably run Playwright. All UI compare runs happen in
GitHub Actions.

## Manual runs (workflow_dispatch)
The workflow supports manual runs to generate artifacts even without a UI PR:
```
gh workflow run ui_compare_home.yml -R joaotovolli/Sustainacore --ref <branch>
```
Then download artifacts and commit snapshots into the PR branch.
