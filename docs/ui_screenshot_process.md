# UI Screenshot Compare Process (VM-first)
<!-- cspell:ignore mismatchPercent ui-home-compare workflow_dispatch -->

## What the local script does (primary evidence)
- Captures **home page** screenshots from:
  - Production: `https://sustainacore.org/`
  - Preview (public): `https://preview.sustainacore.org/`
- Generates a pixel diff and summary.
- Writes evidence for PR244-style review.

Local outputs (minimal evidence bundle):
- `docs/screenshots/ui/home/pr-<PR>/run-<RUN_ID>/before/*.png`
- `docs/screenshots/ui/home/pr-<PR>/run-<RUN_ID>/after/*.png`
- `docs/screenshots/ui/home/pr-<PR>/run-<RUN_ID>/diff/*_diff.png`
- `docs/screenshots/ui/home/pr-<PR>/run-<RUN_ID>/report/ui_compare_report.json`
- `docs/screenshots/ui/home/pr-<PR>/run-<RUN_ID>/report/ui_compare_summary.txt`

## What CI does (informational)
- Captures **home page** screenshots from:
  - Production: `https://sustainacore.org/`
  - Preview (public): `https://preview.sustainacore.org/`
- Generates a pixel diff.
- Writes a report with diff stats and overflow offenders.

Artifacts (CI) under `artifacts/ui/`:
- `before/home_full.png`
- `before/home_top.png`
- `before/home_mid.png`
- `before/home_footer.png`
- `before/home_mobile.png`
- `after/home_full.png`
- `after/home_top.png`
- `after/home_mid.png`
- `after/home_footer.png`
- `after/home_mobile.png`
- `diff/home_full_diff.png`
- `diff/home_top_diff.png`
- `diff/home_mid_diff.png`
- `diff/home_footer_diff.png`
- `diff/home_mobile_diff.png`
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
   - `diff.mismatchPixels`, `diff.mismatchPercent` (max across shots)
   - `shots[]` entries for per-shot stats
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

## Why VM-first
Human approval relies on a minimal, curated evidence bundle in the PR itself.
CI provides additional context but does not block merges.

## Manual runs (workflow_dispatch)
The workflow supports manual runs to generate artifacts even without a UI PR:
```
gh workflow run ui_compare_home.yml -R joaotovolli/Sustainacore --ref <branch>
```
Then download artifacts and commit snapshots into the PR branch.

## Local run (VM2) — primary evidence
Use the local compare script with bounded timeouts (fast mode for VM2):
```
cd /opt/code/Sustainacore/website_django
PROD_BASE_URL=https://sustainacore.org \
PREVIEW_BASE_URL=https://preview.sustainacore.org \
TIMEOUT_MS=15000 \
LOCAL_FAST=1 \
LOCAL_FETCH_HTML=1 \
OUTPUT_DIR=/tmp/ui-compare-local \
node scripts/ui_compare_home.mjs
```
Then copy a minimal set into the PR evidence path:
```
docs/screenshots/ui/home/pr-<PR>/run-<RUN_ID>/{before,after,diff,report}
```

## VM cleanup (required)
After evidence is committed, remove temporary artifacts:
```
find /tmp/ui-compare-local* -type f -delete
rmdir /tmp/ui-compare-local* 2>/dev/null || true
rm -f /tmp/ui_compare_local*.log
```
