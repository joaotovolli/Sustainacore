# UI Screenshot Process (PR244 Contract)

This document captures the PR244-style UI iteration loop and makes it the canonical
workflow for SustainaCore UI changes. It replaces local Playwright runs on VM2
(1GB RAM) with CI-only screenshot comparison against preview.

## PR244 Contract (Reference)
From PR #244 (Dec 21, 2025):
- Capture BEFORE/AFTER screenshots and generate DIFFs.
- Store artifacts under `docs/screenshots/tech100/{before,after,diff}`.
- Use scripts:
  - `website_django/scripts/run_tech100_screenshots_ci.mjs`
  - `website_django/scripts/tech100_screenshot_diff.mjs`
- Include preview links in the PR body.
- Pages captured in PR244:
  - `/`
  - `/tech100/index/`
  - `/tech100/performance/`
  - `/tech100/constituents/`
  - `/tech100/attribution/`
  - `/tech100/stats/`
  - `/tech100/`

## Canonical CI Gate (Home Only)
The new required PR gate is a CI-only compare that captures **home** only:
- Workflow: `.github/workflows/ui_compare_home.yml`
- Script: `website_django/scripts/ui_compare_home.mjs`
- Production: `https://sustainacore.org/`
- Preview: `https://preview.sustainacore.org/`

Artifacts (uploaded from CI runner, not committed):
- `artifacts/ui_home/before/home.png`
- `artifacts/ui_home/after/home.png`
- `artifacts/ui_home/diff/home_diff.png`
- `artifacts/ui_home/report/ui_compare_report.json`
- `artifacts/ui_home/report/ui_compare_summary.txt`

## Required GitHub Secrets
- `PREVIEW_BASIC_AUTH_USER`
- `PREVIEW_BASIC_AUTH_PASS`

## Iteration Loop (Codex)
1) Open PR.
2) CI runs UI compare and uploads artifacts.
3) Download artifacts:
   - `gh run download <run_id> -n ui-home-compare`
4) Review `report/ui_compare_report.json` + diff image.
5) Fix preview, push commit, repeat until diffs are acceptable.

## Job Summary Requirements
The workflow writes a job summary with:
- Preview link and production link
- Artifact name
- Diff pixel count and percent

## Why CI-only
Playwright is not reliable on VM2 (1GB RAM). The CI runner is the only supported
execution environment for screenshot compare.
