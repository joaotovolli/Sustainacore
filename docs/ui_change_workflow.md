# UI Change Workflow (Preview → PR → Production)
<!-- cspell:ignore workflow_dispatch ui-home-compare -->

This is the canonical workflow for any UI-visible change. It prioritizes a
VM2-first iteration loop and publishes minimal, human-reviewable evidence in
the PR (PR244-style).

## When to use this workflow
Use this for **any** change that impacts what users see on sustainacore.org
(templates, CSS, JS, layout, nav, footer, content presentation).

## Definitions
- Production: `https://sustainacore.org/`
- Preview (public): `https://preview.sustainacore.org/`
- CI artifacts: informational only (not a hard gate)

## Non-negotiable rules
- MUST use a PR; never change production directly.
- MUST iterate on VM2 preview first, then publish local evidence for review.
- MUST keep preview public; no Basic Auth required.
- MUST request human approval; the agent never merges.
- PR #244 screenshot-compare contract is canonical for UI changes.

## Step-by-step (agent)
1) Create a branch:
```
git checkout -b feature/<ui-change>
```

2) Make the UI change and commit.

3) Push and open PR:
```
git push -u origin feature/<ui-change>
gh pr create --fill
```

4) Ensure preview reflects the PR branch (not main). Confirm the build marker:
`<!-- build: <short-sha> -->`.

5) Capture local evidence (prod vs preview) using the local compare script:
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

6) Review local artifacts under the evidence directory (see below).

7) Commit PR244-style snapshots into the PR branch:
```
docs/screenshots/ui/home/pr-<PR>/run-<RUN_ID>/before/home.png
docs/screenshots/ui/home/pr-<PR>/run-<RUN_ID>/after/home.png
docs/screenshots/ui/home/pr-<PR>/run-<RUN_ID>/diff/home_diff.png
docs/screenshots/ui/home/pr-<PR>/run-<RUN_ID>/report/ui_compare_report.json
docs/screenshots/ui/home/pr-<PR>/run-<RUN_ID>/report/ui_compare_summary.txt
```

8) Update PR body with:
- Preview + production links
- Embedded before/after/diff images
- CI run link + `gh run download` command (informational)

9) Iterate locally until diffs are acceptable, then keep CI as informational.

10) Request human approval (agent never merges).

## Approval & merge policy
- Humans approve and merge.
- If production regresses after merge, revert the PR.

## Never lose this again
- This workflow is required and referenced in `AGENTS.md`.
- PR checklist enforces preview links and artifact review.
- Local evidence is the primary review evidence; CI artifacts remain informational.

## Minimum viable loop (fast)
1) Push change → open PR.
2) Update preview on VM2; confirm build marker.
3) Run local compare script and commit snapshots to `docs/screenshots/ui/home/pr-<PR>/run-<RUN_ID>/...`.
4) Update PR description with links + images + run link.
5) Iterate until acceptable, then request human approval.

## Links
- `AGENTS.md` UI Change Contract
- `docs/ui_screenshot_process.md`
