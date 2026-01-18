# UI Change Workflow (Preview → PR → Production)
<!-- cspell:ignore workflow_dispatch ui-home-compare -->

This is the canonical workflow for any UI-visible change. It is designed for
VM2 constraints (1GB RAM) and uses CI-only screenshot comparison.

## When to use this workflow
Use this for **any** change that impacts what users see on sustainacore.org
(templates, CSS, JS, layout, nav, footer, content presentation).

## Definitions
- Production: `https://sustainacore.org/`
- Preview (public): `https://preview.sustainacore.org/`
- CI artifacts: the only trusted feedback loop for UI diffs

## Non-negotiable rules
- MUST use a PR; never change production directly.
- MUST use CI artifacts for validation; do not run Playwright locally on VM2.
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

4) Ensure preview reflects the PR branch (not main). The preview deploy workflow validates the build marker:
`<!-- build: <short-sha> -->`.

5) Wait for CI **UI Screenshot Compare (Home)** to finish.

6) Review artifacts:
- PR → Checks → UI Screenshot Compare (Home) → Artifacts
- Files:
  - `before/home.png`
  - `after/home.png`
  - `diff/home_diff.png`
  - `report/ui_compare_report.json`
  - `report/ui_compare_summary.txt`

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
- CI run link + `gh run download` command

9) Iterate until diffs are acceptable.

10) Request human approval (agent never merges).

## Approval & merge policy
- Humans approve and merge.
- If production regresses after merge, revert the PR.

## Never lose this again
- This workflow is required and referenced in `AGENTS.md`.
- PR checklist enforces preview links and artifact review.
- CI artifacts are the primary review evidence.

## Minimum viable loop (fast)
1) Push change → open PR.
2) Trigger/confirm UI compare run (`workflow_dispatch` is available).
3) Download artifacts and commit snapshots to `docs/screenshots/ui/home/pr-<PR>/run-<RUN_ID>/...`.
4) Update PR description with links + images + run link.
5) Iterate until acceptable, then request human approval.

## Links
- `AGENTS.md` UI Change Contract
- `docs/ui_screenshot_process.md`
