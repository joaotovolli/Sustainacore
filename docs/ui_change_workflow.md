# UI Change Workflow (Preview → PR → Production)

This is the canonical workflow for any UI-visible change. It is designed for
VM2 constraints (1GB RAM) and uses CI-only screenshot comparison.

## When to use this workflow
Use this for **any** change that impacts what users see on sustainacore.org
(templates, CSS, JS, layout, nav, footer, content presentation).

## Definitions
- Production: `https://sustainacore.org/`
- Preview (Basic Auth): `https://preview.sustainacore.org/`
- CI artifacts: the only trusted feedback loop for UI diffs

## Non-negotiable rules
- MUST use a PR; never change production directly.
- MUST use CI artifacts for validation; do not run Playwright locally on VM2.
- MUST keep preview Basic Auth credentials in GitHub Secrets only.
- MUST request human approval; the agent never merges.

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

4) Wait for CI **UI Screenshot Compare (Home)** to finish.

5) Review artifacts:
- PR → Checks → UI Screenshot Compare (Home) → Artifacts
- Files:
  - `before/home.png`
  - `after/home.png`
  - `diff/home_diff.png`
  - `report/ui_compare_report.json`
  - `report/ui_compare_summary.txt`

6) Iterate until diffs are acceptable.

7) Request human approval (agent never merges).

## Secrets setup (GitHub)
Required GitHub Secrets:
- `PREVIEW_BASIC_AUTH_USER`
- `PREVIEW_BASIC_AUTH_PASS`

These are low-risk but MUST remain secrets and never be committed or logged.

## Approval & merge policy
- Humans approve and merge.
- If production regresses after merge, revert the PR.

## Never lose this again
- This workflow is required and referenced in `AGENTS.md`.
- PR checklist enforces preview links and artifact review.
- CI artifacts are the primary review evidence.

## Links
- `AGENTS.md` UI Change Contract
- `docs/ui_screenshot_process.md`
