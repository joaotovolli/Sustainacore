# CI overview

This repository runs a focused set of GitHub Actions workflows to keep security and quality checks strong while reducing PR noise.

## Workflows

- **CI (`.github/workflows/ci.yml`)**
  - Jobs: `Ensure sensitive files are ignored`, `Persona guard (manual eval lives in persona-eval)`.
  - Behavior: enforces that `.env` and `wallet/**` files are not tracked. Persona guard only posts guidance; full persona evaluation is manual.

- **Hygiene (`.github/workflows/hygiene.yml`)**
  - Jobs: `spellcheck` (cspell against Markdown).
  - Triggers: PRs touching docs/templates and manual dispatch.

- **Persona eval (`.github/workflows/persona-eval.yml`)**
  - Jobs: PR/push display a manual-only skip summary. Manual dispatch runs preflight checks and `scripts/run_eval.py` when `ASK2_URL`, `OPENAI_API_KEY`, and `ORG_ID` are set.

- **Sanity (`.github/workflows/sanity.yml`)**
  - Jobs: `sanity` (conflict marker check, dependency install, `compileall`, and `pytest`).
  - Triggers: PRs that touch Python, shell, workflow, or app/service code paths; manual dispatch always runs.

- **Workflow Lint (`.github/workflows/workflow-lint.yml`)**
  - Jobs: `lint` (actionlint + yamllint across workflows). Always runs on PRs and pushes to `main`.

- **Canary & Deploy (`canary.yml`, `deploy.yml`, `deploy_vm2_website.yml`)**
  - Deployment health checks and SSH-based deploys with path filtering. Left unchanged by this refinement.

## Recommended required checks

Mark these as required in branch protection for predictable gates:
- **CI / Ensure sensitive files are ignored** (protects against committed secrets).
- **Sanity / sanity** (compile + pytest on relevant code changes).
- **Workflow Lint / lint** (ensures GitHub Actions definitions are valid).

Keep the following advisory/manual:
- **Hygiene / spellcheck** (only runs on doc/template changes).
- **Persona eval (manual-only)** jobs (guidance or manual dispatch results).
- Deployment-related workflows (triggered on main or manual as configured).

## Running checks locally

- **Spellcheck:** `npx cspell --no-progress "**/*.md"`
- **Sanity tests:**
  ```bash
  python -m pip install -U pip wheel
  pip install -r requirements.txt
  pip install fastapi uvicorn pytest
  python -m compileall -q .
  pytest -q
  ```
- **Persona eval (manual):** ensure `ASK2_URL`, `OPENAI_API_KEY`, and `ORG_ID` are set, then run `python scripts/run_eval.py` against a reachable Ask2 endpoint. In CI, trigger via **Actions → Persona eval → Run workflow**.
