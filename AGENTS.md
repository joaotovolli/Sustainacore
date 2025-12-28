SustainaCore — Autopilot rules for Codex.
- Keep /ask2 contract (q,k -> {answer,sources,meta})
- Build: python3 -m venv .venv && source .venv/bin/activate && pip -U pip wheel && pip -r requirements.txt && pytest -q || true
- Run: uvicorn app.retrieval.app:app --host 0.0.0.0 --port 8080
- Deploy: ops/scripts/deploy_vm.sh

Agents:
- vm1-esg-ai: scope esg_ai/**, index/**, oracle_scripts/**, target VM1. Must not modify website_django/**.
- vm2-website: scope website_django/**, target VM2. Must not modify ESG/Ask2 folders.

## Delivery & Verification Requirements
- Done means shipped: for user-facing changes, create a branch, commit, open a PR, and provide the PR URL.
- CI must be green before claiming completion.
- When a preview environment exists, verify there and provide evidence (status codes + rendered HTML grep). Use existing auth env vars; never print secrets.
- Evidence rules: never claim “confirmed” without concrete proof. For UI links, show rendered HTML (curl + grep). If preview curl fails with TLS error (exit code 60), fix CA certs or use Python requests + certifi; only use `curl -k` as a last resort with diagnostics.

## VRT Baseline Updates (VM2 UI)
- If VRT fails after global UI changes (header/footer/base layout/CSS), do NOT lower thresholds.
- Use the repo’s official update-baseline workflow and commit the baseline assets. See `docs/frontend_workflow_vm2.md`.
- If diffs persist due to non-determinism, stabilize output only for VRT mode; do not change production behavior.

## Iteration & Ports
- Keep iterating: diagnose → fix → re-run until acceptance criteria pass.
- Do not stop after the first failure unless blocked; list exactly what is missing.
- Treat `kill <pid>` returning “No such process” as non-fatal; prefer using a new free port.

## Screen Priority
- Desktop/laptop is the primary target.
- Improve mobile without harming desktop; never degrade desktop to “fix” mobile.

## Oracle Bootstrap Contract (VM1)
- Codex CLI shells may start with no DB_* envs; this is expected.
- Never source /etc/sustainacore/*.env or /etc/sustainacore-ai/*.env (not bash-safe).
- Always run `python3 tools/oracle/preflight_oracle.py` (or `python3 tools/test_db_connect.py`) before any Oracle task.
- Any new Oracle-facing script must call `load_env_files()` then use `db_helper.get_connection()`.
- If preflight fails: stop and report the error; do not attempt alternative drivers or wallet rewrites.
