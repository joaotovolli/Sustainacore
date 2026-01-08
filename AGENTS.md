SustainaCore — Autopilot rules for Codex.
<!-- cspell:ignore certifi noreload ionice sysconfig pycache -->
- Keep /ask2 contract (q,k -> {answer,sources,meta})
- Build: python3 -m venv .venv && source .venv/bin/activate && pip -U pip wheel && pip -r requirements.txt && pytest -q || true
- Run: uvicorn app.retrieval.app:app --host 0.0.0.0 --port 8080
- Deploy: ops/scripts/deploy_vm.sh

Agents:
- vm1-esg-ai: scope esg_ai/**, index/**, oracle_scripts/**, target VM1. Must not modify website_django/**.
- vm2-website: scope website_django/**, target VM2. Must not modify ESG/Ask2 folders.

## Repo Discovery (VM2)
Checklist (run in order):
- Identify the running service entrypoint first (systemd/gunicorn/nginx/docker).
- Extract WorkingDirectory/--chdir from `systemctl cat` or docker compose.
- Confirm by locating `manage.py` and `.git` before editing code.
- Only ask the user for paths after these steps fail; include evidence collected.

Recommended paths (non-binding):
- Prefer checkouts under `/srv/sustainacore` or `/opt/sustainacore`.
- Optional symlink: `/srv/app` -> actual repo path.
- `/home/ubuntu` paths are not guaranteed.

Diagnostics bundle (pasteable):
```bash
hostname; whoami; pwd
ps aux | egrep -i "gunicorn|uvicorn|django|manage.py"
systemctl list-units --type=service --state=running | egrep -i "gunicorn|uvicorn|django|sustain|web|app|nginx"
systemctl cat <service>
nginx -T | egrep -n "server_name|root |alias |proxy_pass|upstream|static|media|sustainacore"
docker ps  # if applicable
find /srv /opt /var/www -maxdepth 6 -name manage.py
```

If `.git` is missing at the working directory:
- Check sibling directories for a repo root (e.g., `/opt/code/Sustainacore`).
- Inspect deployment notes (e.g., `DEPLOY_CONFIG*.md`, systemd drop-ins).
- Search for repo URLs in configs or logs (do not print secrets).
- If a repo URL is found and access is available, clone into `/srv/sustainacore` or `/opt/sustainacore`.
- If no URL is found, summarize the evidence and ask for the repo location once.

Do not assume:
- Never hardcode a repo directory without verifying via service configuration first.

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

## VM1 Resource Constraints
- VM1 is resource constrained (1 CPU core, ~1 GB RAM).

## VM2 Resource Constraints
- VM2 is resource constrained (1 CPU core, ~1 GB RAM).
- VRT and headless browser runs can stall the VM; use GitHub Actions when possible.

## VM2 Oracle & Env File Guardrails
- VM2 production MUST use Oracle; sqlite is allowed only for local dev.
- Canonical env sources for VM2 Django:
  - `/etc/sustainacore.env`
  - `/etc/sustainacore/db.env`
  - `/etc/sysconfig/sustainacore-django.env`
- Do NOT `source` env files or parse them in shell; treat env files as data only.
- Never print env contents (no `cat /etc/*.env`, no `printenv`, no `env`, no `set -x`).
- Never use process substitution (`<(...)`) or `/dev/fd/*` for env files.
- Use `sudo -n systemd-run` with `EnvironmentFile=` for manage.py so the service env is applied without printing secrets.
- Canonical DB env var precedence in settings: `DB_USER/DB_PASSWORD/DB_DSN` → `ORACLE_USER/ORACLE_PASSWORD/ORACLE_DSN` → `ORACLE_CONNECT_STRING`.
- Golden verification command (must appear in PRs touching deploy/scripts/settings):
  - `scripts/vm2_manage.sh diagnose_db --fail-on-sqlite --verify-insert --timeout 60`
- Common failure modes to avoid:
  - Permission denied on `/etc/sustainacore.env` (fix via sudo -n access or group perms).
  - “Missing Oracle credentials” because service env differs from shell env (use systemd-run env files).
  - Migrations run against sqlite due to wrong env context (always run with service env).

## Python & Virtual Environment (VM2 / Django)
- On Ubuntu, `python` may be absent; always use `python3`.
- Always activate the repo virtual environment before Django commands:
  - If `.venv` exists: `source .venv/bin/activate`
  - Else create it: `python3 -m venv .venv && source .venv/bin/activate`
- Use `python -m pip ...` inside the venv (never system pip).
- Run Django commands as:
  - `DJANGO_SECRET_KEY=test python website_django/manage.py check`
  - `DJANGO_SECRET_KEY=test python website_django/manage.py runserver 127.0.0.1:<PORT> --noreload`
- Do not stop for “python: command not found”; switch to `python3` + venv and continue.

## Performance Guardrails (VM2)
- MUST run local Django servers with `python manage.py runserver ... --noreload`.
- Prefer running full VRT in GitHub Actions, not on VM2.
- If running VRT locally, use a single worker and throttle with `nice`/`ionice`.
- Never run `npm ci` unless required by a failing command.
- Avoid scanning huge trees (e.g., node_modules) in reload/watch modes.
- Always capture evidence before/after heavy tasks: `uptime`, `free -h`, `df -h`.

### Health Check Commands
```bash
free -h
uptime
vmstat 1 5
ps aux --sort=-%mem | head -n 15
dmesg -T | egrep -i "oom|out of memory|killed process" | tail -n 60
```

### Stop Conditions (VM2)
- If MemAvailable < 150MB or load average > 2.0 for >60s, stop heavy tasks and revert to GitHub-only verification.
- If SSH becomes unresponsive, do not keep retrying heavy commands.

## Oracle Bootstrap Contract (VM1)
- Codex CLI shells may start with no DB_* envs; this is expected.
- Never source /etc/sustainacore/*.env or /etc/sustainacore-ai/*.env (not bash-safe).
- Always run `python3 tools/oracle/preflight_oracle.py` (or `python3 tools/test_db_connect.py`) before any Oracle task.
- Any new Oracle-facing script must call `load_env_files()` then use `db_helper.get_connection()`.
- If preflight fails: stop and report the error; do not attempt alternative drivers or wallet rewrites.
- For SC_IDX price issues, use `tools/index_engine/backfill_prices.py` and see `docs/runbooks/price_ingest_and_backfill.md`.
- For SC_IDX index continuity issues, recompute with `tools/index_engine/calc_index.py --rebuild --no-preflight-self-heal` as documented in `docs/runbooks/price_ingest_and_backfill.md`.

## GitHub Hygiene: Commits & PRs
- Commit messages: imperative subject, <= 72 chars, no trailing period.
- Prefer logical commits for distinct changes; avoid noisy commits like "WIP", "fix", "temp".
- PR descriptions must be real Markdown (no escaped "\\n" sequences). Use blank lines between sections.
- Required PR sections and order:
  - "## Summary"
  - "## Changes"
  - "## Testing"
  - "## Evidence"
  - "## Notes / Follow-ups"
- Testing: include exact commands and outcomes.
- Evidence: include CI run link when CI exists; do not claim "verified" without outputs/links.

## Branch Hygiene & Push Policy (Pragmatic)
- One-active-branch rule: only one active remote branch per task. Before pushing a new branch, delete/close the previous remote branch unless it has an open PR.
- Always sanity-check diffs before any push. Must run and review:
  - `git status`
  - `git diff --stat`
  - `git diff --name-only | head`
  - `git diff --name-status | head`
- If the diff shows mass deletions, unexpected repo-wide changes, vendored folders (venv/, node_modules/, artifacts/, screenshots/, __pycache__/), or large binary files not requested, STOP and fix locally; do not push.
- Explicit denylist (never commit): venv/, .venv/, node_modules/, artifacts/, docs/screenshots/, playwright screenshots, dist/, build/, __pycache__/, *.pyc, *.sqlite3, *.db-journal, *.log, large tmp files.
- If any denylist items appear in `git status`, remove/move them or add minimal .gitignore entries (when appropriate).
- Push discipline: small WIP pushes are allowed only when the diff is small and scoped. No exploratory pushes touching hundreds of files.
- PR expectation: user-facing changes must open a PR. Docs-only changes should use a PR (required for this change). PR body must be clean Markdown (no literal "\\n") with Summary/Changes/Testing/Notes.
- Remote cleanup: delete remote branches after merge or if abandoned to keep branch list clean.
- Safety: never force-push unless explicitly instructed; if required, create a backup tag/branch and document impact.

### Definition of Done (PRs)
- PR includes what/why, testing, and a CI green link (if available).
- Do not claim completion without concrete outputs or links.

### Achievements (Optional, Professional)
- Do not chase achievements by spamming PRs or trivial changes.
- It is fine to earn achievements naturally by making meaningful changes and clear commits.
- Keep PR descriptions professional; avoid badge-farming language.

### Copy/paste PR body template
```markdown
## Summary
- ...

## Changes
- ...

## Testing
- `command --flags`
- `command --flags`

## Evidence
- CI run: <link>
- Screenshots/logs: <link or note>

## Notes / Follow-ups
- ...
```
