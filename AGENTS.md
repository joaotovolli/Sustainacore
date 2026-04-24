SustainaCore — Autopilot rules for Codex.
<!-- cspell:ignore certifi chdir LOOKBACK noreload ionice readlink sysconfig pycache -->
- Keep /ask2 contract (q,k -> {answer,sources,meta})
- Build: python3 -m venv .venv && source .venv/bin/activate && pip -U pip wheel && pip -r requirements.txt && pytest -q || true
- Run: uvicorn app.retrieval.app:app --host 0.0.0.0 --port 8080
- Deploy: ops/scripts/deploy_vm.sh

## WSL2-First Development (Django)
Primary workflow:
WSL2 (local dev + tests + screenshots) → GitHub PR (CI) → deploy to VM2/VM1.

Local preview (fixtures default for WSL2):
```bash
bash scripts/dev/run_django.sh
```

Local checks:
```bash
bash scripts/dev/run_tests.sh
python scripts/dev/preview_verify.py --timeout 15
```

Screenshots (local/prod):
```bash
cd website_django
../scripts/dev/capture_screenshots.sh ../local_artifacts/url_list_<timestamp>.json \
  ../local_artifacts/screenshots_<timestamp>
```

SSH smoke (vm1/vm2 aliases supported):
```bash
VM1_HOST=<vm1_host> VM1_USER=<vm1_user> VM2_HOST=<vm2_host> VM2_USER=<vm2_user> \
  [SSH_KEY_PATH=~/.ssh/<key>] bash scripts/dev/ssh_smoke.sh
```

Oracle smoke (optional, local-only env):
```bash
source .venv/bin/activate
python scripts/dev/oracle_smoke.py
```

Secret handling (public repo):
- Never commit or print secrets (.env contents, keys, wallets, tokens).
- Keep `local_artifacts/` local only (gitignored).
- Use local env vars for Oracle and SSH; document paths without printing contents.

Deploy targets:
- VM2 for Django website; VM1 for data/index/Ask2 backends.
- Deploy via `scripts/deploy/deploy_vm2.sh` and `scripts/deploy/deploy_vm1.sh`.

## UI Change Contract (Preview → Approval → Production)
Definitions:
- Production: `https://sustainacore.org/`
- Preview (public): `https://preview.sustainacore.org/`
- CI: GitHub Actions (informational artifacts only; not a hard gate)
- VRT: Visual regression testing (manual/scheduled only; not a PR gate)

Non-negotiable rules:
- NEVER change production directly for UI work; all changes go through PR review.
- NEVER disable required checks unless explicitly directed by the repo owner.
- NEVER commit secrets; preview is public (no Basic Auth).
- ALWAYS iterate on VM2 preview first, then publish minimal evidence for human approval.
- Playwright runs are allowed on VM2 for evidence capture; keep runs small and bounded.
- PR #244 screenshot-compare contract is canonical (before/after/diff + preview links).
- One UI PR at a time. If the target PR is merged/closed, create a new branch + PR.
- Preflight PR check before any UI work:
  - `gh pr view <PR> --json state,mergedAt`
  - If not OPEN, do not reuse it.

Required agent loop:
A) Open PR
B) Confirm PR is OPEN before doing any work:
   - `gh pr view <PR> --json state,mergedAt`
   - If `state` is CLOSED/MERGED, create a new branch + PR (do not reuse merged PRs).
C) Update preview on VM2 and iterate locally until "good enough"
D) Capture local evidence (prod vs preview) and commit minimal artifacts
E) Use CI compare as informational, not a merge gate
F) Request human approval (agent never merges)

Evidence requirements for UI PRs:
- Include preview + production links in PR body.
- Commit local snapshots PR244-style under:
  `docs/screenshots/ui/home/pr-<PR>/run-<RUN_ID>/{before,after,diff,report}`
- Embed before/after/diff images in PR description and include the CI run link + download command.

Docs:
- `docs/ui_change_workflow.md` (canonical UI change workflow)
- `docs/ui_screenshot_process.md` (UI compare process + artifact interpretation)
- PR checklist in `.github/PULL_REQUEST_TEMPLATE.md`

## Agent Autonomy Rules (UI)
- Use VM2 preview as the primary iteration loop; CI artifacts are informational.
- If a step fails, change strategy (shorter commands, different endpoint, or CI logs).
- If preview/prod HTTP is flaky from VM2, capture evidence locally and document it.
- Avoid long-running commands; prefer short polls using `tools/ci/poll_pr_checks.sh`.

Agents:
- vm1-esg-ai: scope esg_ai/**, index/**, oracle_scripts/**, target VM1. Must not modify website_django/**.
- vm2-website: scope website_django/**, target VM2. Must not modify ESG/Ask2 folders.

## Repo Discovery (VM2)
Checklist (run in order):
- Identify the running service entrypoint first (systemd/gunicorn/nginx/docker).
- Extract WorkingDirectory or the change-directory flag (`--chdir`) from `systemctl cat` or docker compose.
- Confirm by locating `manage.py` and `.git` before editing code.
- Only ask the user for paths after these steps fail; include evidence collected.

Recommended paths (non-binding):
- Prefer checkouts under `/srv/sustainacore` or `/opt/sustainacore`.
- Optional symlink: `/srv/app` -> actual repo path.
- `/home/ubuntu` paths are not guaranteed.

Diagnostics bundle (copy-ready):
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
- When a preview environment exists, verify there and provide evidence (status codes + rendered HTML grep). Preview is public (no Basic Auth); never print secrets.
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

## Codex CLI Timeouts (VM2)
- Avoid long-running commands (>10s) in Codex CLI; use short, repeatable polls.
- Do NOT use `gh pr checks --watch`. Use `tools/ci/poll_pr_checks.sh <pr>` instead.
- Do NOT execute markdown files in shell (e.g., `AGENTS.md`); edit them with apply_patch or heredoc.

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
- Research generator schedule controls are stored in `PROC_RESEARCH_SETTINGS` as a single row (SETTINGS_ID=1), not key/value.
- If preflight fails: stop and report the error; do not attempt alternative drivers or wallet rewrites.
- For SC_IDX price issues, use `tools/index_engine/backfill_prices.py` and see `docs/runbooks/price_ingest_and_backfill.md`.
- For SC_IDX index continuity issues, recompute with `tools/index_engine/calc_index.py --rebuild --no-preflight-self-heal` as documented in `docs/runbooks/price_ingest_and_backfill.md`.
- For “index levels stuck while prices update”, follow the “Stuck index levels” section in `docs/runbooks/price_ingest_and_backfill.md`.

## SC_IDX Pipeline Robustness (VM1)
- LangGraph is the primary orchestration pattern for the SC_IDX / TECH100 operational pipeline on VM1.
- Primary entrypoint: `python3 tools/index_engine/run_pipeline.py`.
- Primary systemd scheduler: `sc-idx-pipeline.timer` -> `sc-idx-pipeline.service`.
- The live scheduler path is `/home/opc/Sustainacore`; it should resolve to a versioned release checkout under `/opt/sustainacore-sc-idx-*`.
- Always prove the active scheduler revision with `readlink -f /home/opc/Sustainacore` and `git -C "$(readlink -f /home/opc/Sustainacore)" rev-parse --short HEAD`.
- `sc-idx-pipeline.service` must invoke `run_pipeline.py` directly. Do not wrap that unit in
  `flock`; the LangGraph runtime owns `/tmp/sc_idx_pipeline.lock` internally, and an outer
  service-level `flock` will self-block the scheduler path.
- Pipeline state is tracked in `SC_IDX_PIPELINE_STATE`; `python3 tools/index_engine/run_pipeline.py` resumes safe completed nodes by default.
- Resume is only for incomplete runs. If a run already reached `persist_terminal_status` or `release_lock`, the next invocation must start a new `run_id` instead of mutating the old terminal row.
- `acquire_lock` is a terminal gate. If lock acquisition is `BLOCKED`, the graph must branch directly to report/alert/telemetry and must not continue into `determine_target_dates`.
- Early blocked/failed reports should still surface last-known calendar and table freshness from the
  Oracle health snapshot whenever preflight succeeded.
- Oracle stage-state rows are intentionally compact; the full same-run stage payload also lives in
  `tools/audit/output/pipeline_state_latest.json` and is keyed by `run_id`, not only by day.
- Force a full restart with `python3 tools/index_engine/run_pipeline.py --restart`.
- Use `python3 tools/index_engine/run_pipeline.py --smoke --smoke-scenario degraded --restart` for a no-provider smoke run.
- Terminal outcomes are:
  - `success`
  - `success_with_degradation`
  - `clean_skip`
  - `failed`
  - `blocked`
- `SC_IDX_JOB_RUNS.STATUS` stores the compact terminal code:
  - `OK` -> `success`
  - `DEGRADED` -> `success_with_degradation`
  - `SKIP` -> `clean_skip`
  - `ERROR` -> `failed`
  - `BLOCKED` -> `blocked`
- Run reports are written under `tools/audit/output/pipeline_runs/`.
- Structured telemetry snapshots are written under `tools/audit/output/pipeline_telemetry/`.
- Daily SC_IDX telemetry artifacts are written under `tools/audit/output/pipeline_daily/`.
- Health summaries are persisted in `SC_IDX_JOB_RUNS` (job_name=`sc_idx_pipeline`) and written to `tools/audit/output/pipeline_health_latest.txt`.
- Health and daily-report artifacts include `repo_root` and `repo_head`; use them to verify the deployed VM1 checkout instead of assuming `origin/main` is live.
- The pipeline computes a first-class freshness verdict from expected target date vs actual table dates.
  - `SC_IDX_STALE_ALLOWED_LAG_DAYS` (default 0) defines tolerated lag before the run/report becomes `Stale`.
  - `SC_IDX_TRADING_DAY_FALLBACK_MAX_GAP` (default 3 weekdays) bounds the synthetic weekday fallback used when trading-day refresh degrades on a provider timeout or 403.
  - A stale verdict is raised when prices advance but levels do not, levels advance but stats do not, levels advance but portfolio tables do not, or the latest complete downstream date lags the expected target date.
- Calc-stage completeness is defined by the slowest required calc table, not only by `SC_IDX_LEVELS`.
  - Treat `SC_IDX_LEVELS`, `SC_IDX_CONTRIBUTION_DAILY`, and `SC_IDX_STATS_DAILY` as one completion unit.
  - If levels advance but contribution/stats do not, the next run must re-enter calc instead of reporting `up_to_date`.
- Portfolio-stage completeness is defined by the slowest required portfolio table, not only by `SC_IDX_PORTFOLIO_ANALYTICS_DAILY`.
  - Treat `SC_IDX_PORTFOLIO_ANALYTICS_DAILY`, `SC_IDX_PORTFOLIO_POSITION_DAILY`, and `SC_IDX_PORTFOLIO_OPT_INPUTS` as one completion unit.
  - If analytics advance but positions/optimizer inputs do not, the next run must re-enter portfolio refresh instead of skipping.
- Success/clean-skip reports must still show last-known downstream freshness when a stage is skipped.
  - `stats_max_date` should fall back to the pre-run Oracle max if calc was skipped.
  - `portfolio_position_max_date` should fall back to the pre-run Oracle max if portfolio refresh was skipped.
- Oracle retry knobs: `SC_IDX_ORACLE_RETRY_ATTEMPTS` (default 5) and `SC_IDX_ORACLE_RETRY_BASE_SEC` (default 1).
- Impute guardrails: `SC_IDX_IMPUTE_LOOKBACK_DAYS` (default 30) and `SC_IDX_IMPUTE_TIMEOUT_SEC` (default 300).
- Imputed replacement guardrails: `SC_IDX_IMPUTED_REPLACEMENT_DAYS` (default 30) and `SC_IDX_IMPUTED_REPLACEMENT_LIMIT` (default 10).
- Oracle evidence files on failure: `tools/audit/output/oracle_health_*.txt` (no secrets).
- Systemd SC_IDX units load `/etc/sustainacore/index.env` for non-secret pipeline config (e.g., `MARKET_DATA_API_BASE_URL`).
- Exit code `2` is a terminal SC_IDX blocked/non-advancing outcome and must not be auto-restarted by systemd.
- Failure alert email policy:
  - `failed` and `blocked` attempt email by default
  - `stale` attempts email by default, even when the graph technically concluded as `clean_skip` or `success_with_degradation`
  - repeated `success_with_degradation` runs attempt email by default after `SC_IDX_ALERT_DEGRADED_REPEAT_THRESHOLD` consecutive degraded runs (default 2)
  - single `success_with_degradation` only emails when `SC_IDX_EMAIL_ON_DEGRADED=1`
  - `daily_budget_stop` only emails when `SC_IDX_EMAIL_ON_BUDGET_STOP=1`
  - `clean_skip` only stays quiet when freshness is not stale
  - smoke runs do not send email
- Alert duplicate-suppression state lives in `SC_IDX_ALERT_STATE`; the once-per-day gate is marked only after successful SMTP delivery.
- Required SMTP env names for SC_IDX alerts are `SMTP_USER`, `SMTP_PASS`, `MAIL_FROM`, and `MAIL_TO`.
- Daily report recipients resolve in this order:
  - `SC_IDX_DAILY_REPORT_RECIPIENTS`
  - `TELEMETRY_REPORT_RECIPIENTS`
  - `MAIL_TO`
- VM1 daily report scheduler: `sc-telemetry-report.timer` -> `sc-telemetry-report.service`, which runs `python3 tools/index_engine/daily_telemetry_report.py --send`.
- The daily report is operator-grade: it shows `overall_health` (`Healthy` / `Degraded` / `Failed` / `Blocked` / `Stale` / `Skipped`), expected target date, latest complete date, stale signals, alert send/suppression state, artifact paths, and deployed repo identity.

## FI -> FISV Normalization (VM1)
- Ticker normalization is enforced in ingest + DB helpers: `FI` is mapped to `FISV`.
- Migration tool: `tools/db_migrations/migrate_fi_to_fisv.py` (dry-run default, `--apply` to write).
- Rollback via backup tables `SC_BAK_FI_FISV_*` created per apply run.

## GitHub Identity and Contribution Attribution
- The only allowed GitHub login for write actions is `joaotovolli`.
- Codex CLI must never create commits, branches, pushes, Pull Requests, Pull Request updates, GitHub comments, or other GitHub write actions using any external Codex, bot, generic, or non-`joaotovolli` GitHub account.
- Before `git checkout -b`, `git switch -c`, `git commit`, `git push`, `gh pr create`, `gh pr edit`, `gh pr comment`, or any other Git/GitHub write action, Codex must run the read-only identity verifier: `bash scripts/verify_github_identity.sh`.
- If `gh api user -q .login` does not return `joaotovolli`, Codex may make local file edits only and must stop before Git/GitHub write actions.
- Git author must be `Joao Tovolli`.
- Git email must be `225354763+joaotovolli@users.noreply.github.com`.
- Every Pull Request must be opened with `gh` authenticated as `joaotovolli`.
- Never add `Co-authored-by` trailers for Codex.
- Never add `Generated-by Codex`, `Authored-by Codex`, or similar generated-by/authored-by trailers.
- The final report for PR work must include authenticated GitHub user, local git author, latest commit author, latest commit committer, and Pull Request author when a PR is created.
- Do not print GitHub tokens, credentials, or GitHub CLI host configuration while verifying identity.

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
- Explicit denylist (never commit): venv/, .venv/, node_modules/, artifacts/, playwright screenshots, dist/, build/, __pycache__/, *.pyc, *.sqlite3, *.db-journal, *.log, large tmp files.
  - Exception: `docs/screenshots/ui/home/pr-<PR>/run-<RUN_ID>/...` is allowed for UI evidence only.
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
