<!-- cspell:ignore readlink -->
# TECH100 pipeline scheduler (VM1)

## Primary scheduler

VM1 uses systemd timers. The primary orchestration path is:

- `sc-idx-pipeline.timer` -> `sc-idx-pipeline.service`

This service runs the LangGraph orchestrator:

```bash
python3 tools/index_engine/run_pipeline.py
```

The daily operator report path is separate and scheduled by:

- `sc-telemetry-report.timer` -> `sc-telemetry-report.service`

This runs:

```bash
python3 tools/index_engine/daily_telemetry_report.py --send
```

Compatibility units still exist for focused manual use:

- `sc-idx-price-ingest.timer` -> `sc-idx-price-ingest.service`
- `sc-idx-completeness-check.timer` -> `sc-idx-completeness-check.service`
- `sc-idx-index-calc.timer` -> `sc-idx-index-calc.service`

## Environment and checkout

Systemd units load:

- `/etc/sustainacore/db.env`
- `/etc/sustainacore/index.env`
- `/etc/sustainacore-ai/secrets.env`

Systemd also sets:

- `TNS_ADMIN=/opt/adb_wallet`
- `PYTHONPATH=/home/opc/Sustainacore`

Confirm the scheduler checkout before any investigation or deploy:

```bash
readlink -f /home/opc/Sustainacore
sudo -n -u opc git -C "$(readlink -f /home/opc/Sustainacore)" rev-parse --short HEAD
```

Do not print env contents in logs or docs.

## Schedule (UTC)

- price-ingest compatibility timer: `00:00`, `05:00`, `09:00`, `13:00`
- primary LangGraph pipeline timer: `00:30`, `05:30`, `09:30`, `13:30`
- daily SC_IDX telemetry report timer: `06:45`
- completeness compatibility timer: weekdays `00:10`
- index-calc compatibility timer: `01:30`

## Lock and runtime guardrails

- ingest + pipeline use the shared lock `/tmp/sc_idx_pipeline.lock` via `flock -n`
- primary pipeline runtime limit: `RuntimeMaxSec=3600`
- ingest runtime limit: `RuntimeMaxSec=7200`
- terminal blocked/guard exits use code `2` and should not be auto-restarted by systemd
- a blocked `acquire_lock` outcome is terminal for that invocation; the graph must report and exit
  before `determine_target_dates`
- only incomplete runs should resume; once a run reaches `persist_terminal_status` or
  `release_lock`, the next invocation must create a fresh `run_id`
- restart storm guardrails remain in the units through `StartLimit*`
- retries inside the graph are bounded and stage-specific

## Healthy signals

- latest `SC_IDX_JOB_RUNS` row for `job_name='sc_idx_pipeline'` is `OK`, `DEGRADED`, or `SKIP`
- latest `SC_IDX_PIPELINE_STATE` rows show the node sequence reaching `persist_terminal_status`
- if a stage detail payload is too large for Oracle, `SC_IDX_PIPELINE_STATE` keeps the compact node
  summary while the full same-run details remain in
  `tools/audit/output/pipeline_state_latest.json`
- `SC_IDX_TRADING_DAYS`, `SC_IDX_PRICES_CANON`, `SC_IDX_LEVELS`, and `SC_IDX_STATS_DAILY` max dates align where expected
- `SC_IDX_PORTFOLIO_ANALYTICS_DAILY` and `SC_IDX_PORTFOLIO_POSITION_DAILY` match the latest `SC_IDX_LEVELS` trade date after a successful run
- latest report exists under `tools/audit/output/pipeline_runs/`
- latest telemetry snapshot exists under `tools/audit/output/pipeline_telemetry/`
- latest daily report exists under `tools/audit/output/pipeline_daily/`
- `tools/audit/output/pipeline_health_latest.txt` shows no active `last_error`

## Logs and status

```bash
systemctl list-timers --all | rg -i "sc-idx"
systemctl status sc-idx-pipeline.service
systemctl status sc-telemetry-report.service
readlink -f /home/opc/Sustainacore
sudo -n -u opc git -C "$(readlink -f /home/opc/Sustainacore)" rev-parse --short HEAD
sudo journalctl -u sc-idx-pipeline.service -n 200 --no-pager
sudo journalctl -u sc-telemetry-report.service -n 200 --no-pager
sudo -n systemd-run --wait --collect --pipe \
  -p WorkingDirectory=/home/opc/Sustainacore \
  -p User=opc -p Group=opc \
  -p Environment=PYTHONPATH=/home/opc/Sustainacore \
  -p Environment=TNS_ADMIN=/opt/adb_wallet \
  -p EnvironmentFile=/etc/sustainacore/db.env \
  -p EnvironmentFile=/etc/sustainacore/index.env \
  -p EnvironmentFile=/etc/sustainacore-ai/secrets.env \
  -- /home/opc/Sustainacore/.venv/bin/python tools/index_engine/pipeline_health.py
```

## Manual runs

Primary fresh run:

```bash
python3 tools/index_engine/run_pipeline.py --restart
```

Normal resume:

```bash
python3 tools/index_engine/run_pipeline.py
```

No-provider smoke:

```bash
python3 tools/index_engine/run_pipeline.py --smoke --smoke-scenario degraded --restart
```

Daily report render:

```bash
python3 tools/index_engine/daily_telemetry_report.py --skip-db --dry-run
```

Service-equivalent run:

```bash
sudo systemctl start sc-idx-pipeline.service
sudo systemctl start sc-telemetry-report.service
```

If repeated `BLOCKED` runs happen with no active SC_IDX process, inspect the effective unit config
and verify both of these before rerunning the pipeline:

- exit code `2` is listed under `SuccessExitStatus` / `RestartPreventExitStatus`
- the installed graph does not continue past `acquire_lock`, and same-day blocked reruns are not
  mutating a single terminal `run_id`

## Alert behavior

- `failed` and `blocked` attempt failure email by default
- `stale` attempts email by default, even when the graph technically concluded
- repeated `success_with_degradation` attempts email by default after
  `SC_IDX_ALERT_DEGRADED_REPEAT_THRESHOLD` consecutive degraded runs
- single `success_with_degradation` only emails when `SC_IDX_EMAIL_ON_DEGRADED=1`
- `daily_budget_stop` only emails when `SC_IDX_EMAIL_ON_BUDGET_STOP=1`
- `clean_skip` only stays quiet when freshness is not stale
- smoke runs do not send email
- same-day duplicate suppression uses `SC_IDX_ALERT_STATE`
- the alert gate is only marked after successful SMTP delivery
- failed SMTP sends are recorded in the run report and telemetry as `send_failed`

## Freshness triage

- Read the latest run report, telemetry snapshot, and `tools/audit/output/pipeline_health_latest.txt`
  together.
- Compare:
  - `expected_target_date`
  - `latest_complete_date`
  - max dates in canon / levels / stats / portfolio tables
- Treat any `overall_health=Stale` verdict as actionable, even if `terminal_status=clean_skip`.
- Use `repo_root` and `repo_head` from the health snapshot plus `systemctl show ... WorkingDirectory`
  to confirm the VM1 scheduler is running the expected checkout before debugging code paths.

## Recovery

- safe retry: `sudo systemctl restart sc-idx-pipeline.service`
- if Oracle preflight blocks, fix wallet/env access first, then rerun
- if the pipeline returns `BLOCKED` because the lock is busy, wait for the active run to conclude
- do not delete `SC_IDX_PIPELINE_STATE` unless following an explicit emergency recovery procedure

## Update workflow

1. Edit repo-managed units under `infra/systemd/`.
2. Copy them to VM1 and reload:

```bash
sudo cp infra/systemd/sc-idx-*.service /etc/systemd/system/
sudo cp infra/systemd/sc-idx-*.timer /etc/systemd/system/
sudo cp infra/systemd/sc-telemetry-report.* /etc/systemd/system/
sudo systemctl daemon-reload
```

3. Restart the affected unit or wait for the next timer tick.

## Deploy note

- VM1 deploys must refresh both repo roots:
  - `/opt/sustainacore-ai` for `sustainacore-ai.service`
  - `/home/opc/Sustainacore` for `sc-idx-*` timers and services
- `ops/scripts/deploy_vm.sh` and `ops/scripts/deploy_vm1_git.sh` handle both paths
- after deploy, re-run the checkout and health commands above to confirm:
  - the active systemd `WorkingDirectory`
  - the deployed `repo_head`
  - the latest report/health artifacts show the same repo identity
  - the portfolio freshness fields advanced together
