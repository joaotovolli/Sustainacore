# TECH100 pipeline scheduler (VM1)

## Primary scheduler

VM1 uses systemd timers. The primary orchestration path is now:

- `sc-idx-pipeline.timer` -> `sc-idx-pipeline.service`

This service runs the LangGraph orchestrator:

```bash
python3 tools/index_engine/run_pipeline.py
```

## Related compatibility units

These units still exist for compatibility and focused manual use, but they are not the primary control plane:

- `sc-idx-price-ingest.timer` -> `sc-idx-price-ingest.service`
- `sc-idx-completeness-check.timer` -> `sc-idx-completeness-check.service`
- `sc-idx-index-calc.timer` -> `sc-idx-index-calc.service`

## Environment files

Systemd units load:

- `/etc/sustainacore/db.env`
- `/etc/sustainacore/index.env`
- `/etc/sustainacore-ai/secrets.env`

Do not print env contents in logs or docs.

## Schedule (UTC)

- price-ingest compatibility timer: `00:00`, `05:00`, `09:00`, `13:00`
- primary LangGraph pipeline timer: `00:30`, `05:30`, `09:30`, `13:30`
- completeness compatibility timer: weekdays `00:10`
- index-calc compatibility timer: `01:30`

## Lock and runtime guardrails

- overlap guard: `/tmp/sc_idx_pipeline.lock`
- primary service runtime limit: `RuntimeMaxSec=3600`
- restart storm guardrails remain in systemd `StartLimit*`
- retries inside the graph are bounded and stage-specific

## Healthy signals

- latest `SC_IDX_JOB_RUNS` row for `job_name='sc_idx_pipeline'` is `OK`, `DEGRADED`, or `SKIP`
- latest `SC_IDX_PIPELINE_STATE` rows show the node sequence reaching `persist_terminal_status`
- latest report exists under `tools/audit/output/pipeline_runs/`
- latest telemetry snapshot exists under `tools/audit/output/pipeline_telemetry/`

## Manual runs

Primary run:

```bash
python3 tools/index_engine/run_pipeline.py --restart
```

No-provider smoke:

```bash
python3 tools/index_engine/run_pipeline.py --smoke --smoke-scenario degraded --restart
```

Systemd-run equivalent:

```bash
sudo systemctl start sc-idx-pipeline.service
```

## Logs and status

```bash
systemctl list-timers --all | rg -i "sc-idx"
systemctl status sc-idx-pipeline.service
sudo journalctl -u sc-idx-pipeline.service -n 200 --no-pager
```

## Recovery

- normal resume: `python3 tools/index_engine/run_pipeline.py`
- force a fresh run: `python3 tools/index_engine/run_pipeline.py --restart`
- if Oracle preflight blocks, fix wallet/env first, then rerun
- if the pipeline reports `BLOCKED` because the lock is busy, wait for the active run to conclude instead of forcing overlap

## Update workflow

1. Edit repo-managed units under `infra/systemd/`.
2. Install to VM1 and reload:

```bash
sudo cp infra/systemd/sc-idx-*.service /etc/systemd/system/
sudo cp infra/systemd/sc-idx-*.timer /etc/systemd/system/
sudo systemctl daemon-reload
```

3. Restart the affected unit or wait for the next timer tick.
