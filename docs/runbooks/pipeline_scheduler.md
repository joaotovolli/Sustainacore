# TECH100 pipeline scheduler (VM1)

## Scheduler type
- VM1 uses systemd timers for the TECH100 pipeline.
- Primary units:
  - `sc-idx-pipeline.timer` → `sc-idx-pipeline.service`
  - `sc-idx-price-ingest.timer` → `sc-idx-price-ingest.service`
  - `sc-idx-completeness-check.timer` → `sc-idx-completeness-check.service`
  - `sc-idx-index-calc.timer` → `sc-idx-index-calc.service`

## Environment files
Systemd units load (in order):
- `/etc/sustainacore/db.env` (Oracle + non-secret defaults)
- `/etc/sustainacore-ai/secrets.env` (API keys / SMTP secrets)
- `/etc/sustainacore/index.env` (non-secret SC_IDX runtime config, including `MARKET_DATA_API_BASE_URL`)
- `/etc/sustainacore/index.env` (non-secret SC_IDX runtime config, including `MARKET_DATA_API_BASE_URL`)
- `/etc/sustainacore-ai/secrets.env` (API keys / SMTP secrets)

## Repo checkout used by systemd
- Units run from `/home/opc/Sustainacore` with `PYTHONPATH=/home/opc/Sustainacore`.

Do not print env contents in logs or docs.

## Schedule (UTC)
- Price ingest: `00:00` + `05:00`
- Pipeline: `00:30` + `05:30`
- Completeness check: weekdays `00:10`
- Index calc: `01:30`

## Lock + runtime guardrails
- Ingest + pipeline use a shared file lock: `/tmp/sc_idx_pipeline.lock` via `flock -n`.
- Runtime limits (systemd): `RuntimeMaxSec=7200` for ingest, `RuntimeMaxSec=3600` for pipeline.

## Status + logs
```bash
systemctl list-timers --all | rg -i "sc-idx"
systemctl status sc-idx-pipeline.service
sudo journalctl -u sc-idx-pipeline.service -n 200 --no-pager
```

## Healthy signals
- Latest `SC_IDX_JOB_RUNS` for `sc_idx_pipeline` is `OK` with `error_msg` showing `last_error=None`.
- `SC_IDX_TRADING_DAYS`, `SC_IDX_PRICES_CANON`, `SC_IDX_LEVELS`, `SC_IDX_STATS_DAILY` max dates align.
- `tools/audit/output/pipeline_health_latest.txt` exists and `last_error` is empty/none.

## If stuck
- Safe retry: `sudo systemctl restart sc-idx-pipeline.service`
- Force a fresh pipeline run only if state is stale: `python3 tools/index_engine/run_pipeline.py --restart`
- Do not delete `SC_IDX_PIPELINE_STATE` unless following the emergency procedure in the ops checklist.

## Manual run (same env as timer)
```bash
sudo systemctl start sc-idx-pipeline.service
```

## Update workflow (systemd)
1) Edit the unit in repo under `infra/systemd/`.
2) Copy to `/etc/systemd/system/` and reload:
```bash
sudo cp infra/systemd/sc-idx-*.service /etc/systemd/system/
sudo cp infra/systemd/sc-idx-*.timer /etc/systemd/system/
sudo systemctl daemon-reload
```
3) Restart the affected unit or wait for the next timer tick.
