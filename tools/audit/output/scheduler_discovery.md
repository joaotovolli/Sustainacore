# Scheduler discovery (VM1)

## Systemd units (active)
- `sc-idx-pipeline.timer` → `sc-idx-pipeline.service`
  - Timer: 00:30 + 05:30 UTC (`Persistent=true`)
  - Service: `User=opc`, `WorkingDirectory=/opt/sustainacore-ai`, `ExecStart=.../tools/index_engine/run_pipeline.py`
  - Environment files at discovery: `/etc/sustainacore/db.env`, `/etc/sustainacore-ai/secrets.env` (no `index.env` at the time)
- `sc-idx-price-ingest.timer` → `sc-idx-price-ingest.service`
  - Timer: 00:00 + 05:00 UTC (`Persistent=true`)
  - Service: `ExecStart=.../tools/index_engine/run_daily.py`
- `sc-idx-completeness-check.timer` → `sc-idx-completeness-check.service`
  - Timer: weekdays 00:10 UTC (`Persistent=true`)
  - Service: `ExecStart=.../tools/index_engine/check_price_completeness.py`
- `sc-idx-index-calc.timer` → `sc-idx-index-calc.service`
  - Timer: 01:30 UTC (`Persistent=true`)
  - Service: `ExecStart=.../tools/index_engine/calc_index.py`

## Cron
- `crontab -l`: only acme.sh renewal entry.
- `sudo -n crontab -l`: no root crontab.
- `/etc/cron.d`, `/etc/cron.daily`, `/etc/cron.hourly`, `/etc/cron.weekly`, `/etc/cron.monthly`: no SC_IDX entries.

## Other schedulers
- `rg` across `/etc`, `/home`, `/opt` found only systemd unit references to SC_IDX pipelines and repo hits; no pm2/supervisor/other schedulers found.
