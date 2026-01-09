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
- `/etc/sustainacore/index.env` (non-secret SC_IDX runtime config, including `MARKET_DATA_API_BASE_URL`)
- `/etc/sustainacore-ai/secrets.env` (API keys / SMTP secrets)

Do not print env contents in logs or docs.

## Schedule (UTC)
- Price ingest: `00:00` + `05:00`
- Pipeline: `00:30` + `05:30`
- Completeness check: weekdays `00:10`
- Index calc: `01:30`

## Status + logs
```bash
systemctl list-timers --all | rg -i "sc-idx"
systemctl status sc-idx-pipeline.service
sudo journalctl -u sc-idx-pipeline.service -n 200 --no-pager
```

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
