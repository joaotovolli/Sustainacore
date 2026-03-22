## SC_IDX daily price ingest (systemd)

Units live under `infra/systemd/`:
- `sc-idx-price-ingest.service`
- `sc-idx-price-ingest.timer`

### Behavior
- Runs incremental backfill up to yesterday UTC via `/usr/bin/python3 /home/opc/Sustainacore/tools/index_engine/run_daily.py`.
- This is now a compatibility ingest unit. The primary orchestration path is `sc-idx-pipeline.service`.
- Default range: start `2025-01-02` to `end = (UTC today - 1 day)`, ticker batches sized to stay under provider plan limits.
- Environment files loaded on VM1: `/etc/sustainacore/db.env`, `/etc/sustainacore-ai/secrets.env`, `/etc/sustainacore/index.env`.
- Environment files loaded on VM1: `/etc/sustainacore/db.env`, `/etc/sustainacore/index.env` (non-secret runtime config), `/etc/sustainacore-ai/secrets.env`.
- Timer schedule: **00:00 + 05:00 UTC** with `Persistent=true` so missed runs catch up on restart.

## SC_IDX LangGraph pipeline (systemd)

Units live under `infra/systemd/`:

- `sc-idx-pipeline.service`
- `sc-idx-pipeline.timer`

### Behavior

- Runs the primary VM1 LangGraph orchestration path:
  - `/usr/bin/python3 /home/opc/Sustainacore/tools/index_engine/run_pipeline.py`
- Coordinates preflight, target-date planning, readiness, ingest, completeness, imputation, index/statistics, reporting, alerting, telemetry, terminal persistence, and lock release.
- Uses the same VM1 env files:
  - `/etc/sustainacore/db.env`
  - `/etc/sustainacore/index.env`
  - `/etc/sustainacore-ai/secrets.env`
- Writes reports under `tools/audit/output/pipeline_runs/`.
- Writes telemetry under `tools/audit/output/pipeline_telemetry/`.
- Timer schedule: **00:30, 05:30, 09:30, 13:30 UTC** with `Persistent=true`.

### Manual run

```bash
sudo systemctl start sc-idx-pipeline.service
# or directly
python tools/index_engine/run_pipeline.py --restart
```

### Install / enable
```bash
sudo cp infra/systemd/sc-idx-price-ingest.* /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now sc-idx-price-ingest.timer
```

### Manual run
```bash
sudo systemctl start sc-idx-price-ingest.service
# or directly
python tools/index_engine/run_daily.py
```
Override end date if needed:
```bash
python tools/index_engine/run_daily.py --end 2025-02-15
```
Override tickers if needed:
```bash
python tools/index_engine/run_daily.py --tickers MSFT,GOOGL
```

### Troubleshooting
- Check timer status/schedule: `systemctl list-timers sc-idx-price-ingest.timer`
- Check last run + logs: `sudo journalctl -u sc-idx-price-ingest.service -n 200 --no-pager`
- Common causes of `rows_ok=0`:
  - Weekend/market holiday (no provider data).
  - Ticker mapping issues (symbol not recognized by the provider).
  - Upstream throttling (wait and rerun the service).

## VM2 web telemetry rollup (systemd)

Units live under `infra/systemd/`:
- `sc-web-telemetry-rollup.service`
- `sc-web-telemetry-rollup.timer`

### Behavior
- Runs `manage.py aggregate_web_telemetry` daily on VM2.
- Immediately follows with `manage.py purge_web_telemetry --aggregates --sessions --consents`.
- Uses the same VM2 env files as Gunicorn:
  - `/etc/sustainacore.env`
  - `/etc/sustainacore/db.env`
  - `/etc/sysconfig/sustainacore-django.env`
- Timer schedule: **00:35 UTC** with `Persistent=true` so missed runs catch up after restart.

### Install / enable
```bash
sudo cp infra/systemd/sc-web-telemetry-rollup.* /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now sc-web-telemetry-rollup.timer
```

### Manual run
```bash
sudo systemctl start sc-web-telemetry-rollup.service
sudo journalctl -u sc-web-telemetry-rollup.service -n 200 --no-pager
```
