<!-- cspell:ignore sysconfig -->
## SC_IDX daily price ingest (systemd)

Units live under `infra/systemd/`:
- `sc-idx-price-ingest.service`
- `sc-idx-price-ingest.timer`

### Behavior
- Runs incremental backfill up to yesterday UTC via `/usr/bin/python3 /home/opc/Sustainacore/tools/index_engine/run_daily.py`.
- This is now a compatibility ingest unit. The primary orchestration path is `sc-idx-pipeline.service`.
- Default range: start `2025-01-02` to `end = (UTC today - 1 day)`, ticker batches sized to stay under provider plan limits.
- Environment files loaded on VM1: `/etc/sustainacore/db.env`, `/etc/sustainacore/index.env` (non-secret runtime config), `/etc/sustainacore-ai/secrets.env`.
- Timer schedule: **00:00 + 05:00 UTC** with `Persistent=true` so missed runs catch up on restart.

## SC_IDX LangGraph pipeline (systemd)

Units live under `infra/systemd/`:

- `sc-idx-pipeline.service`
- `sc-idx-pipeline.timer`

### Behavior

- Runs the primary VM1 LangGraph orchestration path:
  - `/home/opc/Sustainacore/.venv/bin/python /home/opc/Sustainacore/tools/index_engine/run_pipeline.py`
- Coordinates preflight, target-date planning, readiness, ingest, completeness, imputation, index/statistics, reporting, alerting, telemetry, terminal persistence, and lock release.
- Uses the same VM1 env files:
  - `/etc/sustainacore/db.env`
  - `/etc/sustainacore/index.env`
  - `/etc/sustainacore-ai/secrets.env`
- Writes reports under `tools/audit/output/pipeline_runs/`.
- Writes telemetry under `tools/audit/output/pipeline_telemetry/`.
- Writes health snapshots under `tools/audit/output/pipeline_health_latest.txt`, including
  `repo_root`, `repo_head`, freshness dates, and `last_error`.
- The pipeline service must not wrap the CLI in `flock`; `run_pipeline.py` owns
  `/tmp/sc_idx_pipeline.lock` internally and writes blocked/report/telemetry state itself.
- Treats exit code `2` as a terminal blocked/non-advancing outcome so systemd does not auto-restart into the same lock.
- Timer schedule: **00:30, 05:30, 09:30, 13:30 UTC** with `Persistent=true`.

### Manual run

```bash
sudo systemctl start sc-idx-pipeline.service
# or directly
python tools/index_engine/run_pipeline.py --restart
```

Before assuming the installed unit matches the repo copy, verify the active systemd checkout:

```bash
systemctl show sc-idx-pipeline.service -p WorkingDirectory -p ExecStart
grep -E '^(repo_root|repo_head|calendar_max_date|levels_max_date|stats_max_date|portfolio_max_date|portfolio_position_max_date|alignment_verdict|last_error)=' \
  tools/audit/output/pipeline_health_latest.txt
```

## SC_IDX daily telemetry report (systemd)

Units live under `infra/systemd/`:

- `sc-telemetry-report.service`
- `sc-telemetry-report.timer`

### Behavior

- Runs the daily SC_IDX operator report:
  - `/usr/bin/python3 /home/opc/Sustainacore/tools/index_engine/daily_telemetry_report.py --send`
- Uses the same VM1 env files as the pipeline:
  - `/etc/sustainacore/db.env`
  - `/etc/sustainacore/index.env`
  - `/etc/sustainacore-ai/secrets.env`
- Sets `TNS_ADMIN=/opt/adb_wallet`
- Writes artifacts under `tools/audit/output/pipeline_daily/`
- The report surfaces `overall_health` (`Healthy` / `Degraded` / `Failed` / `Blocked` / `Stale` / `Skipped`),
  expected target date, latest complete date, stale signals, alert send/suppression state, and
  deployed repo identity.
- Uses `flock -n /tmp/sc-telemetry-report.lock`
- Timer schedule: **06:45 UTC** with `Persistent=true`

### Manual run

```bash
sudo systemctl start sc-telemetry-report.service
# or directly
python tools/index_engine/daily_telemetry_report.py --skip-db --dry-run
```

### Install / enable
```bash
sudo cp infra/systemd/sc-idx-price-ingest.* /etc/systemd/system/
sudo cp infra/systemd/sc-idx-pipeline.* /etc/systemd/system/
sudo cp infra/systemd/sc-telemetry-report.* /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now sc-idx-price-ingest.timer
sudo systemctl enable --now sc-idx-pipeline.timer
sudo systemctl enable --now sc-telemetry-report.timer
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
