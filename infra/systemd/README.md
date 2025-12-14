## SC_IDX daily price ingest (systemd)

Units live under `infra/systemd/`:
- `sc-idx-price-ingest.service`
- `sc-idx-price-ingest.timer`

### Behavior
- Runs incremental backfill up to yesterday UTC via `/usr/bin/python3 /opt/sustainacore-ai/tools/index_engine/run_daily.py`.
- Default range: start `2025-01-02` to `end = (UTC today - 1 day)`, ticker batches sized to stay under TwelveData free-tier limits.
- Environment files loaded on VM1: `/etc/sustainacore/db.env`, `/etc/sustainacore-ai/secrets.env`.
- Timer schedule: **23:30 UTC** daily with `Persistent=true` so missed runs catch up on restart.

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
  - Ticker mapping issues (symbol not recognized by TwelveData).
  - Upstream throttling (wait and rerun the service).

## Alpha Vantage incremental ingest (Mon–Fri)

Units live under `infra/systemd/`:
- `sc-idx-av-price-ingest.service`
- `sc-idx-av-price-ingest.timer`

### Behavior
- Runs `/usr/bin/python3 /opt/sustainacore-ai/tools/index_engine/ingest_alphavantage.py --incremental` to fetch the latest daily adjusted bars via Alpha Vantage.
- Uses the same environment files (`/etc/sustainacore/db.env` + `/etc/sustainacore-ai/secrets.env`) with permission sanity checks before launch.
- Enforces the Alpha Vantage daily limit/buffer and cross-process throttle (`SC_IDX_ALPHAVANTAGE_*` envs + `/tmp/sc_idx_alphavantage.lock`).
- Reconciles `SC_IDX_PRICES_CANON` immediately after each run to merge `ALPHAVANTAGE` with `TWELVEDATA` data.
- Timer: `OnCalendar=Mon..Fri 23:40:00 UTC`, `Persistent=true`.

### Install / enable
```bash
sudo cp infra/systemd/sc-idx-av-price-ingest.* /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now sc-idx-av-price-ingest.timer
```

### Manual run
```bash
sudo systemctl start sc-idx-av-price-ingest.service
# or directly
python tools/index_engine/ingest_alphavantage.py --incremental
```

### Troubleshooting
- Timer status: `systemctl list-timers sc-idx-av-price-ingest.timer`
- Logs: `sudo journalctl -u sc-idx-av-price-ingest.service -n 200 --no-pager`
- Budget stops print `budget_stop_av` (exit code 0); inspect `SC_IDX_JOB_RUNS` for usage details.
