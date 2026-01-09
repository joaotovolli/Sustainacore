## SC_IDX daily price ingest (systemd)

Units live under `infra/systemd/`:
- `sc-idx-price-ingest.service`
- `sc-idx-price-ingest.timer`

### Behavior
- Runs incremental backfill up to yesterday UTC via `/usr/bin/python3 /opt/sustainacore-ai/tools/index_engine/run_daily.py`.
- Default range: start `2025-01-02` to `end = (UTC today - 1 day)`, ticker batches sized to stay under provider plan limits.
- Environment files loaded on VM1: `/etc/sustainacore/db.env`, `/etc/sustainacore/index.env` (non-secret runtime config), `/etc/sustainacore-ai/secrets.env`.
- Timer schedule: **00:00 + 05:00 UTC** with `Persistent=true` so missed runs catch up on restart.

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
