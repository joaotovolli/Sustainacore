## SC_IDX daily price ingest (systemd)

Units live under `infra/systemd/`:
- `sc-idx-price-ingest.service`
- `sc-idx-price-ingest.timer`
- `sc-idx-completeness-check.service`
- `sc-idx-completeness-check.timer`

### Behavior
- Runs incremental backfill up to yesterday UTC via `/usr/bin/python3 /opt/sustainacore-ai/tools/index_engine/run_daily.py`.
- Default range: start `2025-01-02` to `end = (UTC today - 1 day)`, ticker batches sized to stay under TwelveData free-tier limits.
- Environment files loaded on VM1: `/etc/sustainacore/db.env`, `/etc/sustainacore-ai/secrets.env`.
- Timer schedule: **23:30 UTC** daily with `Persistent=true` so missed runs catch up on restart.

### Install / enable
```bash
sudo cp infra/systemd/sc-idx-price-ingest.* /etc/systemd/system/
sudo cp infra/systemd/sc-idx-completeness-check.* /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now sc-idx-price-ingest.timer
sudo systemctl enable --now sc-idx-completeness-check.timer
```

### Manual run
```bash
sudo systemctl start sc-idx-price-ingest.service
# or directly
python tools/index_engine/run_daily.py
```
Completeness check:
```bash
sudo systemctl start sc-idx-completeness-check.service
python tools/index_engine/check_price_completeness.py --since-base --end today --email-on-fail
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
- Check completeness timer: `systemctl list-timers sc-idx-completeness-check.timer`
- Check last run + logs: `sudo journalctl -u sc-idx-price-ingest.service -n 200 --no-pager`
- Completeness logs: `sudo journalctl -u sc-idx-completeness-check.service -n 200 --no-pager`
- Common causes of `rows_ok=0`:
  - Weekend/market holiday (no provider data).
  - Ticker mapping issues (symbol not recognized by TwelveData).
  - Upstream throttling (wait and rerun the service).
