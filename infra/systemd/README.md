## SC_IDX daily price ingest (systemd)

Units live under `infra/systemd/`:
- `sc-idx-price-ingest.service`
- `sc-idx-price-ingest.timer`
- `sc-idx-completeness-check.service`
- `sc-idx-completeness-check.timer`
- `sc-idx-index-calc.service`
- `sc-idx-index-calc.timer`
- `sc-idx-pipeline.service`
- `sc-idx-pipeline.timer`

### Behavior
- Runs incremental backfill up to the latest available EOD trade date (SPY) via `/usr/bin/python3 /opt/sustainacore-ai/tools/index_engine/run_daily.py`.
- Default range: start `2025-01-02` to `end = latest_eod_date(SPY)`, ticker batches sized to stay under TwelveData free-tier limits.
- Environment files loaded on VM1: `/etc/sustainacore/db.env`, `/etc/sustainacore-ai/secrets.env`.
- Timer schedule:
  - Ingest: **00:00, 05:00 UTC** with `Persistent=true` so missed runs catch up on restart.
  - Pipeline orchestrator (ingest + completeness + impute + index calc): **00:30, 05:30 UTC** (~30 minutes after ingest to include the latest effective end date).

### Install / enable
```bash
sudo cp infra/systemd/sc-idx-price-ingest.* /etc/systemd/system/
sudo cp infra/systemd/sc-idx-completeness-check.* /etc/systemd/system/
sudo cp infra/systemd/sc-idx-index-calc.* /etc/systemd/system/
sudo cp infra/systemd/sc-idx-pipeline.* /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now sc-idx-pipeline.timer
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
Index calc:
```bash
sudo systemctl start sc-idx-index-calc.service
python tools/index_engine/calc_index.py --since-base --strict
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
- Check index calc timer: `systemctl list-timers sc-idx-index-calc.timer`
- Check pipeline timer: `systemctl list-timers sc-idx-pipeline.timer`
<!-- cspell:disable-next-line -->
- Check last run + logs: `sudo journalctl -u sc-idx-price-ingest.service -n 200 --no-pager`
<!-- cspell:disable-next-line -->
- Completeness logs: `sudo journalctl -u sc-idx-completeness-check.service -n 200 --no-pager`
<!-- cspell:disable-next-line -->
- Index calc logs: `sudo journalctl -u sc-idx-index-calc.service -n 200 --no-pager`
- Common causes of `rows_ok=0`:
  - Weekend/market holiday (no provider data).
  - Ticker mapping issues (symbol not recognized by TwelveData).
  - Upstream throttling (wait and rerun the service).
