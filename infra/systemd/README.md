## SC_IDX daily price ingest (systemd)

Units live in this repo under `infra/systemd/`:
- `sc-idx-price-ingest.service`
- `sc-idx-price-ingest.timer`

### Behavior
- Runs incremental backfill up to yesterday UTC via `/usr/bin/python3 /opt/sustainacore-ai/tools/index_engine/run_daily.py`.
- Default range: start `2025-01-02` to `end = (UTC today - 1 day)`, chunked tickers to stay under TwelveData free-tier limits.
- Environment files loaded: `/etc/sustainacore/db.env`, `/etc/sustainacore-ai/secrets.env`.

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

### Logs
```bash
sudo journalctl -u sc-idx-price-ingest.service -n 200 --no-pager
```
