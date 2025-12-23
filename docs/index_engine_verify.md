## SC_IDX ingest verification (VM1)

Run from the repo root unless noted.

1) Verify market data credits and Oracle rows

```bash
python tools/index_engine/verify_pipeline.py
```
If the CLI user cannot read `/etc/sustainacore*` env files, run the command with `sudo` (and set `PYTHONPATH=/home/opc/.local/lib/python3.9/site-packages` if `oracledb` is installed in the user site-packages).
The script reads `/api_usage` for a per-minute before/after delta and fetches the latest AAPL daily bar; daily credit limits are enforced separately via `SC_IDX_MARKET_DATA_DAILY_LIMIT` (default 800) and `SC_IDX_MARKET_DATA_DAILY_BUFFER` recorded in Oracle job runs. Ensure `MARKET_DATA_API_KEY` and `MARKET_DATA_API_BASE_URL` are available in the env files.
The provider uses a shared throttle (default 6 calls per 120s) with a cross-process lock at `/tmp/sc_idx_market_data.lock`; override with `SC_IDX_MARKET_DATA_CALLS_PER_WINDOW` and `SC_IDX_MARKET_DATA_WINDOW_SECONDS` if needed.

2) Confirm the systemd timer is scheduled

```bash
systemctl list-timers --all | grep sc-idx
```

3) Review recent service logs

```bash
journalctl -u sc-idx-price-ingest.service -n 200 --no-pager
```
