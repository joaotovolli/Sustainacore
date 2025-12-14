## SC_IDX ingest verification (VM1)

Run from the repo root unless noted.

1) Verify Twelve Data credits and Oracle rows

```bash
python tools/index_engine/verify_pipeline.py
```
If the CLI user cannot read `/etc/sustainacore*` env files, run the command with `sudo` (and set `PYTHONPATH=/home/opc/.local/lib/python3.9/site-packages` if `oracledb` is installed in the user site-packages).

2) Confirm the systemd timer is scheduled

```bash
systemctl list-timers --all | grep sc-idx
```

3) Review recent service logs

```bash
journalctl -u sc-idx-price-ingest.service -n 200 --no-pager
```
