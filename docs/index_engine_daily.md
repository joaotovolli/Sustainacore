# Index engine pipeline cadence

The pipeline runs every 6 hours (00:30, 06:30, 12:30, 18:30 UTC). The flow now:

- Calls `/api_usage` once (cost: 1 credit) only for per-minute awareness (plan_limit=8 on the Basic tier). Minute-level throttling is enforced by the provider’s shared throttle/lock (default 6 calls per 120 seconds, process-serialized).
- Computes daily usage from Oracle: `SUM(PROVIDER_CALLS_USED)` in `SC_IDX_JOB_RUNS` for `provider='TWELVEDATA'` and `started_at` in the current UTC day (`TRUNC(SYSTIMESTAMP)` window).
- Applies a daily cap: `remaining_daily = daily_limit - calls_used_today`, `max_provider_calls = max(0, remaining_daily - daily_buffer)`. Environment:
  - `SC_IDX_TWELVEDATA_DAILY_LIMIT` (default 800)
  - `SC_IDX_TWELVEDATA_DAILY_BUFFER` (default 25; falls back to `SC_IDX_TWELVEDATA_CREDIT_BUFFER` if set)
- Hitting the daily cap prints `daily_budget_stop: ...` and exits 0 so systemd does not treat it as a failure (email only when `SC_IDX_EMAIL_ON_BUDGET_STOP=1`).
- Fetches the latest available EOD trade date using `SPY` (Twelve Data) and ingests up to the latest trading day <= that provider date.
- Refreshes the explicit trading-day calendar (`SC_IDX_TRADING_DAYS`) before ingest.
- Runs `tools/index_engine/ingest_prices.py --backfill --start 2025-01-02 --end <latest_eod> --max-provider-calls <computed>` and passes `SC_IDX_TICKERS` when present for the ticker list.
- Executes a strict completeness check against `SC_IDX_TRADING_DAYS`. If gaps remain, it runs carry-forward imputation and emails detailed alerts.
- Note: if Twelve Data has not published today’s bar yet, the job will lag a day and catch up automatically once the bar becomes available.
- Alerts are suppressed to once per UTC day per alert type (completeness fail, missing-without-prior, daily digest) via `SC_IDX_ALERT_STATE`.
- Replacement attempts re-fetch real prices for a bounded subset of imputed rows and overwrite canonical imputed rows when real data arrives.

Environment:

- `SC_TWELVEDATA_API_KEY` or `TWELVEDATA_API_KEY` must be set (never logged).
- All index-engine CLI tools auto-load environment files (best-effort) on startup via `tools/index_engine/env_loader.py`, reading (in order): `/etc/sustainacore/db.env` then `/etc/sustainacore-ai/secrets.env`. Explicit shell env vars still win.
- `SC_IDX_TWELVEDATA_DAILY_LIMIT` sets the daily call ceiling (default 800).
- `SC_IDX_TWELVEDATA_DAILY_BUFFER` reserves extra headroom near the daily cap (default 25; alias `SC_IDX_TWELVEDATA_CREDIT_BUFFER` for back-compat).
- Twelve Data throttle override (rarely needed): `SC_IDX_TWELVEDATA_CALLS_PER_WINDOW` (default 6) and `SC_IDX_TWELVEDATA_WINDOW_SECONDS` (default 120). All provider calls are serialized via `/tmp/sc_idx_twelvedata.lock` to avoid cross-process spikes.
- Optional: `SC_IDX_TICKERS` (comma separated), `SC_IDX_ENABLE_IMPUTATION` (default 1), `SC_IDX_IMPUTED_REPLACEMENT_DAYS` (default 30), `SC_IDX_IMPUTED_REPLACEMENT_LIMIT` (default 10), and `SC_IDX_DAILY_DIGEST_ALWAYS` (default 0).
- Email alerts on failure use SMTP envs from `/etc/sustainacore-ai/secrets.env`: `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASS`, `MAIL_FROM`, `MAIL_TO`. Errors trigger an email with a compact run report; set `SC_IDX_EMAIL_ON_BUDGET_STOP=1` to also email budget stops.
- Daily usage and statuses are persisted in `SC_IDX_JOB_RUNS` (DDL: `oracle_scripts/sc_idx_job_runs_v1.sql`). Run `oracle_scripts/sc_idx_job_runs_v1_drop.sql` to drop if rollback is needed. Example query: `SELECT run_id, status, error_msg, started_at, ended_at FROM SC_IDX_JOB_RUNS ORDER BY started_at DESC FETCH FIRST 20 ROWS ONLY;`

## Oracle preflight

- `tools/index_engine/run_daily.py` (Twelve Data) runs an Oracle preflight (`SELECT USER FROM dual`) before doing any provider/API work.
- If the wallet/env is broken, the job prints wallet diagnostics (TNS_ADMIN + best-effort `sqlnet.ora`/`cwallet.sso` checks), writes an `ERROR` run log row with error token `oracle_preflight_failed`, sends an email alert (if SMTP is configured), and exits with code `2` so systemd treats it as a failure.

New CLI flag:

- `--max-provider-calls N` enforces the credit budget in backfill runs. Each ticker download counts as one provider call.

Environment files and permissions:

- Service unit (`infra/systemd/sc-idx-price-ingest.service`) now pre-checks `EnvironmentFile` readability with `ExecStartPre` and prints `ls -l`/`namei -l` for `/etc/sustainacore/db.env` and `/etc/sustainacore-ai/secrets.env` to make permission failures obvious.
- Use `tools/index_engine/fix_env_permissions.sh` to set expected ownership/mode (root:opc 640, directories 750) and SELinux context `etc_t` if enabled.

Alert testing and failure drills:

- To force an error without spending credits: run `python tools/index_engine/verify_sc_idx_env.py --force-fail-run` (uses `SC_IDX_FORCE_FAIL=1`, sends an ERROR email if SMTP envs are present).
- To send a benign ping email without failing the ingest: `python tools/index_engine/verify_sc_idx_env.py --send-email`.
- To inspect recent outcomes: `SELECT run_id, status, error_msg, provider_calls_used, raw_upserts, canon_upserts, end_date FROM SC_IDX_JOB_RUNS ORDER BY started_at DESC FETCH FIRST 20 ROWS ONLY;`.

## VM1 Verification Checklist

Apply trading-day DDL (idempotent, run twice):
```bash
python - <<'PY'
from pathlib import Path
from db_helper import get_connection

sql_path = Path("oracle_scripts/sc_idx_trading_days_v1.sql")
script = sql_path.read_text(encoding="utf-8")
blocks = [b.strip() for b in script.split("/\n") if b.strip()]

def run_blocks():
    with get_connection() as conn:
        cur = conn.cursor()
        for block in blocks:
            cur.execute(block)
        conn.commit()

run_blocks()
run_blocks()
PY
```

Confirm tables exist:
```sql
SELECT COUNT(*) FROM SC_IDX_TRADING_DAYS;
SELECT COUNT(*) FROM SC_IDX_IMPUTATIONS;
```

Populate trading days + latest EOD:
```bash
PYTHONPATH=/opt/sustainacore-ai python tools/index_engine/update_trading_days.py --start 2025-01-02
```
Expected output:
```
latest_eod_date_spy=YYYY-MM-DD
inserted_count=N total_count=M
```

Verify date range:
```sql
SELECT MIN(trade_date), MAX(trade_date), COUNT(*) FROM SC_IDX_TRADING_DAYS;
```

Latest EOD detection (dry run):
```bash
PYTHONPATH=/opt/sustainacore-ai python tools/index_engine/run_daily.py --debug --dry-run
```
Expected output (snippet):
```
latest_eod_date_spy=YYYY-MM-DD
```

Strict completeness + imputation drill:
```sql
DELETE FROM SC_IDX_PRICES_CANON WHERE ticker=:t AND trade_date=:d;
COMMIT;
```
```bash
PYTHONPATH=/opt/sustainacore-ai python tools/index_engine/check_price_completeness.py --since-base --strict --email
PYTHONPATH=/opt/sustainacore-ai python tools/index_engine/impute_missing_prices.py --since-base --email
PYTHONPATH=/opt/sustainacore-ai python tools/index_engine/check_price_completeness.py --since-base --strict
```

Verify imputation rows:
```sql
SELECT trade_date, COUNT(*) FROM SC_IDX_IMPUTATIONS GROUP BY trade_date ORDER BY trade_date DESC FETCH FIRST 5 ROWS ONLY;
SELECT COUNT(*) FROM SC_IDX_PRICES_CANON WHERE quality='IMPUTED';
```

Systemd + logs:
```bash
systemctl list-timers --all | grep sc-idx-price-ingest
sudo journalctl -u sc-idx-price-ingest.service -n 200 --no-pager
```

Manual run:
```bash
PYTHONPATH=/opt/sustainacore-ai python tools/index_engine/run_daily.py --debug
```

Stuck latest date diagnostics:
```bash
PYTHONPATH=/opt/sustainacore-ai python tools/index_engine/debug_latest_eod.py --debug
```

Trading day calendar auto-extend:
```bash
PYTHONPATH=/opt/sustainacore-ai python tools/index_engine/update_trading_days.py --auto --debug
```

Force catch-up ingest for latest effective date:
```bash
PYTHONPATH=/opt/sustainacore-ai python tools/index_engine/run_once_latest.py --debug --force-no-budget-stop
```

Oracle verification for impacted coverage (example date):
```sql
SELECT COUNT(*) FROM SC_IDX_PRICES_CANON
WHERE trade_date = DATE '2025-12-17'
AND ticker IN (
  SELECT ticker
  FROM tech11_ai_gov_eth_index
  WHERE port_weight > 0
  AND port_date = (SELECT MAX(port_date) FROM tech11_ai_gov_eth_index WHERE port_date <= DATE '2025-12-17')
  FETCH FIRST 25 ROWS ONLY
);
```
