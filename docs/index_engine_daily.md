# Index engine daily ingestion

The daily index ingestion timer calls `tools/index_engine/run_daily.py` at 23:30 UTC. The flow now:

- Calls `/api_usage` once (cost: 1 credit) only for per-minute awareness (plan_limit=8 on the Basic tier). Minute-level throttling is enforced by the provider’s shared throttle/lock (default 6 calls per 120 seconds, process-serialized).
- Computes daily usage from Oracle: `SUM(PROVIDER_CALLS_USED)` in `SC_IDX_JOB_RUNS` for `provider='TWELVEDATA'` and `started_at` in the current UTC day (`TRUNC(SYSTIMESTAMP)` window).
- Applies a daily cap: `remaining_daily = daily_limit - calls_used_today`, `max_provider_calls = max(0, remaining_daily - daily_buffer)`. Environment:
  - `SC_IDX_TWELVEDATA_DAILY_LIMIT` (default 800)
  - `SC_IDX_TWELVEDATA_DAILY_BUFFER` (default 25; falls back to `SC_IDX_TWELVEDATA_CREDIT_BUFFER` if set)
- Hitting the daily cap prints `daily_budget_stop: ...` and exits 0 so systemd does not treat it as a failure (email only when `SC_IDX_EMAIL_ON_BUDGET_STOP=1`).
- Probes whether a daily bar for today is already published using a single `time_series` request (default probe symbol: `AAPL`, override with `SC_IDX_PROBE_SYMBOL`). If found, we ingest through today; otherwise we fall back to yesterday.
- Runs `tools/index_engine/ingest_prices.py --backfill --start 2025-01-02 --end <chosen_end> --max-provider-calls <computed>` and passes `SC_IDX_TICKERS` when present for the ticker list.

Environment:

- `SC_TWELVEDATA_API_KEY` or `TWELVEDATA_API_KEY` must be set (never logged).
- All index-engine CLI tools auto-load environment files (best-effort) on startup via `tools/index_engine/env_loader.py`, reading (in order): `/etc/sustainacore/db.env` then `/etc/sustainacore-ai/secrets.env`. Explicit shell env vars still win.
- `SC_IDX_TWELVEDATA_DAILY_LIMIT` sets the daily call ceiling (default 800).
- `SC_IDX_TWELVEDATA_DAILY_BUFFER` reserves extra headroom near the daily cap (default 25; alias `SC_IDX_TWELVEDATA_CREDIT_BUFFER` for back-compat).
- Twelve Data throttle override (rarely needed): `SC_IDX_TWELVEDATA_CALLS_PER_WINDOW` (default 6) and `SC_IDX_TWELVEDATA_WINDOW_SECONDS` (default 120). All provider calls are serialized via `/tmp/sc_idx_twelvedata.lock` to avoid cross-process spikes.
- Optional: `SC_IDX_TICKERS` (comma separated) and `SC_IDX_PROBE_SYMBOL`.
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
