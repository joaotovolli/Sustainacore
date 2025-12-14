# Index engine daily ingestion

The daily index ingestion timer calls `tools/index_engine/run_daily.py` at 23:30 UTC. The flow now:

- Calls `/api_usage` once (cost: 1 credit) only for per-minute awareness (plan_limit=8 on the Basic tier). Minute-level throttling is already enforced by the token bucket in `app/providers/twelvedata.py`.
- Computes daily usage from Oracle: `SUM(PROVIDER_CALLS_USED)` in `SC_IDX_JOB_RUNS` for `provider='TWELVEDATA'` and `started_at` in the current UTC day (`TRUNC(SYSTIMESTAMP)` window).
- Applies a daily cap: `remaining_daily = daily_limit - calls_used_today`, `max_provider_calls = max(0, remaining_daily - daily_buffer)`. Environment:
  - `SC_IDX_TWELVEDATA_DAILY_LIMIT` (default 800)
  - `SC_IDX_TWELVEDATA_DAILY_BUFFER` (default 25; falls back to `SC_IDX_TWELVEDATA_CREDIT_BUFFER` if set)
- Hitting the daily cap prints `daily_budget_stop: ...` and exits 0 so systemd does not treat it as a failure.
- Probes whether a daily bar for today is already published using a single `time_series` request (default probe symbol: `AAPL`, override with `SC_IDX_PROBE_SYMBOL`). If found, we ingest through today; otherwise we fall back to yesterday.
- Runs `tools/index_engine/ingest_prices.py --backfill --start 2025-01-02 --end <chosen_end> --max-provider-calls <computed>` and passes `SC_IDX_TICKERS` when present for the ticker list.

Environment:

- `SC_TWELVEDATA_API_KEY` or `TWELVEDATA_API_KEY` must be set (never logged).
- `SC_IDX_TWELVEDATA_DAILY_LIMIT` sets the daily call ceiling (default 800).
- `SC_IDX_TWELVEDATA_DAILY_BUFFER` reserves extra headroom near the daily cap (default 25; alias `SC_IDX_TWELVEDATA_CREDIT_BUFFER` for back-compat).
- Optional: `SC_IDX_TICKERS` (comma separated) and `SC_IDX_PROBE_SYMBOL`.

New CLI flag:

- `--max-provider-calls N` enforces the credit budget in backfill runs. Each ticker download counts as one provider call.
