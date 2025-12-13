# Index engine daily ingestion

The daily index ingestion timer calls `tools/index_engine/run_daily.py` at 23:30 UTC. The flow now:

- Calls `/api_usage` once (cost: 1 credit) to read the current plan usage and limit. The response includes plan IDs like `358001bd-cab0-429a-a8ff-524fbfd0001`; the parser keeps `timestamp`, `current_usage`, `plan_limit`, and `plan_category` when present.
- Computes `remaining_credits = plan_limit - current_usage` and reserves a buffer before any price downloads. Configure the buffer with `SC_IDX_TWELVEDATA_CREDIT_BUFFER` (default 25). We subtract one extra credit for the `/api_usage` request itself.
- Sets `--max-provider-calls` for the ingestion step to `max(0, remaining - buffer - 1)`. Hitting the cap prints `budget_stop: provider_calls_used=X max_provider_calls=Y` and exits 0 so systemd does not treat it as a failure.
- Probes whether a daily bar for today is already published using a single `time_series` request (default probe symbol: `AAPL`, override with `SC_IDX_PROBE_SYMBOL`). If found, we ingest through today; otherwise we fall back to yesterday.
- Runs `tools/index_engine/ingest_prices.py --backfill --start 2025-01-02 --end <chosen_end> --max-provider-calls <computed>` and passes `SC_IDX_TICKERS` when present for the ticker list.

Environment:

- `SC_TWELVEDATA_API_KEY` or `TWELVEDATA_API_KEY` must be set (never logged).
- `SC_IDX_TWELVEDATA_CREDIT_BUFFER` reserves extra headroom near the daily cap (default 25).
- Optional: `SC_IDX_TICKERS` (comma separated) and `SC_IDX_PROBE_SYMBOL`.

New CLI flag:

- `--max-provider-calls N` enforces the credit budget in backfill runs. Each ticker download counts as one provider call.

