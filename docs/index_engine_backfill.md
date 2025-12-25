# SC_IDX price backfill and daily ingest

## How the daily run works
- `tools/index_engine/run_daily.py` uses `(UTC today - 1 day)` as the end date and performs an incremental backfill so gaps from earlier days are filled in the same pass.
- Tickers are split into batches via `--tickers` to stay under the provider throttle of **8 credits/min**; the ingest script sleeps after 429/credit errors.
- Default window: start at `2025-01-02`, end at `(UTC today - 1)`, chunked ticker batches.

## Running ingest/backfill
Ensure the environment has `MARKET_DATA_API_KEY`, `MARKET_DATA_API_BASE_URL`, and `DB_*` loaded (for VM1 use `/etc/sustainacore/db.env` and `/etc/sustainacore-ai/secrets.env`). Example invocations:

```bash
python tools/index_engine/run_daily.py
python tools/index_engine/ingest_prices.py --backfill --start 2025-01-02 --end 2025-01-31 --debug
python tools/index_engine/ingest_prices.py --date 2025-01-02 --tickers AAPL,MSFT
```

Notes:
- Use `--tickers` to limit scope or to manually size batches.
- Each ticker probes `SC_IDX_PRICES_RAW` for the last OK `trade_date` before fetching, so reruns are idempotent.
- Weekends/holidays are naturally skipped because the provider does not return rows for those dates.

## Verification queries (Oracle)
```sql
-- Provider status counts
SELECT status, COUNT(*) FROM SC_IDX_PRICES_RAW WHERE provider='MARKET_DATA' GROUP BY status;

-- Null checks on canonized rows
SELECT COUNT(*) FROM SC_IDX_PRICES_CANON WHERE close IS NULL OR volume IS NULL;

-- Overall canon row count
SELECT COUNT(*) FROM SC_IDX_PRICES_CANON;
```
Re-running the same date range should not increase counts (`MERGE` is idempotent).
