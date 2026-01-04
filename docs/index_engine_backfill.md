# SC_IDX price backfill and daily ingest

## How the daily run works
- `tools/index_engine/run_daily.py` updates `SC_IDX_TRADING_DAYS`, then selects the latest trading day that is <= provider latest and <= `(UTC today - 1 day)`.
- The start date is the next trading day after `MAX(trade_date)` in `SC_IDX_PRICES_CANON` (falls back to the end date if already up to date).
- Tickers are split into batches via `--tickers` to stay under the provider throttle of **8 credits/min**; the ingest script sleeps after 429/credit errors.

## Running ingest/backfill
Ensure the environment has `MARKET_DATA_API_KEY`, `MARKET_DATA_API_BASE_URL`, and `DB_*` loaded (for VM1 use `/etc/sustainacore/db.env` and `/etc/sustainacore-ai/secrets.env`). Example invocations:

```bash
python tools/index_engine/run_daily.py
python tools/index_engine/backfill_prices.py --start 2025-01-02 --end 2025-01-31 --debug
python tools/index_engine/backfill_prices.py --date 2025-01-02 --tickers AAPL,MSFT
python tools/index_engine/backfill_prices.py --start 2025-01-02 --end 2025-01-31 --missing-only
```

Notes:
- Use `--tickers` to limit scope or to manually size batches.
- Each ticker probes `SC_IDX_PRICES_RAW` for the last OK `trade_date` before fetching, so reruns are idempotent.
- Weekends/holidays are skipped via `SC_IDX_TRADING_DAYS`; ensure the calendar is current when backfilling.

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
