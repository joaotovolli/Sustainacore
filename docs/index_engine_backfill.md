# SC_IDX price backfill and TwelveData throttling

## Throttle behavior
- TwelveData free tier allows 8 credits/min. The fetcher now requests **one ticker per call** and uses a token bucket (8/min) with automatic sleeps after HTTP 429 or credit errors. Each ticker call is retried up to 5 times.
- Requests use `/time_series` with `interval=1day`, `order=ASC`, `timezone=Exchange`, and `adjust=all` so the `close` we store is already adjusted (dividends + splits).

## Running backfill safely
```
# ensure env has TWELVEDATA_API_KEY and DB_* (e.g. source /etc/sustainacore/db.env and /etc/sustainacore-ai/secrets.env)
python tools/index_engine/ingest_prices.py --backfill --start 2025-01-02 --end 2025-01-31 --debug
```
- Optional: `--tickers T1,T2` to limit scope.
- The backfill probes each ticker for the last OK `trade_date` in `SC_IDX_PRICES_RAW` and only fetches missing days. Weekends/holidays are skipped naturally.
- Canonical rows are written only when provider status is OK.

## Verification queries (Oracle)
```
SELECT status, COUNT(*) FROM SC_IDX_PRICES_RAW WHERE provider='TWELVEDATA' GROUP BY status;
SELECT COUNT(*) FROM SC_IDX_PRICES_CANON;
```
Re-running the same backfill range should not increase counts (idempotent MERGE).
