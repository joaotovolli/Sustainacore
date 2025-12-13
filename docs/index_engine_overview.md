# SC_IDX index engine overview

## What the SC_IDX tables store
- **SC_IDX_PRICES_RAW**: Provider responses stored as-is per ticker/day with status and metadata for troubleshooting.
- **SC_IDX_PRICES_CANON**: Canonicalized close/volume rows derived from provider payloads after validation.
- **SC_IDX_HOLDINGS**: Target index constituents and weights for each rebalance period (planned; not populated yet).
- **SC_IDX_DIVISOR**: Index divisor history to align index levels across rebalances (planned).
- **SC_IDX_LEVELS**: Final index levels calculated from canon prices, holdings, and divisors (planned).

## Implemented components (prices only)
- **Provider**: Twelve Data `/time_series` with throttling at 8 credits/min to respect the free tier.
- **Ingest script**: `tools/index_engine/ingest_prices.py` handles incremental fetch + MERGE into RAW and CANON.
- **Daily runner**: `tools/index_engine/run_daily.py` computes `(UTC today - 1)` as the end date and runs chunked ingest.
- **Systemd timer**: `infra/systemd/sc-idx-price-ingest.service` + `.timer` execute the daily runner at 23:30 UTC with env files `/etc/sustainacore/db.env` and `/etc/sustainacore-ai/secrets.env`.

## Not implemented yet
- Index level computation from holdings + divisor series.
- Dividend verification / adjustments beyond provider-adjusted closes.
- Secondary provider (Provider B) failover.
