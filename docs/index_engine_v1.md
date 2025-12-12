# TECH100 Index Engine v1

This document outlines the first iteration of the TECH100 index engine to be run on VM1. The goal is to stage schema objects and offline logic before wiring them into the live service.

## Schema (SC_IDX_*)

All objects are prefixed with `SC_IDX_` to avoid collisions with other Sustainacore data. The Oracle DDL lives in `oracle_scripts/sc_idx_index_engine_v1.sql` (creation) and `oracle_scripts/sc_idx_index_engine_v1_drop.sql` (idempotent teardown).

- **SC_IDX_PRICES_RAW**: Raw price rows per provider (e.g., TwelveData, AlphaVantage) including close/adjusted close, volume, currency, and ingest status/error metadata.
- **SC_IDX_PRICES_CANON**: Canonical per-ticker prices after reconciliation, capturing the chosen provider, divergence metrics, and quality flags.
- **SC_IDX_HOLDINGS**: Target weights and share counts for each ticker at a given rebalance date for TECH100.
- **SC_IDX_DIVISOR**: Effective divisors with reasons (corporate actions/rebalances) ensuring index continuity.
- **SC_IDX_LEVELS**: Daily Total Return (TR) levels computed for TECH100.

## Daily flow (planned)

1. **Ingest raw prices**: Pull adjusted closes and closes from configured providers into `SC_IDX_PRICES_RAW` with an ingest timestamp and status.
2. **Reconcile canonical prices**: For each ticker/date, evaluate provider-adjusted closes. If providers agree within a small divergence threshold, store the median adjusted close (and median close when available) in `SC_IDX_PRICES_CANON` with quality `HIGH`; otherwise, choose the best provider (prefer TwelveData, then AlphaVantage) and mark quality `CONFLICT`.
3. **Compute index levels**:
   - Use TECH100 holdings from `SC_IDX_HOLDINGS`.
   - Ensure divisor continuity via `SC_IDX_DIVISOR` when rebalances or actions occur.
   - Calculate the TR level as `sum(shares * adjusted_close) / divisor` and persist into `SC_IDX_LEVELS`.

## Base date

- TECH100 TR base level is **1000** on **2025-01-02**.
- Subsequent levels will be chained from this base using the canonical adjusted closes and divisors.

## Next steps (future PR)

- Wire VM1 jobs to run the DDL via the Codex CLI.
- Implement daily ingestion from data providers and expose APIs/endpoints for TECH100 levels.
