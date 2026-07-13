<!-- cspell:ignore LOOKBACK -->
# TECH100 Index Engine v1

This document describes how the TECH100 index is calculated and stored daily.

In production, this script is now invoked by the SC_IDX LangGraph pipeline after completeness and
imputation have been resolved for the target window. The orchestrator calls it with
`--no-preflight-self-heal` because ingest, readiness, and imputation are handled earlier in the graph.

## What the script does

`tools/index_engine/calc_index.py`:

- Reads trading days from `SC_IDX_TRADING_DAYS`.
- For each trading day, determines the impacted universe:
  - `PORT_DATE = MAX(PORT_DATE) <= trade_date` from `TECH11_AI_GOV_ETH_INDEX`.
  - `PORT_WEIGHT > 0`, top 25 by weight.
- On each rebalance date, sets equal weights and computes shares/divisor for continuity.
- When the run starts mid-series, it anchors off the prior trading day's stored level/holdings/divisor if available (base=1000 only when no prior level exists).
- Computes daily index level (Total Return) using `SC_IDX_PRICES_CANON`:
  - Prefers `CANON_ADJ_CLOSE_PX`.
  - Allows `CANON_CLOSE_PX` only with `--allow-close`.
- Runs fail-closed validation before publishing derived rows for a rebalance window:
  - every incoming constituent must have a canonical previous trading day price;
  - stale historical or current-day anchors are rejected for rebalance continuity;
  - one-day constituent returns above the configured sanity threshold are rejected;
  - the new basket valued at previous-day prices must reconcile to the previous stored level.
- Stores:
  - `SC_IDX_LEVELS` (level_tr)
  - `SC_IDX_CONSTITUENT_DAILY` (daily holdings snapshot + weights)
  - `SC_IDX_CONTRIBUTION_DAILY` (daily contribution per ticker)
  - `SC_IDX_STATS_DAILY` (level, returns, stats)

## Methodology summary

Base:

- Base date: 2025-01-02
- Base level: 1000.0

Rebalance:

- Rebalance occurs on the first trading day where the `PORT_DATE` changes.
- Target weights are equal weight across the impacted universe.
- Shares are computed off the previous trading day's level to avoid jumps:
  - `MV_prev = Level_TR(P) * Divisor(P)`
  - `Shares_i = (TargetWeight_i * MV_prev) / Price_i(P)`
  - `Divisor(R) = MV_newBasket_prevClose / Level_TR(P)`
- Rebalance continuity must use prices from the immediately previous trading day. The calculator does
  not substitute stale historical anchors for missing prior-day prices.
- The continuity bridge check requires `MV_newBasket_prevClose / Divisor(R)` to match
  `Level_TR(P)` within tolerance before any rebalance-derived rows are written.

Daily levels:

- For each trading day `t`, use the latest rebalance holdings:
  - `MV_t = Σ Shares_i * Price_i(t)`
  - `Level_TR(t) = MV_t / Divisor(rebalance)`
- Before publishing, the calculator checks one-day constituent price moves for suspicious
  adjusted/unadjusted or split-basis mismatches. The default absolute return threshold is 20%.
- Ratio-like moves create an auditable pending corporate-action candidate and block publication.
- A confirmed action still blocks publication until adjusted history is consistent across the event.
- Under the adjusted-price methodology, confirmed splits trigger a bounded ticker-history refresh and
  dependent rebuild; synthetic shares are not also multiplied.
- Controlled `--rebuild --strict` runs make one bounded exact-date recovery attempt for missing
  rebalance anchors, then refetch and apply the same fail-closed quality checks. Scheduled incremental
  calculations do not enable this reconstruction-only retry.
- Full-range rebuilds retain only the previous and current price maps for return and contribution
  calculations. Oracle array DML is emitted in configurable bounded batches; levels, shares, divisors,
  contributions and statistics use the same formulas and tolerances as the unbatched path.

Stats lookback windows:

- Rolling return/volatility windows pull prior levels from Oracle so single-day recomputes still populate `ret_1d/ret_5d/ret_20d/vol_20d`.

## Running locally

```bash
PYTHONPATH=/opt/sustainacore-ai python tools/index_engine/calc_index.py --since-base --strict
```

Optional flags:

- `--start YYYY-MM-DD` / `--end YYYY-MM-DD`
- `--rebuild` (delete and recompute the range)
- `--allow-close` (use close if adj close is missing)
- `--debug`
- `--preflight-self-heal` / `--no-preflight-self-heal`
- `--diagnose-missing` / `--no-diagnose-missing`
- `--diagnose-missing-sql` / `--no-diagnose-missing-sql`
- `--max-dates` / `--max-tickers` / `--max-samples`
- `--email-on-fail` (send one alert/day when strict fails)
- `--dry-run` (print missing diagnostics only)

Optional validation tuning:

- `SC_IDX_MAX_ABS_CONSTITUENT_RETURN` (default `0.20`)
- `SC_IDX_REBALANCE_CONTINUITY_ABS_TOL` (default `1e-6`)
- `SC_IDX_REBALANCE_CONTINUITY_REL_TOL` (default `1e-8`)

Corporate-action repair and rollback are documented in
`docs/runbooks/corporate_action_reconstruction.md`.

Before a controlled reconstruction, run `tools/index_engine/reconstruction_readiness.py`. It scans the
complete intended range, aggregates all rebalance and price-basis blockers, rehearses portfolio output
construction without writes, and exits non-zero unless `overall_status=PASS`.

Primary orchestrated entrypoint:

```bash
python3 tools/index_engine/run_pipeline.py
```

## Strict failure diagnostics

When strict mode fails, the script prints a diagnostic block with:

- date range evaluated
- impacted universe definition (top 25, `PORT_WEIGHT > 0`, latest rebalance <= trade date)
- top missing dates and tickers
- sample missing rows

Use this to pinpoint which dates/tickers need ingest or imputation.

## Validation queries (Oracle)

```sql
SELECT COUNT(*) FROM SC_IDX_LEVELS;
SELECT COUNT(*) FROM SC_IDX_CONSTITUENT_DAILY;
SELECT COUNT(*) FROM SC_IDX_CONTRIBUTION_DAILY;
SELECT COUNT(*) FROM SC_IDX_STATS_DAILY;

SELECT trade_date, level_tr
FROM SC_IDX_LEVELS
ORDER BY trade_date DESC
FETCH FIRST 5 ROWS ONLY;
```

Daily weights sanity:

```sql
SELECT trade_date, SUM(weight) AS weight_sum
FROM SC_IDX_CONSTITUENT_DAILY
GROUP BY trade_date
ORDER BY trade_date DESC
FETCH FIRST 5 ROWS ONLY;
```
