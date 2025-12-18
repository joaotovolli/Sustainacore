# TECH100 Index Engine v1

This document describes how the TECH100 index is calculated and stored daily.

## What the script does

`tools/index_engine/calc_index.py`:

- Reads trading days from `SC_IDX_TRADING_DAYS`.
- For each trading day, determines the impacted universe:
  - `PORT_DATE = MAX(PORT_DATE) <= trade_date` from `TECH11_AI_GOV_ETH_INDEX`.
  - `PORT_WEIGHT > 0`, top 25 by weight.
- On each rebalance date, sets equal weights and computes shares/divisor for continuity.
- Computes daily index level (Total Return) using `SC_IDX_PRICES_CANON`:
  - Prefers `CANON_ADJ_CLOSE_PX`.
  - Allows `CANON_CLOSE_PX` only with `--allow-close`.
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

Daily levels:

- For each trading day `t`, use the latest rebalance holdings:
  - `MV_t = Σ Shares_i * Price_i(t)`
  - `Level_TR(t) = MV_t / Divisor(rebalance)`

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
