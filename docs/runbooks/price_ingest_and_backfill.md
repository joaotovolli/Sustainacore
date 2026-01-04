# TECH100 price ingest and backfill runbook

## Overview
- Daily ingest uses `tools/index_engine/run_daily.py`.
- Trading days are sourced from `SC_IDX_TRADING_DAYS` and updated before ingest.
- The ingest window starts at the next trading day after `MAX(trade_date)` in `SC_IDX_PRICES_CANON`.

## Preflight (Oracle)
Run this before any Oracle task:

```bash
python3 tools/oracle/preflight_oracle.py
```

## Manual backfill (single day)
Use the deterministic backfill CLI:

```bash
python3 tools/index_engine/backfill_prices.py --date 2026-01-02 --debug
```

If the environment variables are only available via root-only env files, run via systemd:

```bash
sudo -n systemd-run --wait --collect --pipe --unit=sc-idx-backfill \
  --working-directory=/home/opc/Sustainacore \
  -p EnvironmentFile=/etc/sustainacore/db.env \
  -p EnvironmentFile=/etc/sustainacore-ai/app.env \
  -p EnvironmentFile=/etc/sustainacore-ai/secrets.env \
  -- python3 tools/index_engine/backfill_prices.py --date 2026-01-02 --debug
```

## Manual backfill (range or missing-only)
```bash
python3 tools/index_engine/backfill_prices.py --start 2026-01-02 --end 2026-01-06
python3 tools/index_engine/backfill_prices.py --start 2026-01-02 --end 2026-01-06 --missing-only
```

## Recompute index levels/stats for a single day (no price ingest)
Use this when levels/stats are wrong but prices are already present:

```bash
PYTHONPATH=/home/opc/Sustainacore python3 tools/index_engine/calc_index.py \
  --start 2026-01-02 --end 2026-01-02 --rebuild --no-preflight-self-heal
```

## Trading day calendar refresh
```bash
python3 tools/index_engine/update_trading_days.py --auto --debug
```

## Verification (Oracle)
```sql
SELECT trade_date, COUNT(*)
FROM SC_IDX_PRICES_CANON
WHERE trade_date IN (DATE '2025-12-31', DATE '2026-01-02')
GROUP BY trade_date
ORDER BY trade_date;

SELECT MAX(trade_date) FROM SC_IDX_LEVELS;
```

## Troubleshooting
- If `SC_IDX_TRADING_DAYS` lags behind the provider latest EOD date, the daily run will fail with
  `trading_days_behind_provider` and should be retried after refreshing the calendar.
- If `missing_prices_for_date` appears, re-run a single-day backfill for the reported trade date.
