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
  -p EnvironmentFile=/etc/sustainacore/index.env \
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

## FI -> FISV migration
To permanently replace ticker `FI` with `FISV` across history, run the migration tool:

```bash
python3 tools/db_migrations/migrate_fi_to_fisv.py          # dry-run (default)
python3 tools/db_migrations/migrate_fi_to_fisv.py --apply  # apply with backups
```

Artifacts are written to:
- `tools/audit/output/fi_to_fisv_migration_report.md`
- `tools/audit/output/fi_to_fisv_collisions.csv`
- rollback backups in `SC_BAK_FI_FISV_*` tables

After applying, recompute index outputs for the affected window and verify:

```bash
PYTHONPATH=/home/opc/Sustainacore python3 tools/index_engine/calc_index.py \
  --start 2026-01-05 --end 2026-01-08 --strict --debug --no-preflight-self-heal
PYTHONPATH=/home/opc/Sustainacore python3 tools/index_engine/check_price_completeness.py \
  --start 2026-01-08 --end 2026-01-08 --min-daily-coverage 1.0 --max-bad-days 0 --allow-canon-close
```

## Pipeline health + resume
The daily pipeline writes a compact health snapshot to:

```
tools/audit/output/pipeline_health_latest.txt
```

Key fields:
- `calendar_max_date`, `canon_max_date`, `levels_max_date`, `stats_max_date`
- `next_missing_trading_day`
- `stage_duration_<stage>_sec`
- `last_error`

To resume a failed run, re-run the pipeline (resume is default):

```bash
python3 tools/index_engine/run_pipeline.py
```

To force a full restart and re-run every stage:

```bash
python3 tools/index_engine/run_pipeline.py --restart
```

## Stuck index levels (prices updated, levels not)
When `SC_IDX_LEVELS` stops advancing but prices continue, run this diagnostic sequence:

```sql
SELECT trade_date FROM SC_IDX_TRADING_DAYS
 WHERE trade_date BETWEEN DATE '2025-12-29' AND DATE '2026-01-10'
 ORDER BY trade_date;

SELECT trade_date, COUNT(*) FROM SC_IDX_PRICES_CANON
 WHERE trade_date BETWEEN DATE '2025-12-31' AND DATE '2026-01-10'
 GROUP BY trade_date ORDER BY trade_date;

SELECT trade_date, level_tr FROM SC_IDX_LEVELS
 WHERE index_code='TECH100' AND trade_date BETWEEN DATE '2025-12-31' AND DATE '2026-01-10'
 ORDER BY trade_date;
```

If levels lag by >= 1 trading day:
1) Run calc diagnostics (safe, read-only):
```bash
PYTHONPATH=/home/opc/Sustainacore python3 tools/index_engine/calc_index.py \
  --start 2026-01-02 --end 2026-01-07 --diagnose-only --diagnose-missing --diagnose-missing-sql
```
2) If missing prices are reported, backfill only those tickers for the missing date:
```bash
python3 tools/index_engine/ingest_prices.py --start 2026-01-05 --end 2026-01-05 --backfill-missing --tickers TICKER1,TICKER2
```
3) Recompute levels/stats/contrib for the missing date range:
```bash
PYTHONPATH=/home/opc/Sustainacore python3 tools/index_engine/calc_index.py \
  --start 2026-01-05 --end 2026-01-07 --no-preflight-self-heal --debug
```
